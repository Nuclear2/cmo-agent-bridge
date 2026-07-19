[CmdletBinding()]
param(
    [Parameter()]
    [ValidatePattern('^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$')]
    [string]$Version = '0.5.0',

    [Parameter()]
    [string]$BundlePath,

    [Parameter()]
    [string]$UserHome = [Environment]::GetFolderPath('UserProfile')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$PluginName = 'cmo-agent-bridge'
$BundleName = "$PluginName-plugin-$Version.zip"
$ReleaseBaseUrl = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v$Version"
$Utf8NoBom = New-Object Text.UTF8Encoding($false)

function Test-ObjectProperty {
    param(
        [Parameter(Mandatory)]
        [object]$InputObject,

        [Parameter(Mandatory)]
        [string]$Name
    )

    return $null -ne $InputObject.PSObject.Properties[$Name]
}

function Get-JsonFile {
    param([Parameter(Mandatory)][string]$Path)

    try {
        return Get-Content -Raw -Encoding UTF8 -LiteralPath $Path | ConvertFrom-Json
    }
    catch {
        throw "Invalid JSON file '$Path': $($_.Exception.Message)"
    }
}

function Assert-SafeZipEntries {
    param(
        [Parameter(Mandatory)]
        [string]$ArchivePath,

        [Parameter(Mandatory)]
        [string]$DestinationDirectory
    )

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $destinationRoot = [IO.Path]::GetFullPath($DestinationDirectory)
    $destinationPrefix = $destinationRoot.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
    $archive = [IO.Compression.ZipFile]::OpenRead($ArchivePath)
    try {
        foreach ($entry in $archive.Entries) {
            $entryPath = $entry.FullName.Replace('/', [IO.Path]::DirectorySeparatorChar)
            if (
                [IO.Path]::IsPathRooted($entryPath) -or
                $entryPath.IndexOf([char]0) -ge 0
            ) {
                throw "Bundle contains an unsafe path: '$($entry.FullName)'."
            }

            $expandedPath = [IO.Path]::GetFullPath((Join-Path $destinationRoot $entryPath))
            if (
                $expandedPath -ne $destinationRoot -and
                -not $expandedPath.StartsWith($destinationPrefix, [StringComparison]::OrdinalIgnoreCase)
            ) {
                throw "Bundle contains a path outside its extraction directory: '$($entry.FullName)'."
            }
        }
    }
    finally {
        $archive.Dispose()
    }
}

function Assert-PluginBundle {
    param(
        [Parameter(Mandatory)]
        [string]$PluginDirectory,

        [Parameter(Mandatory)]
        [string]$ExpectedVersion
    )

    if (-not (Test-Path -LiteralPath $PluginDirectory -PathType Container)) {
        throw "Bundle does not contain 'plugins\cmo-agent-bridge'."
    }

    $manifestPath = Join-Path $PluginDirectory '.codex-plugin\plugin.json'
    $codexMcpPath = Join-Path $PluginDirectory '.mcp.json'
    $skillPath = Join-Path $PluginDirectory 'skills\operate-cmo\SKILL.md'
    foreach ($requiredPath in @($manifestPath, $codexMcpPath, $skillPath)) {
        if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
            throw "Bundle is missing required plugin file '$requiredPath'."
        }
    }

    $manifest = Get-JsonFile $manifestPath
    if (-not (Test-ObjectProperty -InputObject $manifest -Name 'name')) {
        throw 'Codex plugin manifest has no name.'
    }
    if ([string]$manifest.name -ne $PluginName) {
        throw "Codex plugin manifest name '$($manifest.name)' is not '$PluginName'."
    }
    if (-not (Test-ObjectProperty -InputObject $manifest -Name 'version')) {
        throw 'Codex plugin manifest has no version.'
    }
    if ([string]$manifest.version -ne $ExpectedVersion) {
        throw "Bundle version '$($manifest.version)' does not match requested version '$ExpectedVersion'."
    }

    $codexMcp = Get-JsonFile $codexMcpPath
    if (-not (Test-ObjectProperty -InputObject $codexMcp -Name 'mcpServers')) {
        throw "Codex MCP configuration has no 'mcpServers' object."
    }
    if (-not (Test-ObjectProperty -InputObject $codexMcp.mcpServers -Name $PluginName)) {
        throw "Codex MCP configuration does not define '$PluginName'."
    }

    $skillText = Get-Content -Raw -Encoding UTF8 -LiteralPath $skillPath
    if ($skillText -notmatch '(?ms)^---\s+.*?^name:\s*operate-cmo\s*$.*?^---\s*$') {
        throw "The packaged operate-cmo skill has invalid frontmatter."
    }
}

function Get-ReleaseBundle {
    param(
        [Parameter(Mandatory)]
        [string]$WorkingDirectory
    )

    $downloadedBundle = Join-Path $WorkingDirectory $BundleName
    $checksumsPath = Join-Path $WorkingDirectory 'SHA256SUMS'
    $webRequestParameters = @{
        UseBasicParsing = $true
        ErrorAction = 'Stop'
    }

    Write-Host "Downloading CMO Agent Bridge v${Version}..."
    Invoke-WebRequest @webRequestParameters `
        -Uri "$ReleaseBaseUrl/$BundleName" `
        -OutFile $downloadedBundle
    Invoke-WebRequest @webRequestParameters `
        -Uri "$ReleaseBaseUrl/SHA256SUMS" `
        -OutFile $checksumsPath

    $escapedName = [regex]::Escape($BundleName)
    $checksumText = Get-Content -Raw -Encoding UTF8 -LiteralPath $checksumsPath
    $checksumMatch = [regex]::Match(
        $checksumText,
        "(?im)^(?<hash>[0-9a-f]{64})\s+\*?${escapedName}\s*$"
    )
    if (-not $checksumMatch.Success) {
        throw "SHA256SUMS has no checksum for '$BundleName'."
    }

    $expectedHash = $checksumMatch.Groups['hash'].Value.ToLowerInvariant()
    $actualHash = (
        Get-FileHash -Algorithm SHA256 -LiteralPath $downloadedBundle
    ).Hash.ToLowerInvariant()
    if ($actualHash -ne $expectedHash) {
        throw "Checksum mismatch for '$BundleName'."
    }

    return $downloadedBundle
}

function New-MarketplaceDocument {
    return [PSCustomObject][ordered]@{
        name = 'personal'
        interface = [ordered]@{
            displayName = 'Personal'
        }
        plugins = @()
    }
}

function Update-PersonalMarketplace {
    param([Parameter(Mandatory)][string]$MarketplacePath)

    if (Test-Path -LiteralPath $MarketplacePath -PathType Leaf) {
        $marketplace = Get-JsonFile $MarketplacePath
        if (
            $null -eq $marketplace -or
            $marketplace.GetType().FullName -ne 'System.Management.Automation.PSCustomObject'
        ) {
            throw "Personal marketplace root must be a JSON object: '$MarketplacePath'."
        }
    }
    else {
        $marketplace = New-MarketplaceDocument
    }

    if (-not (Test-ObjectProperty -InputObject $marketplace -Name 'name')) {
        $marketplace | Add-Member -MemberType NoteProperty -Name 'name' -Value 'personal'
    }
    if (-not (Test-ObjectProperty -InputObject $marketplace -Name 'interface')) {
        $marketplace | Add-Member -MemberType NoteProperty -Name 'interface' -Value (
            [ordered]@{ displayName = 'Personal' }
        )
    }

    $existingPlugins = @()
    if (Test-ObjectProperty -InputObject $marketplace -Name 'plugins') {
        if (-not ($marketplace.plugins -is [Array])) {
            throw "Personal marketplace 'plugins' must be a JSON array: '$MarketplacePath'."
        }
        $existingPlugins = @($marketplace.plugins)
    }

    $updatedPlugins = @(
        foreach ($plugin in $existingPlugins) {
            if (
                $null -ne $plugin -and
                (Test-ObjectProperty -InputObject $plugin -Name 'name') -and
                [string]$plugin.name -eq $PluginName
            ) {
                continue
            }
            $plugin
        }
    )
    $updatedPlugins += [PSCustomObject][ordered]@{
        name = $PluginName
        source = [ordered]@{
            source = 'local'
            path = './.codex/plugins/cmo-agent-bridge'
        }
        policy = [ordered]@{
            installation = 'AVAILABLE'
            authentication = 'ON_INSTALL'
        }
        category = 'Productivity'
    }

    if (Test-ObjectProperty -InputObject $marketplace -Name 'plugins') {
        $marketplace.plugins = @($updatedPlugins)
    }
    else {
        $marketplace | Add-Member -MemberType NoteProperty -Name 'plugins' -Value @($updatedPlugins)
    }

    $marketplaceDirectory = Split-Path -Parent $MarketplacePath
    New-Item -ItemType Directory -Force -Path $marketplaceDirectory | Out-Null
    $temporaryPath = Join-Path $marketplaceDirectory (
        '.marketplace.json.{0}.tmp' -f [guid]::NewGuid().ToString('N')
    )
    $backupPath = Join-Path $marketplaceDirectory (
        '.marketplace.json.{0}.backup' -f [guid]::NewGuid().ToString('N')
    )

    try {
        $json = $marketplace | ConvertTo-Json -Depth 100
        [IO.File]::WriteAllText($temporaryPath, $json + [Environment]::NewLine, $Utf8NoBom)
        if (Test-Path -LiteralPath $MarketplacePath -PathType Leaf) {
            [IO.File]::Replace($temporaryPath, $MarketplacePath, $backupPath, $true)
            if (Test-Path -LiteralPath $backupPath) {
                Remove-Item -Force -LiteralPath $backupPath -ErrorAction SilentlyContinue
            }
        }
        else {
            [IO.File]::Move($temporaryPath, $MarketplacePath)
        }
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPath) {
            Remove-Item -Force -LiteralPath $temporaryPath
        }
    }
}

if ([string]::IsNullOrWhiteSpace($UserHome)) {
    throw 'Could not determine the user home directory. Pass -UserHome explicitly.'
}
$UserHome = [IO.Path]::GetFullPath($UserHome)
if (Test-Path -LiteralPath $UserHome -PathType Leaf) {
    throw "User home is a file, not a directory: '$UserHome'."
}

$workingDirectory = Join-Path ([IO.Path]::GetTempPath()) (
    'cmo-agent-bridge-install-{0}' -f [guid]::NewGuid().ToString('N')
)
$extractDirectory = Join-Path $workingDirectory 'extracted'
New-Item -ItemType Directory -Force -Path $extractDirectory | Out-Null

$stagedPlugin = $null
$backupPlugin = $null
$installedPlugin = $false
try {
    if ([string]::IsNullOrWhiteSpace($BundlePath)) {
        $resolvedBundlePath = Get-ReleaseBundle -WorkingDirectory $workingDirectory
    }
    else {
        $resolvedBundlePath = [IO.Path]::GetFullPath($BundlePath)
        if (-not (Test-Path -LiteralPath $resolvedBundlePath -PathType Leaf)) {
            throw "Plugin bundle does not exist: '$resolvedBundlePath'."
        }
    }

    Assert-SafeZipEntries `
        -ArchivePath $resolvedBundlePath `
        -DestinationDirectory $extractDirectory
    Expand-Archive -LiteralPath $resolvedBundlePath -DestinationPath $extractDirectory -Force

    $pluginSource = Join-Path $extractDirectory 'plugins\cmo-agent-bridge'
    Assert-PluginBundle -PluginDirectory $pluginSource -ExpectedVersion $Version

    $pluginParent = Join-Path $UserHome '.codex\plugins'
    $pluginDestination = Join-Path $pluginParent $PluginName
    $marketplacePath = Join-Path $UserHome '.agents\plugins\marketplace.json'
    New-Item -ItemType Directory -Force -Path $pluginParent | Out-Null

    $transactionId = [guid]::NewGuid().ToString('N')
    $stagedPlugin = Join-Path $pluginParent ".${PluginName}.install.$transactionId"
    $backupPlugin = Join-Path $pluginParent ".${PluginName}.backup.$transactionId"
    Copy-Item -Recurse -Force -LiteralPath $pluginSource -Destination $stagedPlugin
    Assert-PluginBundle -PluginDirectory $stagedPlugin -ExpectedVersion $Version

    try {
        if (Test-Path -LiteralPath $pluginDestination) {
            Move-Item -LiteralPath $pluginDestination -Destination $backupPlugin
        }
        Move-Item -LiteralPath $stagedPlugin -Destination $pluginDestination
        $installedPlugin = $true

        Update-PersonalMarketplace -MarketplacePath $marketplacePath
    }
    catch {
        if ($installedPlugin -and (Test-Path -LiteralPath $pluginDestination)) {
            Remove-Item -Recurse -Force -LiteralPath $pluginDestination
        }
        if (Test-Path -LiteralPath $backupPlugin) {
            Move-Item -LiteralPath $backupPlugin -Destination $pluginDestination
        }
        throw
    }

    if (Test-Path -LiteralPath $backupPlugin) {
        Remove-Item -Recurse -Force -LiteralPath $backupPlugin -ErrorAction SilentlyContinue
        if (Test-Path -LiteralPath $backupPlugin) {
            Write-Warning "The previous plugin backup could not be removed: '$backupPlugin'."
        }
    }

    Write-Host "Installed CMO Agent Bridge v${Version} for ChatGPT/Codex Desktop."
    Write-Host "Plugin source: $pluginDestination"
    Write-Host "Restart ChatGPT Desktop, open Plugins, and install CMO Agent Bridge from Personal."
    Write-Host "This installer does not install uv; make sure uvx is available before using the MCP tools."
}
finally {
    if ($null -ne $stagedPlugin -and (Test-Path -LiteralPath $stagedPlugin)) {
        Remove-Item -Recurse -Force -LiteralPath $stagedPlugin
    }
    if (Test-Path -LiteralPath $workingDirectory) {
        Remove-Item -Recurse -Force -LiteralPath $workingDirectory
    }
}
