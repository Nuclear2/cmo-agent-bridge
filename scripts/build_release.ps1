[CmdletBinding()]
param(
    [Parameter()]
    [ValidatePattern('^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$')]
    [string]$Version,

    [Parameter()]
    [string]$OutputDirectory,

    [Parameter()]
    [switch]$RequireClean
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $RepoRoot 'dist\release'
}
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)

$rootWithSeparator = $RepoRoot.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
if (
    $OutputDirectory -eq $RepoRoot -or
    -not $OutputDirectory.StartsWith($rootWithSeparator, [StringComparison]::OrdinalIgnoreCase)
) {
    throw "Release output must be a child of the repository root: $OutputDirectory"
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory)]
        [string]$FilePath,

        [Parameter(ValueFromRemainingArguments)]
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $FilePath $($Arguments -join ' ')"
    }
}

function Get-JsonFile {
    param([Parameter(Mandatory)][string]$Path)

    return Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
}

function New-ZipFromDirectory {
    param(
        [Parameter(Mandatory)][string]$SourceDirectory,
        [Parameter(Mandatory)][string]$DestinationPath
    )

    if (Test-Path -LiteralPath $DestinationPath) {
        Remove-Item -Force -LiteralPath $DestinationPath
    }
    [IO.Compression.ZipFile]::CreateFromDirectory(
        $SourceDirectory,
        $DestinationPath,
        [IO.Compression.CompressionLevel]::Optimal,
        $false
    )
}

Add-Type -AssemblyName System.IO.Compression.FileSystem

$gitRoot = (
    Invoke-Checked git -c "safe.directory=$RepoRoot" -C $RepoRoot rev-parse --show-toplevel |
        Select-Object -Last 1
).Trim()
if ([IO.Path]::GetFullPath($gitRoot) -ne $RepoRoot) {
    throw "Expected repository root '$RepoRoot', but git resolved '$gitRoot'."
}

if ($RequireClean) {
    $dirty = @(
        Invoke-Checked git -c "safe.directory=$RepoRoot" -C $RepoRoot `
            status --porcelain --untracked-files=all
    )
    if ($dirty.Count -gt 0) {
        throw "Release builds require a clean checkout.`n$($dirty -join [Environment]::NewLine)"
    }
}

$pyprojectText = Get-Content -Raw -LiteralPath (Join-Path $RepoRoot 'pyproject.toml')
$projectVersionMatch = [regex]::Match(
    $pyprojectText,
    '(?ms)^\[project\]\s+.*?^version\s*=\s*"(?<version>[^"]+)"'
)
if (-not $projectVersionMatch.Success) {
    throw 'Could not read [project].version from pyproject.toml.'
}
$projectVersion = $projectVersionMatch.Groups['version'].Value
if ([string]::IsNullOrWhiteSpace($Version)) {
    $Version = $projectVersion
}
elseif ($projectVersion -ne $Version) {
    throw "Requested version '$Version' does not match pyproject.toml version '$projectVersion'."
}

$codexManifestPath = Join-Path $RepoRoot 'plugins\cmo-agent-bridge\.codex-plugin\plugin.json'
$claudeManifestPath = Join-Path $RepoRoot 'plugins\cmo-agent-bridge\.claude-plugin\plugin.json'
$claudeMarketplacePath = Join-Path $RepoRoot '.claude-plugin\marketplace.json'
$codexManifest = Get-JsonFile $codexManifestPath
$claudeManifest = Get-JsonFile $claudeManifestPath
$claudeMarketplace = Get-JsonFile $claudeMarketplacePath

$declaredVersions = @(
    $codexManifest.version
    $claudeManifest.version
    $claudeMarketplace.plugins[0].version
)
foreach ($declaredVersion in $declaredVersions) {
    if ($declaredVersion -ne $Version) {
        throw "Distribution metadata version '$declaredVersion' does not match '$Version'."
    }
}

$pluginSource = Join-Path $RepoRoot 'plugins\cmo-agent-bridge'
$skillSource = Join-Path $pluginSource 'skills\operate-cmo'
if (-not (Test-Path -LiteralPath (Join-Path $skillSource 'SKILL.md'))) {
    throw "Missing operate-cmo skill at '$skillSource'."
}
$desktopInstallerSource = Join-Path $RepoRoot 'scripts\install-codex-desktop.ps1'
if (-not (Test-Path -LiteralPath $desktopInstallerSource -PathType Leaf)) {
    throw "Missing Codex Desktop installer at '$desktopInstallerSource'."
}
$desktopInstallerText = Get-Content -Raw -LiteralPath $desktopInstallerSource
$desktopInstallerVersionMatch = [regex]::Match(
    $desktopInstallerText,
    '(?m)^\s*\[string\]\$Version\s*=\s*''(?<version>[^'']+)'''
)
if (-not $desktopInstallerVersionMatch.Success) {
    throw "Could not read the default version from '$desktopInstallerSource'."
}
$desktopInstallerVersion = $desktopInstallerVersionMatch.Groups['version'].Value
if ($desktopInstallerVersion -ne $Version) {
    throw "Codex Desktop installer version '$desktopInstallerVersion' does not match '$Version'."
}

if (Test-Path -LiteralPath $OutputDirectory) {
    Remove-Item -Recurse -Force -LiteralPath $OutputDirectory
}
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

$staging = Join-Path $OutputDirectory '.staging'
$packageOutput = Join-Path $staging 'packages'
$marketplaceBundle = Join-Path $staging 'marketplace-bundle'
$skillBundle = Join-Path $staging 'skill-bundle'
New-Item -ItemType Directory -Force -Path $packageOutput | Out-Null

try {
    Push-Location $RepoRoot
    try {
        Invoke-Checked uv build --out-dir $packageOutput
    }
    finally {
        Pop-Location
    }

    $wheel = @(Get-ChildItem -File -LiteralPath $packageOutput -Filter '*.whl')
    $sdist = @(Get-ChildItem -File -LiteralPath $packageOutput -Filter '*.tar.gz')
    if ($wheel.Count -ne 1 -or $sdist.Count -ne 1) {
        throw "Expected one wheel and one sdist; found $($wheel.Count) wheel(s) and $($sdist.Count) sdist(s)."
    }

    Copy-Item -LiteralPath $wheel[0].FullName -Destination $OutputDirectory
    Copy-Item -LiteralPath $sdist[0].FullName -Destination $OutputDirectory
    Copy-Item -LiteralPath $desktopInstallerSource -Destination $OutputDirectory

    New-Item -ItemType Directory -Force -Path (Join-Path $marketplaceBundle '.agents\plugins') | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $marketplaceBundle '.claude-plugin') | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $marketplaceBundle 'plugins') | Out-Null
    Copy-Item -LiteralPath (Join-Path $RepoRoot '.agents\plugins\marketplace.json') `
        -Destination (Join-Path $marketplaceBundle '.agents\plugins\marketplace.json')
    Copy-Item -LiteralPath $claudeMarketplacePath `
        -Destination (Join-Path $marketplaceBundle '.claude-plugin\marketplace.json')
    Copy-Item -Recurse -LiteralPath $pluginSource `
        -Destination (Join-Path $marketplaceBundle 'plugins\cmo-agent-bridge')
    Copy-Item -LiteralPath (Join-Path $RepoRoot 'LICENSE') `
        -Destination (Join-Path $marketplaceBundle 'LICENSE')

    $stagedPlugin = Join-Path $marketplaceBundle 'plugins\cmo-agent-bridge'
    $stagedAssets = Join-Path $stagedPlugin 'assets'
    if (Test-Path -LiteralPath $stagedAssets) {
        Get-ChildItem -File -LiteralPath $stagedAssets -Filter '*.whl' | Remove-Item -Force
    }
    Copy-Item -LiteralPath (Join-Path $RepoRoot 'LICENSE') `
        -Destination (Join-Path $stagedPlugin 'LICENSE')

    $pluginZip = Join-Path $OutputDirectory "cmo-agent-bridge-plugin-$Version.zip"
    New-ZipFromDirectory -SourceDirectory $marketplaceBundle -DestinationPath $pluginZip

    New-Item -ItemType Directory -Force -Path $skillBundle | Out-Null
    Copy-Item -Recurse -LiteralPath $skillSource `
        -Destination (Join-Path $skillBundle 'operate-cmo')
    Copy-Item -LiteralPath (Join-Path $RepoRoot 'LICENSE') `
        -Destination (Join-Path $skillBundle 'LICENSE')
    $skillZip = Join-Path $OutputDirectory "operate-cmo-skill-$Version.zip"
    New-ZipFromDirectory -SourceDirectory $skillBundle -DestinationPath $skillZip
}
finally {
    if (Test-Path -LiteralPath $staging) {
        Remove-Item -Recurse -Force -LiteralPath $staging
    }
}

$releaseFiles = @(
    Get-ChildItem -File -LiteralPath $OutputDirectory |
        Where-Object Name -ne 'SHA256SUMS' |
        Sort-Object Name
)
if ($releaseFiles.Count -ne 5) {
    throw "Expected five release artifacts before checksums; found $($releaseFiles.Count)."
}

$checksumLines = foreach ($file in $releaseFiles) {
    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLowerInvariant()
    "$hash  $($file.Name)"
}
$utf8NoBom = New-Object Text.UTF8Encoding($false)
[IO.File]::WriteAllLines(
    (Join-Path $OutputDirectory 'SHA256SUMS'),
    [string[]]$checksumLines,
    $utf8NoBom
)

Write-Host "Release artifacts for v${Version}:"
Get-ChildItem -File -LiteralPath $OutputDirectory |
    Sort-Object Name |
    ForEach-Object { Write-Host "  $($_.Name)" }
