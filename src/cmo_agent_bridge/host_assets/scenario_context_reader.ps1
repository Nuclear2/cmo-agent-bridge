param(
    [Parameter(Mandatory = $true)]
    [string]$GameRoot,

    [Parameter(Mandatory = $true)]
    [string]$ScenarioPath,

    [Parameter(Mandatory = $true)]
    [string]$PlayerSideGuid
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)

function Write-JsonResult {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value
    )

    [Console]::Out.Write(($Value | ConvertTo-Json -Compress -Depth 4))
}

try {
    Set-Location -LiteralPath $GameRoot

    $commandAssemblyPath = Join-Path -Path $GameRoot -ChildPath "Command.exe"
    if (-not (Test-Path -LiteralPath $commandAssemblyPath -PathType Leaf)) {
        throw "Command.exe was not found in the configured game root."
    }
    if (-not (Test-Path -LiteralPath $ScenarioPath -PathType Leaf)) {
        throw "The active scenario file was not found."
    }

    $assembly = [Reflection.Assembly]::LoadFrom($commandAssemblyPath)
    $containerType = $assembly.GetType("Command_Core.ScenContainer", $true)
    $container = $containerType::LoadFromFile($ScenarioPath)
    $scenarioXml = [string]$container.GetScenarioObject_AsXML()

    $document = New-Object System.Xml.XmlDocument
    $document.PreserveWhitespace = $true
    $document.LoadXml($scenarioXml)
    $root = $document.DocumentElement
    if ($null -eq $root -or ($root.Name -ne "Scenario" -and $root.Name -ne "ContentScenario")) {
        throw "The scenario XML has an unsupported root element."
    }

    $matchingNodes = New-Object System.Collections.Generic.List[System.Xml.XmlNode]
    foreach ($node in $root.SelectNodes("Sides/Side")) {
        $idNode = $node.SelectSingleNode("ID")
        if (
            $null -ne $idNode -and
            [string]::Equals(
                $idNode.InnerText.Trim(),
                $PlayerSideGuid.Trim(),
                [StringComparison]::OrdinalIgnoreCase
            )
        ) {
            $matchingNodes.Add($node)
        }
    }
    if ($matchingNodes.Count -ne 1) {
        throw "The live player side GUID did not identify exactly one side in the scenario file."
    }

    # Deserialize only the live player's side. Other-side briefings remain inaccessible.
    $scenarioType = $assembly.GetType("Command_Core.Scenario", $true)
    $scenario = [Activator]::CreateInstance($scenarioType)
    $scenarioObjectType = $assembly.GetType("Command_Core.ScenarioObject", $true)
    $dictionaryType = [Collections.Concurrent.ConcurrentDictionary``2].MakeGenericType(
        [string],
        $scenarioObjectType
    )
    $dictionary = [Activator]::CreateInstance($dictionaryType)
    $sideType = $assembly.GetType("Command_Core.Side", $true)
    $selectedNode = [Xml.XmlNode]$matchingNodes[0]
    $selectedSide = $sideType::FromXML(
        [ref]$selectedNode,
        [ref]$scenario,
        [ref]$dictionary,
        $null
    )
    if ($null -eq $selectedSide) {
        throw "CMO could not deserialize the live player's side briefing."
    }

    Write-JsonResult ([ordered]@{
        ok = $true
        scenario_description = [string]$container.ScenDescription
        player_side_guid = $PlayerSideGuid.Trim()
        player_side_name = [string]$selectedSide.Name
        side_briefing = [string]$selectedSide.Briefing
        scoring = [ordered]@{
            major_defeat = [int]$selectedSide.Scoring_MajorDefeat
            minor_defeat = [int]$selectedSide.Scoring_MinorDefeat
            average = [int]$selectedSide.Scoring_Average
            minor_victory = [int]$selectedSide.Scoring_MinorVictory
            major_victory = [int]$selectedSide.Scoring_MajorVictory
        }
    })
}
catch {
    Write-JsonResult ([ordered]@{
        ok = $false
        error = $_.Exception.Message
    })
    exit 1
}
