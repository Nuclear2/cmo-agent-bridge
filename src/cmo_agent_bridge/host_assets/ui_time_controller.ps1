param(
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, [int]::MaxValue)]
    [int]$ProcessId,

    [Parameter(Mandatory = $true)]
    [string]$ExpectedExecutable,

    [Parameter(Mandatory = $true)]
    [ValidateRange(0, [long]::MaxValue)]
    [long]$ExpectedCreateTimeUnixMs,

    [Parameter(Mandatory = $true)]
    [ValidateSet("get", "pause", "resume", "set-rate", "play-1x")]
    [string]$Action,

    [ValidateRange(-1, 5)]
    [int]$RateCode = -1
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$script:FailureCode = "HOST_ERROR"
$script:FailureDetails = [ordered]@{}
$script:RunningTransitionAttempted = $false
$script:SafetyPauseRequired = $false

function Write-JsonResult {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value
    )

    [Console]::Out.Write(($Value | ConvertTo-Json -Compress -Depth 8))
}

function Throw-UiFailure {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Code,

        [Parameter(Mandatory = $true)]
        [string]$Message,

        [hashtable]$Details = @{}
    )

    $script:FailureCode = $Code
    $script:FailureDetails = [ordered]@{}
    foreach ($key in $Details.Keys) {
        $script:FailureDetails[$key] = $Details[$key]
    }
    throw [InvalidOperationException]::new($Message)
}

try {
    Add-Type -AssemblyName UIAutomationClient
    Add-Type -AssemblyName UIAutomationTypes
    Add-Type @"
using System;
using System.Runtime.InteropServices;

public static class CmoForegroundWindow
{
    [DllImport("user32.dll")]
    public static extern IntPtr GetForegroundWindow();

    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool IsWindow(IntPtr windowHandle);

    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool SetForegroundWindow(IntPtr windowHandle);
}
"@
}
catch {
    Write-JsonResult ([ordered]@{
        ok = $false
        code = "UI_AUTOMATION_UNAVAILABLE"
        message = $_.Exception.Message
        details = [ordered]@{}
    })
    exit 1
}

function Get-ProcessStartUnixMs {
    param(
        [Parameter(Mandatory = $true)]
        [Diagnostics.Process]$Process
    )

    $utc = $Process.StartTime.ToUniversalTime()
    return ([DateTimeOffset]::new($utc)).ToUnixTimeMilliseconds()
}

function Assert-ProcessIdentity {
    $matches = @(Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)
    if ($matches.Count -ne 1) {
        Throw-UiFailure `
            -Code "PROCESS_IDENTITY_MISMATCH" `
            -Message "The selected CMO process no longer exists." `
            -Details @{ pid = $ProcessId; count = $matches.Count }
    }

    [Diagnostics.Process]$process = $matches[0]
    $process.Refresh()
    $expectedPath = [IO.Path]::GetFullPath($ExpectedExecutable)
    $actualPath = [IO.Path]::GetFullPath([string]$process.Path)
    $samePath = [string]::Equals($actualPath, $expectedPath, [StringComparison]::OrdinalIgnoreCase)
    $actualStartMs = Get-ProcessStartUnixMs -Process $process
    if (-not $samePath -or $actualStartMs -ne $ExpectedCreateTimeUnixMs) {
        Throw-UiFailure `
            -Code "PROCESS_IDENTITY_MISMATCH" `
            -Message "The selected PID does not match the expected CMO process identity." `
            -Details @{
                pid = $ProcessId
                expected_executable = $expectedPath
                observed_executable = $actualPath
                expected_create_time_unix_ms = $ExpectedCreateTimeUnixMs
                observed_create_time_unix_ms = $actualStartMs
            }
    }
    return $process
}

function Get-UniqueControl {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Root,

        [Parameter(Mandatory = $true)]
        [string]$AutomationId,

        [Parameter(Mandatory = $true)]
        [Windows.Automation.ControlType]$ControlType
    )

    $condition = New-Object Windows.Automation.PropertyCondition(
        [Windows.Automation.AutomationElement]::AutomationIdProperty,
        $AutomationId
    )
    $matches = $Root.FindAll([Windows.Automation.TreeScope]::Descendants, $condition)
    if ($matches.Count -ne 1) {
        Throw-UiFailure `
            -Code "CONTROL_AMBIGUOUS" `
            -Message "CMO did not expose exactly one required UI Automation control." `
            -Details @{ automation_id = $AutomationId; count = $matches.Count }
    }

    [Windows.Automation.AutomationElement]$element = $matches.Item(0)
    if (
        $element.Current.ProcessId -ne $ProcessId -or
        $element.Current.ControlType -ne $ControlType -or
        -not $element.Current.IsEnabled
    ) {
        Throw-UiFailure `
            -Code "CONTROL_INVALID" `
            -Message "The required CMO UI Automation control has an invalid identity or state." `
            -Details @{
                automation_id = $AutomationId
                observed_process_id = $element.Current.ProcessId
                observed_control_type = $element.Current.ControlType.ProgrammaticName
                is_enabled = $element.Current.IsEnabled
            }
    }
    return $element
}

function Get-RunControl {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Root,

        [ValidateRange(1, 20)]
        [int]$RetryCount = 20
    )

    $playCondition = New-Object Windows.Automation.PropertyCondition(
        [Windows.Automation.AutomationElement]::AutomationIdProperty,
        "PlayButton"
    )
    $pauseCondition = New-Object Windows.Automation.PropertyCondition(
        [Windows.Automation.AutomationElement]::AutomationIdProperty,
        "PauseButton"
    )
    $condition = [Windows.Automation.OrCondition]::new(
        [Windows.Automation.Condition[]]@($playCondition, $pauseCondition)
    )
    $lastCount = 0
    for ($attempt = 0; $attempt -lt $RetryCount; $attempt++) {
        $matches = $Root.FindAll([Windows.Automation.TreeScope]::Descendants, $condition)
        $lastCount = $matches.Count
        if ($matches.Count -eq 1) {
            [Windows.Automation.AutomationElement]$element = $matches.Item(0)
            if (
                $element.Current.ProcessId -ne $ProcessId -or
                $element.Current.ControlType -ne [Windows.Automation.ControlType]::Button -or
                -not $element.Current.IsEnabled
            ) {
                Throw-UiFailure `
                    -Code "CONTROL_INVALID" `
                    -Message "CMO's run-state control has an invalid identity or state." `
                    -Details @{
                        automation_id = [string]$element.Current.AutomationId
                        observed_process_id = $element.Current.ProcessId
                        observed_control_type = $element.Current.ControlType.ProgrammaticName
                        is_enabled = $element.Current.IsEnabled
                    }
            }
            return $element
        }
        if ($attempt -lt ($RetryCount - 1)) {
            Start-Sleep -Milliseconds 50
        }
    }
    Throw-UiFailure `
        -Code "CONTROL_AMBIGUOUS" `
        -Message "CMO did not expose a run-state control after its UI transition settled." `
        -Details @{
            automation_ids = @("PlayButton", "PauseButton")
            count = $lastCount
        }
}

function Get-WindowContext {
    [Diagnostics.Process]$process = Assert-ProcessIdentity
    $process.Refresh()
    $handle = $process.MainWindowHandle
    if ($handle -eq [IntPtr]::Zero) {
        Throw-UiFailure `
            -Code "WINDOW_NOT_FOUND" `
            -Message "The selected CMO process has no accessible main window." `
            -Details @{ pid = $ProcessId }
    }

    $root = [Windows.Automation.AutomationElement]::FromHandle($handle)
    if ($null -eq $root -or $root.Current.ProcessId -ne $ProcessId) {
        Throw-UiFailure `
            -Code "WINDOW_IDENTITY_MISMATCH" `
            -Message "The CMO main window does not belong to the selected process." `
            -Details @{ pid = $ProcessId; window_handle = $handle.ToInt64() }
    }
    if (-not $root.Current.IsEnabled) {
        Throw-UiFailure `
            -Code "MODAL_WINDOW" `
            -Message "CMO's main window is disabled by a modal window; time control was not attempted." `
            -Details @{ pid = $ProcessId; window_handle = $handle.ToInt64() }
    }

    $runControl = Get-RunControl -Root $root
    $combo = Get-UniqueControl `
        -Root $root `
        -AutomationId "TimeComboBox" `
        -ControlType ([Windows.Automation.ControlType]::ComboBox)
    return [ordered]@{
        process = $process
        root = $root
        run_control = $runControl
        combo = $combo
        handle = $handle
    }
}

function Get-StateClassification {
    param([AllowEmptyString()][string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }
    $normalized = [regex]::Replace($Text.Trim().ToLowerInvariant(), "\s+", " ")
    if (
        $normalized -match "^start(?:\s|/|$)" -or
        $normalized -match "^resume(?:\s|$)" -or
        $normalized.Contains("start progressing time")
    ) {
        return "paused"
    }
    if (
        $normalized -match "^pause(?:\s|/|$)" -or
        $normalized -match "^stop(?:\s|$)" -or
        $normalized.Contains("pause progressing time") -or
        $normalized.Contains("stop progressing time")
    ) {
        return "running"
    }
    return $null
}

function Get-RunState {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$RunControl
    )

    $automationId = [string]$RunControl.Current.AutomationId
    $identityClassification = switch ($automationId) {
        "PlayButton" { "paused" }
        "PauseButton" { "running" }
        default { $null }
    }
    $observed = @(
        [string]$RunControl.Current.HelpText,
        [string]$RunControl.Current.Name,
        [string]$RunControl.Current.ItemStatus
    )
    $classifications = New-Object System.Collections.Generic.HashSet[string]
    if ($null -ne $identityClassification) {
        [void]$classifications.Add($identityClassification)
    }
    foreach ($text in $observed) {
        $classification = Get-StateClassification -Text $text
        if ($null -ne $classification) {
            [void]$classifications.Add($classification)
        }
    }
    if ($classifications.Count -ne 1) {
        Throw-UiFailure `
            -Code "STATE_UNKNOWN" `
            -Message "CMO's run-state control did not identify one unambiguous run state." `
            -Details @{
                automation_id = $automationId
                observed_text = $observed
                classifications = @($classifications)
            }
    }
    return @($classifications)[0]
}

function ConvertTo-RateCode {
    param([AllowEmptyString()][string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }
    $normalized = $Text.Trim().ToLowerInvariant().Replace([char]0x00D7, [char]0x0078)
    $normalized = [regex]::Replace($normalized, "[\s\(\)\[\]\{\}_\-/:]+", "")
    switch -Regex ($normalized) {
        "^(x1|1x|1)$" { return 0 }
        "^(x2|2x|2)$" { return 1 }
        "^(x5|5x|5)$" { return 2 }
        "^(x15|15x|15)$" { return 3 }
        "^(x30|30x|30|turbo|x30turbo|30xturbo|turbox30|turbo30x)$" { return 4 }
        "^(x150|150x|150|coarse|x150coarse|150xcoarse|coarsex150|coarse150x)$" {
            return 5
        }
        default { return $null }
    }
}

function ConvertAutomationIdTo-RateCode {
    param([AllowEmptyString()][string]$AutomationId)

    switch ($AutomationId) {
        "TimeItem1x" { return 0 }
        "TimeItem2x" { return 1 }
        "TimeItem5x" { return 2 }
        "TimeItem15x" { return 3 }
        "TimeItemTurbo" { return 4 }
        "TimeItemDoubleFlame" { return 5 }
        default { return $null }
    }
}

function Get-RateCode {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$ComboBox
    )

    $observed = New-Object System.Collections.Generic.List[string]
    $codes = New-Object System.Collections.Generic.HashSet[int]
    $selectionObject = $null
    if (
        $ComboBox.TryGetCurrentPattern(
            [Windows.Automation.SelectionPattern]::Pattern,
            [ref]$selectionObject
        )
    ) {
        $selection = ([Windows.Automation.SelectionPattern]$selectionObject).Current.GetSelection()
        if ($selection.Count -gt 1) {
            Throw-UiFailure `
                -Code "RATE_UNKNOWN" `
                -Message "CMO's TimeComboBox reported multiple selected rates." `
                -Details @{ selected_count = $selection.Count }
        }
        if ($selection.Count -eq 1) {
            $text = [string]$selection[0].Current.Name
            [void]$observed.Add($text)
            $code = ConvertAutomationIdTo-RateCode `
                -AutomationId ([string]$selection[0].Current.AutomationId)
            if ($null -eq $code) {
                $code = ConvertTo-RateCode -Text $text
            }
            if ($null -ne $code) {
                [void]$codes.Add([int]$code)
            }
        }
    }

    $valueObject = $null
    if (
        $ComboBox.TryGetCurrentPattern(
            [Windows.Automation.ValuePattern]::Pattern,
            [ref]$valueObject
        )
    ) {
        $text = [string]([Windows.Automation.ValuePattern]$valueObject).Current.Value
        [void]$observed.Add($text)
        $code = ConvertTo-RateCode -Text $text
        if ($null -ne $code) {
            [void]$codes.Add([int]$code)
        }
    }

    foreach ($text in @(
        [string]$ComboBox.Current.Name,
        [string]$ComboBox.Current.ItemStatus,
        [string]$ComboBox.Current.HelpText
    )) {
        [void]$observed.Add($text)
        $code = ConvertTo-RateCode -Text $text
        if ($null -ne $code) {
            [void]$codes.Add([int]$code)
        }
    }
    if ($codes.Count -ne 1) {
        Throw-UiFailure `
            -Code "RATE_UNKNOWN" `
            -Message "CMO's TimeComboBox did not identify one unambiguous time rate." `
            -Details @{ observed_text = @($observed); rate_codes = @($codes) }
    }
    return @($codes)[0]
}

function Get-InvokePattern {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Element,

        [Parameter(Mandatory = $true)]
        [string]$AutomationId
    )

    $patternObject = $null
    if (
        -not $Element.TryGetCurrentPattern(
            [Windows.Automation.InvokePattern]::Pattern,
            [ref]$patternObject
        )
    ) {
        Throw-UiFailure `
            -Code "PATTERN_UNAVAILABLE" `
            -Message "A required CMO button does not expose InvokePattern." `
            -Details @{ automation_id = $AutomationId }
    }
    return [Windows.Automation.InvokePattern]$patternObject
}

function Wait-RunState {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Root,

        [Parameter(Mandatory = $true)]
        [ValidateSet("paused", "running")]
        [string]$ExpectedState
    )

    $lastObserved = $null
    $lastTransitionError = $null
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        try {
            $observedControl = Get-RunControl -Root $Root -RetryCount 1
            $lastObserved = Get-RunState -RunControl $observedControl
            if ($lastObserved -eq $ExpectedState) {
                return
            }
        }
        catch {
            if ($script:FailureCode -notin @("CONTROL_AMBIGUOUS", "STATE_UNKNOWN")) {
                throw
            }
            $lastTransitionError = $_.Exception.Message
        }
        Start-Sleep -Milliseconds 50
    }
    Throw-UiFailure `
        -Code "ACTION_NOT_OBSERVED" `
        -Message "CMO did not reach the requested run state after UI Automation invocation." `
        -Details @{
            expected_state = $ExpectedState
            observed_state = $lastObserved
            transition_error = $lastTransitionError
        }
}

function Ensure-RunState {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Root,

        [Parameter(Mandatory = $true)]
        [ValidateSet("paused", "running")]
        [string]$ExpectedState
    )

    $runControl = Get-RunControl -Root $Root
    $current = Get-RunState -RunControl $runControl
    if ($current -eq $ExpectedState) {
        return
    }
    $invoke = Get-InvokePattern `
        -Element $runControl `
        -AutomationId ([string]$runControl.Current.AutomationId)
    $invoke.Invoke()
    Wait-RunState -Root $Root -ExpectedState $ExpectedState
}

function Wait-TimeRate {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Root,

        [Parameter(Mandatory = $true)]
        [ValidateRange(0, 5)]
        [int]$ExpectedRateCode
    )

    $lastObserved = $null
    $lastTransitionError = $null
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        try {
            $combo = Get-UniqueControl `
                -Root $Root `
                -AutomationId "TimeComboBox" `
                -ControlType ([Windows.Automation.ControlType]::ComboBox)
            $lastObserved = Get-RateCode -ComboBox $combo
            if ($lastObserved -eq $ExpectedRateCode) {
                return
            }
        }
        catch {
            if ($script:FailureCode -notin @("CONTROL_AMBIGUOUS", "RATE_UNKNOWN")) {
                throw
            }
            $lastTransitionError = $_.Exception.Message
        }
        Start-Sleep -Milliseconds 50
    }
    Throw-UiFailure `
        -Code "ACTION_NOT_OBSERVED" `
        -Message "CMO did not reach the requested time rate after UI Automation invocation." `
        -Details @{
            expected_rate_code = $ExpectedRateCode
            observed_rate_code = $lastObserved
            transition_error = $lastTransitionError
        }
}

function Get-ElementRuntimeKey {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Element
    )

    $runtimeId = $Element.GetRuntimeId()
    if ($null -eq $runtimeId -or $runtimeId.Count -eq 0) {
        Throw-UiFailure `
            -Code "CONTROL_INVALID" `
            -Message "A CMO rate item has no stable UI Automation runtime identity."
    }
    return [string]::Join(".", $runtimeId)
}

function Add-MatchingRateItems {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElementCollection]$Elements,

        [Parameter(Mandatory = $true)]
        [int]$ExpectedRateCode,

        [Parameter(Mandatory = $true)]
        [Collections.Generic.Dictionary[string, Windows.Automation.AutomationElement]]$Matches
    )

    foreach ($element in $Elements) {
        if ($element.Current.ProcessId -ne $ProcessId -or -not $element.Current.IsEnabled) {
            continue
        }
        $selectionItem = $null
        if (
            -not $element.TryGetCurrentPattern(
                [Windows.Automation.SelectionItemPattern]::Pattern,
                [ref]$selectionItem
            )
        ) {
            continue
        }
        $code = ConvertAutomationIdTo-RateCode `
            -AutomationId ([string]$element.Current.AutomationId)
        if ($null -eq $code) {
            $code = ConvertTo-RateCode -Text ([string]$element.Current.Name)
        }
        if ($null -eq $code -or [int]$code -ne $ExpectedRateCode) {
            continue
        }
        $key = Get-ElementRuntimeKey -Element $element
        if (-not $Matches.ContainsKey($key)) {
            $Matches.Add($key, $element)
        }
    }
}

function Set-TimeRate {
    param(
        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$Root,

        [Parameter(Mandatory = $true)]
        [Windows.Automation.AutomationElement]$ComboBox,

        [Parameter(Mandatory = $true)]
        [ValidateRange(0, 5)]
        [int]$ExpectedRateCode
    )

    $current = Get-RateCode -ComboBox $ComboBox
    if ($current -eq $ExpectedRateCode) {
        return
    }

    $expandObject = $null
    if (
        -not $ComboBox.TryGetCurrentPattern(
            [Windows.Automation.ExpandCollapsePattern]::Pattern,
            [ref]$expandObject
        )
    ) {
        Throw-UiFailure `
            -Code "PATTERN_UNAVAILABLE" `
            -Message "CMO's TimeComboBox does not expose ExpandCollapsePattern."
    }
    $expand = [Windows.Automation.ExpandCollapsePattern]$expandObject
    $matches = New-Object `
        "Collections.Generic.Dictionary[string,Windows.Automation.AutomationElement]"
    try {
        if ($expand.Current.ExpandCollapseState -ne [Windows.Automation.ExpandCollapseState]::Expanded) {
            $expand.Expand()
            Start-Sleep -Milliseconds 75
        }

        $comboElements = $ComboBox.FindAll(
            [Windows.Automation.TreeScope]::Descendants,
            [Windows.Automation.Condition]::TrueCondition
        )
        Add-MatchingRateItems `
            -Elements $comboElements `
            -ExpectedRateCode $ExpectedRateCode `
            -Matches $matches

        $processCondition = New-Object Windows.Automation.PropertyCondition(
            [Windows.Automation.AutomationElement]::ProcessIdProperty,
            $ProcessId
        )
        $desktopElements = [Windows.Automation.AutomationElement]::RootElement.FindAll(
            [Windows.Automation.TreeScope]::Descendants,
            $processCondition
        )
        Add-MatchingRateItems `
            -Elements $desktopElements `
            -ExpectedRateCode $ExpectedRateCode `
            -Matches $matches

        if ($matches.Count -ne 1) {
            Throw-UiFailure `
                -Code "CONTROL_AMBIGUOUS" `
                -Message "CMO did not expose exactly one selectable item for the requested time rate." `
                -Details @{ rate_code = $ExpectedRateCode; count = $matches.Count }
        }
        $target = @($matches.Values)[0]
        $selectionObject = $null
        if (
            -not $target.TryGetCurrentPattern(
                [Windows.Automation.SelectionItemPattern]::Pattern,
                [ref]$selectionObject
            )
        ) {
            Throw-UiFailure `
                -Code "PATTERN_UNAVAILABLE" `
                -Message "The selected CMO rate item does not expose SelectionItemPattern."
        }
        ([Windows.Automation.SelectionItemPattern]$selectionObject).Select()
        Start-Sleep -Milliseconds 75
    }
    finally {
        if ($expand.Current.ExpandCollapseState -eq [Windows.Automation.ExpandCollapseState]::Expanded) {
            $expand.Collapse()
        }
    }

    Wait-TimeRate -Root $Root -ExpectedRateCode $ExpectedRateCode
}

function Get-Snapshot {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Context
    )

    $state = Get-RunState -RunControl $Context.run_control
    $rate = Get-RateCode -ComboBox $Context.combo
    $multipliers = @(1, 2, 5, 15, 30, 150)
    $startMs = Get-ProcessStartUnixMs -Process $Context.process
    return [ordered]@{
        ok = $true
        pid = $ProcessId
        process_start_time_unix_ms = $startMs
        executable = [IO.Path]::GetFullPath([string]$Context.process.Path)
        window_handle = $Context.handle.ToInt64()
        window_title = [string]$Context.root.Current.Name
        state = $state
        rate_code = [int]$rate
        rate_multiplier = [int]$multipliers[[int]$rate]
    }
}

function Restore-OriginalForeground {
    param(
        [Parameter(Mandatory = $true)]
        [IntPtr]$OriginalForeground,

        [Parameter(Mandatory = $true)]
        [IntPtr]$CmoWindow
    )

    if (
        $OriginalForeground -eq [IntPtr]::Zero -or
        $OriginalForeground -eq $CmoWindow -or
        -not [CmoForegroundWindow]::IsWindow($OriginalForeground)
    ) {
        return
    }
    $currentForeground = [CmoForegroundWindow]::GetForegroundWindow()
    if ($currentForeground -ne $CmoWindow) {
        return
    }

    if (-not [CmoForegroundWindow]::SetForegroundWindow($OriginalForeground)) {
        try {
            $originalRoot = [Windows.Automation.AutomationElement]::FromHandle($OriginalForeground)
            if ($null -ne $originalRoot) {
                $originalRoot.SetFocus()
            }
        }
        catch {
            # Foreground restoration is best-effort and must not obscure a verified time action.
        }
    }
}

$originalForeground = [IntPtr]::Zero
$context = $null
try {
    $originalForeground = [CmoForegroundWindow]::GetForegroundWindow()
    $context = Get-WindowContext
    $originalHandle = $context.handle
    switch ($Action) {
        "get" { }
        "pause" {
            Ensure-RunState -Root $context.root -ExpectedState "paused"
        }
        "resume" {
            $state = Get-RunState -RunControl $context.run_control
            if ($RateCode -ge 0) {
                Set-TimeRate -Root $context.root -ComboBox $context.combo -ExpectedRateCode $RateCode
            }
            if ($state -eq "paused") {
                $script:SafetyPauseRequired = $true
                $script:RunningTransitionAttempted = $true
            }
            Ensure-RunState -Root $context.root -ExpectedState "running"
        }
        "set-rate" {
            if ($RateCode -lt 0) {
                Throw-UiFailure `
                    -Code "INVALID_ARGUMENT" `
                    -Message "set-rate requires a time rate code."
            }
            Set-TimeRate -Root $context.root -ComboBox $context.combo -ExpectedRateCode $RateCode
        }
        "play-1x" {
            $state = Get-RunState -RunControl $context.run_control
            $rate = Get-RateCode -ComboBox $context.combo
            if ($state -ne "running" -or $rate -ne 0) {
                $playAtOne = Get-UniqueControl `
                    -Root $context.root `
                    -AutomationId "PlayButtonAt1Time" `
                    -ControlType ([Windows.Automation.ControlType]::Button)
                $invoke = Get-InvokePattern -Element $playAtOne -AutomationId "PlayButtonAt1Time"
                $script:SafetyPauseRequired = ($state -eq "paused")
                $script:RunningTransitionAttempted = $true
                $invoke.Invoke()
                Wait-RunState -Root $context.root -ExpectedState "running"
                Wait-TimeRate -Root $context.root -ExpectedRateCode 0
            }
        }
    }

    $verified = Get-WindowContext
    if ($verified.handle -ne $originalHandle) {
        Throw-UiFailure `
            -Code "WINDOW_IDENTITY_MISMATCH" `
            -Message "CMO's main window changed during UI time control." `
            -Details @{
                original_window_handle = $originalHandle.ToInt64()
                observed_window_handle = $verified.handle.ToInt64()
            }
    }
    $snapshot = Get-Snapshot -Context $verified
    if ($Action -eq "pause" -and $snapshot.state -ne "paused") {
        Throw-UiFailure `
            -Code "ACTION_NOT_OBSERVED" `
            -Message "CMO was not paused after the pause action."
    }
    if ($Action -eq "resume" -and $snapshot.state -ne "running") {
        Throw-UiFailure `
            -Code "ACTION_NOT_OBSERVED" `
            -Message "CMO was not running after the resume action."
    }
    if ($Action -eq "set-rate" -and $snapshot.rate_code -ne $RateCode) {
        Throw-UiFailure `
            -Code "ACTION_NOT_OBSERVED" `
            -Message "CMO did not retain the requested time rate."
    }
    if (
        $Action -eq "play-1x" -and
        ($snapshot.state -ne "running" -or $snapshot.rate_code -ne 0)
    ) {
        Throw-UiFailure `
            -Code "ACTION_NOT_OBSERVED" `
            -Message "CMO did not start running at 1x."
    }
    Restore-OriginalForeground `
        -OriginalForeground $originalForeground `
        -CmoWindow $verified.handle
    Write-JsonResult $snapshot
}
catch {
    $primaryException = $_.Exception
    $primaryCode = $script:FailureCode
    $primaryDetails = [ordered]@{}
    foreach ($key in $script:FailureDetails.Keys) {
        $primaryDetails[$key] = $script:FailureDetails[$key]
    }
    if ($script:RunningTransitionAttempted -and $script:SafetyPauseRequired) {
        $primaryDetails["action_may_have_applied"] = $true
        $primaryDetails["safety_pause_attempted"] = $true
        try {
            $recovery = Get-WindowContext
            Ensure-RunState -Root $recovery.root -ExpectedState "paused"
            $recoveryVerified = Get-WindowContext
            $recoveryState = Get-RunState -RunControl $recoveryVerified.run_control
            $primaryDetails["safety_pause_verified"] = ($recoveryState -eq "paused")
        }
        catch {
            $primaryDetails["safety_pause_verified"] = $false
            $primaryDetails["safety_pause_error"] = $_.Exception.Message
            $primaryDetails["safety_pause_helper_code"] = $script:FailureCode
        }
    }
    if ($null -ne $originalForeground -and $null -ne $context) {
        Restore-OriginalForeground `
            -OriginalForeground $originalForeground `
            -CmoWindow $context.handle
    }
    Write-JsonResult ([ordered]@{
        ok = $false
        code = $primaryCode
        message = $primaryException.Message
        details = $primaryDetails
    })
    exit 1
}
