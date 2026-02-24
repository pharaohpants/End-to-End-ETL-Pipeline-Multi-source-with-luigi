# ============================================================
# setup_scheduler.ps1 - Setup Windows Task Scheduler for ETL
# ============================================================

# Auto-detect path (same folder as this script)
$ScriptPath = Join-Path $PSScriptRoot "run_etl.bat"

if (-not (Test-Path $ScriptPath)) {
    Write-Host "ERROR: run_etl.bat not found at $ScriptPath" -ForegroundColor Red
    exit 1
}

$TaskName = "ETL_Pipeline_XYZ"
$Action   = New-ScheduledTaskAction -Execute $ScriptPath
$Trigger  = New-ScheduledTaskTrigger -Daily -At "02:00"
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "ETL Pipeline - Perusahaan XYZ (Daily 02:00)"

Write-Host "Task created: $TaskName (daily at 02:00)" -ForegroundColor Green
Write-Host "Script path: $ScriptPath"
Write-Host ""
Write-Host "Commands:"
Write-Host "  View   : Get-ScheduledTask -TaskName $TaskName"
Write-Host "  Run    : Start-ScheduledTask -TaskName $TaskName"
Write-Host "  Delete : Unregister-ScheduledTask -TaskName $TaskName"