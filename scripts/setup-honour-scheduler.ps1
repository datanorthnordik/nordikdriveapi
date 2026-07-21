param(
    [string]$ProjectId = "planar-ray-472112-e8",
    [string]$Region = "us-west1",
    [string]$SchedulerLocation = "us-west1",
    [string]$ServiceName = "nordikdriveapi",
    [string]$JobName = "honour-daily",
    [string]$TimeZone = "America/Toronto",
    [string]$HonourJobSecret,
    [switch]$RunNow
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:GCloudCommand = $null

function New-RandomSecret {
    $bytes = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $secret = [Convert]::ToBase64String($bytes).TrimEnd("=") -replace "\+", "-" -replace "/", "_"
    return $secret
}

function Invoke-GCloud {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    Write-Host "gcloud $($Args -join ' ')" -ForegroundColor Cyan
    & $script:GCloudCommand @Args
    if ($LASTEXITCODE -ne 0) {
        throw "gcloud command failed with exit code $LASTEXITCODE"
    }
}

function Ensure-GCloudApiEnabled {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProjectId,
        [Parameter(Mandatory = $true)]
        [string]$ServiceName
    )

    $isEnabled = (& $script:GCloudCommand services list `
        --project $ProjectId `
        --enabled `
        --filter "config.name=$ServiceName" `
        --format "value(config.name)")

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to verify whether $ServiceName is enabled for project $ProjectId."
    }

    if ([string]::IsNullOrWhiteSpace($isEnabled)) {
        Write-Host "Enabling required API: $ServiceName" -ForegroundColor Yellow
        Invoke-GCloud -Args @(
            "services", "enable", $ServiceName,
            "--project", $ProjectId
        )
    }
}

if (Get-Command gcloud.cmd -ErrorAction SilentlyContinue) {
    $script:GCloudCommand = "gcloud.cmd"
} elseif (Get-Command gcloud -ErrorAction SilentlyContinue) {
    $script:GCloudCommand = "gcloud"
} else {
    throw "gcloud CLI is required but was not found in PATH."
}

if ([string]::IsNullOrWhiteSpace($HonourJobSecret)) {
    $HonourJobSecret = New-RandomSecret
    Write-Host "Generated HONOUR_JOB_SECRET for this setup:" -ForegroundColor Yellow
    Write-Host $HonourJobSecret -ForegroundColor Yellow
}

$serviceUrl = (& $script:GCloudCommand run services describe $ServiceName `
    --project $ProjectId `
    --region $Region `
    --format "value(status.url)")

if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($serviceUrl)) {
    throw "Unable to resolve Cloud Run service URL for $ServiceName in $Region."
}

$targetUri = "$serviceUrl/api/internal/jobs/honour/run"
$headers = "X-Honour-Job-Secret=$HonourJobSecret,Content-Type=application/json"
$envVars = "APP_TIMEZONE=$TimeZone,HONOUR_JOB_SECRET=$HonourJobSecret"

Invoke-GCloud -Args @(
    "run", "services", "update", $ServiceName,
    "--project", $ProjectId,
    "--region", $Region,
    "--update-env-vars", $envVars
)

Ensure-GCloudApiEnabled -ProjectId $ProjectId -ServiceName "cloudscheduler.googleapis.com"

$jobExists = $false
$jobNames = (& $script:GCloudCommand scheduler jobs list `
    --project $ProjectId `
    --location $SchedulerLocation `
    --format "value(name)")

if ($LASTEXITCODE -ne 0) {
    throw "Unable to list Cloud Scheduler jobs for project $ProjectId in $SchedulerLocation."
}

foreach ($existingJobName in @($jobNames)) {
    $normalizedJobName = [string]$existingJobName
    if (
        $normalizedJobName -eq $JobName -or
        $normalizedJobName -like "*/$JobName"
    ) {
        $jobExists = $true
        break
    }
}

$headerFlag = if ($jobExists) { "--update-headers" } else { "--headers" }

$commonSchedulerArgs = @(
    "scheduler", "jobs",
    $(if ($jobExists) { "update" } else { "create" }),
    "http", $JobName,
    "--project", $ProjectId,
    "--location", $SchedulerLocation,
    "--schedule", "0 0 * * *",
    "--time-zone", $TimeZone,
    "--uri", $targetUri,
    "--http-method", "POST",
    $headerFlag, $headers,
    "--message-body", "{}",
    "--attempt-deadline", "300s",
    "--max-retry-attempts", "3",
    "--min-backoff", "30s",
    "--max-backoff", "600s"
)

Invoke-GCloud -Args $commonSchedulerArgs

if ($RunNow) {
    Invoke-GCloud -Args @(
        "scheduler", "jobs", "run", $JobName,
        "--project", $ProjectId,
        "--location", $SchedulerLocation
    )
}

Write-Host ""
Write-Host "Honour scheduler setup complete." -ForegroundColor Green
Write-Host "Service URL: $serviceUrl"
Write-Host "Scheduler job: $JobName"
Write-Host "Time zone: $TimeZone"
if (-not $RunNow) {
    Write-Host "To test immediately, run:" -ForegroundColor Green
    Write-Host "gcloud scheduler jobs run $JobName --project $ProjectId --location $SchedulerLocation"
}
