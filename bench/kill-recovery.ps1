param(
    [ValidateRange(1, 65535)]
    [int]$Port = 18080,
    [ValidateRange(1, 20)]
    [int]$KillRuns = 3
)

$ErrorActionPreference = 'Stop'
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$outDir = Join-Path $PSScriptRoot 'out'
$runStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$buildDir = Join-Path ([System.IO.Path]::GetTempPath()) "scrap-heap-kill-recovery-$PID"
$ownedProcesses = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()

function Assert-PortFree([int]$ListenPort) {
    $listeners = @(Get-NetTCPConnection -LocalPort $ListenPort -State Listen -ErrorAction SilentlyContinue)
    if ($listeners.Count -gt 0) {
        throw "Port $ListenPort is already in use. Choose another -Port; the script will not stop existing processes."
    }

    $probe = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, $ListenPort)
    try {
        $probe.Start()
    }
    catch {
        throw "Port $ListenPort is already in use or unavailable ($($_.Exception.Message)). Choose another -Port; the script will not stop existing processes."
    }
    finally {
        $probe.Stop()
    }
}

function Stop-OwnedProcess([System.Diagnostics.Process]$Process) {
    if ($null -eq $Process) { return }
    try {
        $Process.Refresh()
        if (-not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
            [void]$Process.WaitForExit(5000)
        }
    }
    catch {
        # Cleanup is best effort and only targets a process started by this script.
    }
}

function Wait-ServerReady([System.Diagnostics.Process]$Process, [string]$BaseUrl) {
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    while ($watch.Elapsed.TotalSeconds -lt 15) {
        $Process.Refresh()
        if ($Process.HasExited) { throw "Coordinator exited before becoming ready (exit $($Process.ExitCode))." }
        try {
            $null = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/job" -TimeoutSec 2
            return
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    }
    throw 'Coordinator did not become ready within 15 seconds.'
}

function Read-Job([string]$BaseUrl) {
    return Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/job" -TimeoutSec 3
}

function Run-One([string]$Name, [bool]$KillFirst, [string]$ServerExe, [string]$WorkerExe) {
    $baseUrl = "http://127.0.0.1:$Port"
    $serverLog = Join-Path $outDir "$Name-server.log"
    $serverStdout = Join-Path $outDir "$Name-server.stdout.tmp"
    $workerLog = Join-Path $outDir "$Name-worker.log"
    $workerStderr = Join-Path $outDir "$Name-worker.stderr.tmp"
    $serverProcess = $null
    $workerProcess = $null
    $failure = ''
    $jobId = 0
    $jobFinished = $false
    $done = 0
    $total = 0
    $devices = -1

    Remove-Item -LiteralPath $serverLog, $serverStdout, $workerLog, $workerStderr -Force -ErrorAction SilentlyContinue
    try {
        $serverArgs = @('-addr', ":$Port", '-autostart=false', '-verbose')
        $serverProcess = Start-Process -FilePath $ServerExe -ArgumentList $serverArgs -WorkingDirectory $repoRoot -PassThru -RedirectStandardOutput $serverStdout -RedirectStandardError $serverLog
        $ownedProcesses.Add($serverProcess)
        Wait-ServerReady $serverProcess $baseUrl

        $started = Invoke-RestMethod -Method Post -Uri "$baseUrl/api/run-again" -TimeoutSec 5
        $jobId = [uint32]$started.jobId
        if ($jobId -eq 0) { throw 'POST /api/run-again returned jobId 0.' }

        if ($KillFirst) {
            $beforeWorkers = Read-Job $baseUrl
            if ([int]$beforeWorkers.devices -ne 0) {
                throw "Pre-worker check failed: devices=$($beforeWorkers.devices), expected 0."
            }
        }

        $workerArgs = @('-url', "ws://localhost:$Port/ws/worker", '-devices', '3', '-threads', '2', '-tiles-per-sec', '20', '-name', $Name)
        if ($KillFirst) { $workerArgs += @('-kill-after', '4s') }
        $workerProcess = Start-Process -FilePath $WorkerExe -ArgumentList $workerArgs -WorkingDirectory $repoRoot -PassThru -RedirectStandardOutput $workerLog -RedirectStandardError $workerStderr
        $ownedProcesses.Add($workerProcess)

        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        while ($watch.Elapsed.TotalSeconds -lt 60) {
            $serverProcess.Refresh()
            $workerProcess.Refresh()
            if ($serverProcess.HasExited) { throw "Coordinator exited during job $jobId (exit $($serverProcess.ExitCode))." }
            try {
                $state = Read-Job $baseUrl
                $done = [int]$state.done
                $total = [int]$state.total
                $devices = [int]$state.devices
                if ($total -gt 0 -and $done -eq $total) {
                    $jobFinished = $true
                    break
                }
            }
            catch {
                if ($serverProcess.HasExited) { throw }
            }
            Start-Sleep -Milliseconds 500
        }
        if (-not $jobFinished) { throw "Job $jobId did not finish within 60 seconds (done=$done total=$total)." }

        if ($KillFirst) {
            $settle = [System.Diagnostics.Stopwatch]::StartNew()
            while ($settle.Elapsed.TotalSeconds -lt 5 -and $devices -ne 2) {
                Start-Sleep -Milliseconds 250
                $state = Read-Job $baseUrl
                $devices = [int]$state.devices
                $done = [int]$state.done
                $total = [int]$state.total
            }
        }
    }
    catch {
        $failure = $_.Exception.Message
    }
    finally {
        Stop-OwnedProcess $workerProcess
        Stop-OwnedProcess $serverProcess
        if (Test-Path -LiteralPath $serverStdout) {
            $stdoutLines = @(Get-Content -LiteralPath $serverStdout -ErrorAction SilentlyContinue)
            if ($stdoutLines.Count -gt 0) { Add-Content -LiteralPath $serverLog -Value $stdoutLines }
            Remove-Item -LiteralPath $serverStdout -Force -ErrorAction SilentlyContinue
        }
        if (Test-Path -LiteralPath $workerStderr) {
            $stderrLines = @(Get-Content -LiteralPath $workerStderr -ErrorAction SilentlyContinue)
            if ($stderrLines.Count -gt 0) { Add-Content -LiteralPath $workerLog -Value $stderrLines }
            Remove-Item -LiteralPath $workerStderr -Force -ErrorAction SilentlyContinue
        }
    }

    $lines = @()
    if (Test-Path -LiteralPath $serverLog) { $lines = @(Get-Content -LiteralPath $serverLog) }
    $jobStartedIndex = -1
    $deviceLeftIndex = -1
    $jobDoneLines = @()
    $requeueReasons = @()
    $wall = ''
    $imageHash = ''
    for ($i = 0; $i -lt $lines.Count; $i++) {
        $line = [string]$lines[$i]
        if ($jobStartedIndex -lt 0 -and $line -match "job started id=$jobId(?:\s|$)") { $jobStartedIndex = $i }
        if ($deviceLeftIndex -lt 0 -and $line -match 'device left id=') { $deviceLeftIndex = $i }
        if ($line -match "job done id=$jobId\s") {
            $jobDoneLines += $line
            if ($line -match 'imageHash=([0-9a-fA-F]+)\s+wall=(\S+)') {
                $imageHash = $Matches[1]
                $wall = $Matches[2]
            }
        }
        if ($line -match 'task requeued task=\d+ attempt=\d+ reason=([a-z_]+)') { $requeueReasons += $Matches[1] }
    }

    $orderValid = $jobStartedIndex -ge 0 -and $deviceLeftIndex -ge 0 -and $jobStartedIndex -lt $deviceLeftIndex
    $requeueReasons = @($requeueReasons | Sort-Object -Unique)
    return [pscustomobject]@{
        Run = $Name
        KillRun = $KillFirst
        JobId = $jobId
        Finished = $jobFinished -and $total -gt 0 -and $done -eq $total
        Done = $done
        Total = $total
        Devices = $devices
        RequeueCount = @($lines | Where-Object { $_ -match 'task requeued task=\d+ attempt=\d+ reason=[a-z_]+' }).Count
        RequeueReasons = ($requeueReasons -join ',')
        JobStartedBeforeDeviceLeft = $orderValid
        JobDoneCount = $jobDoneLines.Count
        Wall = $wall
        ImageHash = $imageHash
        ServerLog = $serverLog
        Failure = $failure
        Passed = $false
    }
}

try {
    if ($KillRuns -lt 1) { throw '-KillRuns must be at least 1.' }
    Assert-PortFree $Port
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
    New-Item -ItemType Directory -Path $buildDir -Force | Out-Null

    $serverExe = Join-Path $buildDir 'server.exe'
    $workerExe = Join-Path $buildDir 'mockworker.exe'
    Push-Location $repoRoot
    try {
        & go build -o $serverExe ./cmd/server
        if ($LASTEXITCODE -ne 0) { throw "go build ./cmd/server failed with exit $LASTEXITCODE." }
        & go build -o $workerExe ./cmd/mockworker
        if ($LASTEXITCODE -ne 0) { throw "go build ./cmd/mockworker failed with exit $LASTEXITCODE." }
    }
    finally { Pop-Location }

    $results = [System.Collections.Generic.List[object]]::new()
    $baseline = Run-One "$runStamp-baseline" $false $serverExe $workerExe
    $results.Add($baseline)
    for ($run = 1; $run -le $KillRuns; $run++) {
        $name = '{0}-kill-{1:D2}' -f $runStamp, $run
        $results.Add((Run-One $name $true $serverExe $workerExe))
    }

    foreach ($result in $results) {
        $ok = $result.Finished -and $result.JobStartedBeforeDeviceLeft -and $result.JobDoneCount -eq 1 -and -not $result.Failure
        if ($result.KillRun) {
            $reasons = @($result.RequeueReasons -split ',' | Where-Object { $_ })
            $ok = $ok -and $result.Devices -eq 2 -and $result.RequeueCount -gt 0 -and $reasons -contains 'device_disconnected' -and $baseline.ImageHash -ne '' -and $result.ImageHash -eq $baseline.ImageHash
        }
        $result.Passed = [bool]$ok
    }

    $results | Select-Object Run, Passed, @{Name='Done/Total';Expression={"$($_.Done)/$($_.Total)"}}, Devices, RequeueCount, RequeueReasons, JobStartedBeforeDeviceLeft, JobDoneCount | Format-Table -AutoSize -Wrap
    $detailTable = $results | Select-Object Run, Wall, ImageHash | Format-Table -AutoSize -Wrap | Out-String -Width 240
    Write-Host "Wall time and image hash:`n$detailTable"
    foreach ($result in $results) {
        if ($result.Failure) { Write-Host "$($result.Run) failed: $($result.Failure)" -ForegroundColor Red }
        if (-not $result.JobStartedBeforeDeviceLeft) { Write-Host "$($result.Run) invalid: job started was not logged before device left." -ForegroundColor Red }
        Write-Host "Server log: $($result.ServerLog)"
    }

    $failedRuns = @($results | Where-Object { -not $_.Passed })
    if ($failedRuns.Count -gt 0) { exit 1 }
    Write-Host "Kill-recovery check passed: baseline plus $KillRuns kill run(s); all kill hashes match baseline."
}
catch {
    Write-Error $_
    exit 1
}
finally {
    foreach ($process in $ownedProcesses) { Stop-OwnedProcess $process }
    Remove-Item -LiteralPath $buildDir -Recurse -Force -ErrorAction SilentlyContinue
}
