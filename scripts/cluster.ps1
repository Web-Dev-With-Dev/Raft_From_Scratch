<#
.SYNOPSIS
    Launches or stops a 3-node local Raft cluster on Windows.
.EXAMPLE
    .\scripts\cluster.ps1 start
    .\scripts\cluster.ps1 stop
#>

param(
    [ValidateSet("start", "stop", "build", "status")]
    [string]$Action = "start",

    [string]$Config = "cluster.json"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
Set-Location $RootDir

$BinDir = Join-Path $RootDir "bin"
$NodeExe = Join-Path $BinDir "node.exe"

function Build-Node {
    Write-Host "==> Building Raft node binary..." -ForegroundColor Cyan
    if (!(Test-Path $BinDir)) {
        New-Item -ItemType Directory -Path $BinDir | Out-Null
    }
    go build -o $NodeExe ./cmd/node
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed with exit code $LASTEXITCODE"
        exit 1
    }
    Write-Host "==> Built: $NodeExe" -ForegroundColor Green
}

function Start-Cluster {
    Build-Node

    if (!(Test-Path $Config)) {
        Write-Error "Configuration file '$Config' not found."
        exit 1
    }

    Write-Host "==> Launching 3-node Raft cluster (Nodes 1, 2, 3) using $Config..." -ForegroundColor Green
    Write-Host "==> Press Ctrl+C at any time to stop all nodes." -ForegroundColor Yellow
    Write-Host "----------------------------------------------------------------------"

    $nodes = @(
        @{ ID = 1; Name = "Node 1" },
        @{ ID = 2; Name = "Node 2" },
        @{ ID = 3; Name = "Node 3" }
    )

    $processes = @()
    try {
        foreach ($node in $nodes) {
            $p = Start-Process -FilePath $NodeExe -ArgumentList "-id $($node.ID) -config $Config" -NoNewWindow -PassThru
            $processes += $p
        }

        # Wait for all processes to finish or user to interrupt
        $processes | Wait-Process
    }
    finally {
        Write-Host "`n==> Stopping cluster nodes..." -ForegroundColor Yellow
        foreach ($p in $processes) {
            if ($p -and -not $p.HasExited) {
                Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
            }
        }
        Stop-Process -Name "node" -Force -ErrorAction SilentlyContinue
        Write-Host "==> All cluster nodes stopped." -ForegroundColor Green
    }
}

function Stop-Cluster {
    Write-Host "==> Stopping any running node processes..." -ForegroundColor Yellow
    Get-Process -Name "node" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "==> Done." -ForegroundColor Green
}

function Show-Status {
    $procs = Get-Process -Name "node" -ErrorAction SilentlyContinue
    if ($procs) {
        Write-Host "Running cluster processes:" -ForegroundColor Green
        $procs | Format-Table Id, ProcessName, StartTime, WorkingSet
    } else {
        Write-Host "No active cluster nodes running." -ForegroundColor Yellow
    }
}

switch ($Action.ToLower()) {
    "start"  { Start-Cluster }
    "stop"   { Stop-Cluster }
    "build"  { Build-Node }
    "status" { Show-Status }
}
