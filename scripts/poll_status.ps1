<#
.SYNOPSIS
    Polls /status on all Raft cluster nodes and prints a formatted table every second.
.EXAMPLE
    .\scripts\poll_status.ps1
    .\scripts\poll_status.ps1 -Interval 2 -Endpoints "http://127.0.0.1:9001/status","http://127.0.0.1:9002/status"
#>

param(
    [string[]]$Endpoints = @(
        "http://127.0.0.1:9001/status",
        "http://127.0.0.1:9002/status",
        "http://127.0.0.1:9003/status"
    ),
    [int]$Interval = 1,
    [int]$Count = 0,
    [switch]$ClearScreen
)

Write-Host "Polling $($Endpoints.Count) node(s) every ${Interval}s (Press Ctrl+C to stop)..." -ForegroundColor Cyan

$iteration = 0

try {
    while ($true) {
        $iteration++
        if ($ClearScreen) {
            Clear-Host
        }

        $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        Write-Host "`n--- [Iteration #$iteration | $timestamp] Raft Cluster Status ---" -ForegroundColor Green

        $table = @()
        foreach ($ep in $Endpoints) {
            $row = [PSCustomObject]@{
                "Node ID"  = "-"
                "State"    = "-"
                "Term"     = "-"
                "VotedFor" = "-"
                "Status"   = "OFFLINE"
                "Endpoint" = $ep
            }

            try {
                $resp = Invoke-RestMethod -Uri $ep -Method Get -TimeoutSec 1 -ErrorAction Stop
                if ($resp) {
                    $votedForStr = "$($resp.votedFor)"
                    if ($resp.votedFor -eq -1) {
                        $votedForStr = "none (-1)"
                    } elseif ($resp.votedFor -eq $resp.id) {
                        $votedForStr = "self ($($resp.id))"
                    }

                    $row."Node ID"  = "Node $($resp.id)"
                    $row."State"    = "$($resp.state)"
                    $row."Term"     = "$($resp.term)"
                    $row."VotedFor" = $votedForStr
                    $row."Status"   = "ONLINE"
                }
            } catch {
                # Keep row marked as OFFLINE
            }

            $table += $row
        }

        $table | Format-Table -AutoSize

        if ($Count -gt 0 -and $iteration -ge $Count) {
            break
        }

        Start-Sleep -Seconds $Interval
    }
} finally {
    Write-Host "`nStopped status polling." -ForegroundColor Yellow
}

