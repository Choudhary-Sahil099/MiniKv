[CmdletBinding()]
param(
    [string]$BackupDirectory = "./backups"
)

$ErrorActionPreference = "Stop"

$nodes = @("minikv-a", "minikv-b", "minikv-c")
$volumeByNode = @{
    "minikv-a" = "minikv-demo_minikv_a_data"
    "minikv-b" = "minikv-demo_minikv_b_data"
    "minikv-c" = "minikv-demo_minikv_c_data"
}

New-Item -ItemType Directory -Force -Path $BackupDirectory | Out-Null
$backupPath = (Resolve-Path $BackupDirectory).Path
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backupSucceeded = $false
$stopAttempted = $false

try {
    Write-Host "Stopping MiniKV nodes to take a consistent offline volume backup..."
    $stopAttempted = $true
    docker compose stop $nodes
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose stop failed"
    }

    foreach ($node in $nodes) {
        $volume = $volumeByNode[$node]
        $archive = "minikv-$node-$stamp.tar.gz"
        Write-Host "Backing up $volume to $archive"
        docker run --rm `
            --volume "${volume}:/source:ro" `
            --volume "${backupPath}:/backup" `
            alpine:3.22 `
            tar -czf "/backup/$archive" -C /source .
        if ($LASTEXITCODE -ne 0) {
            throw "Backup failed for $node ($volume)"
        }
    }
    $backupSucceeded = $true
}
finally {
    if ($stopAttempted) {
        Write-Host "Starting MiniKV nodes..."
        docker compose start $nodes
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Could not restart every node automatically; inspect docker compose ps."
        }
    }
}

if ($backupSucceeded) {
    Get-ChildItem -Path $backupPath -Filter "minikv-*$stamp.tar.gz" |
        Get-FileHash -Algorithm SHA256 |
        Format-Table -AutoSize
    Write-Host "Backup completed: $backupPath"
}
