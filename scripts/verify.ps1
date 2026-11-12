[CmdletBinding()]
param(
    [switch]$SkipDockerBuild
)

$ErrorActionPreference = "Stop"

$goVersion = (go env GOVERSION).Trim()
if ($LASTEXITCODE -ne 0) {
    throw "Go is not installed or is not available on PATH."
}
if ($goVersion -ne "go1.26.3") {
    throw "MiniKV declares Go 1.26.3; found $goVersion. Install the declared toolchain before release verification."
}

Write-Host "Downloading Go modules..."
go mod download
if ($LASTEXITCODE -ne 0) { throw "go mod download failed" }

Write-Host "Checking gofmt..."
$goFiles = Get-ChildItem -Path . -Recurse -Filter *.go | ForEach-Object FullName
$unformatted = gofmt -l $goFiles
if ($LASTEXITCODE -ne 0) { throw "gofmt check failed" }
if ($unformatted) { throw "Files need gofmt:`n$($unformatted -join "`n")" }

Write-Host "Running unit and integration tests with race detector..."
go test -race -count=1 ./...
if ($LASTEXITCODE -ne 0) { throw "go test -race failed" }

Write-Host "Running go vet..."
go vet ./...
if ($LASTEXITCODE -ne 0) { throw "go vet failed" }

Write-Host "Building server, client and benchmark packages..."
go build -trimpath ./cmd/server ./cmd/client ./benchmarks
if ($LASTEXITCODE -ne 0) { throw "Go build failed" }

Write-Host "Validating Docker Compose configuration..."
docker compose config --quiet
if ($LASTEXITCODE -ne 0) { throw "docker compose config failed" }

if (-not $SkipDockerBuild) {
    Write-Host "Building MiniKV container image..."
    docker compose build
    if ($LASTEXITCODE -ne 0) { throw "docker compose build failed" }
}

Write-Host "Pre-deployment verification passed. This does not replace a cluster restart, backup/restore, failure and load test on the target host."
