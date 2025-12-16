<# 
.SYNOPSIS
    Build script for Parquet to CSV Windows executable

.DESCRIPTION
    Creates a virtual environment, installs dependencies, and builds
    the PyInstaller package in onedir mode.

.NOTES
    Output: dist\ParquetToCSV\ParquetToCSV.exe
#>

param(
    [switch]$Clean,
    [switch]$SkipVenv
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectRoot) { $ProjectRoot = (Get-Location).Path }

Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host "  Parquet -> CSV : Build Windows" -ForegroundColor Cyan  
Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host ""
Write-Host "Project root: $ProjectRoot"

# Check Python
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    Write-Host "ERROR: Python not found in PATH" -ForegroundColor Red
    exit 1
}
Write-Host "Python: $($python.Source)"

# Clean if requested
if ($Clean) {
    Write-Host "`nCleaning previous build..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force "$ProjectRoot\build" -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force "$ProjectRoot\dist" -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force "$ProjectRoot\build_venv" -ErrorAction SilentlyContinue
}

# Create venv if not skipping
$venvPath = "$ProjectRoot\build_venv"
$venvPython = "$venvPath\Scripts\python.exe"

if (-not $SkipVenv) {
    if (-not (Test-Path $venvPython)) {
        Write-Host "`nCreating virtual environment..." -ForegroundColor Yellow
        python -m venv $venvPath
        if ($LASTEXITCODE -ne 0) { throw "Failed to create venv" }
    }
    
    Write-Host "`nInstalling dependencies..." -ForegroundColor Yellow
    & $venvPython -m pip install --upgrade pip
    & $venvPython -m pip install -r "$ProjectRoot\requirements.txt"
    & $venvPython -m pip install pyinstaller
    if ($LASTEXITCODE -ne 0) { throw "Failed to install dependencies" }
} else {
    $venvPython = "python"
}

# Build with PyInstaller
Write-Host "`nBuilding with PyInstaller..." -ForegroundColor Yellow
Push-Location $ProjectRoot
try {
    & $venvPython -m PyInstaller --clean --noconfirm ParquetToCSV.spec
    if ($LASTEXITCODE -ne 0) { throw "PyInstaller build failed" }
} finally {
    Pop-Location
}

# Verify output
$exePath = "$ProjectRoot\dist\ParquetToCSV\ParquetToCSV.exe"
if (Test-Path $exePath) {
    Write-Host "`n" + "=" * 60 -ForegroundColor Green
    Write-Host "  BUILD SUCCESSFUL" -ForegroundColor Green
    Write-Host "=" * 60 -ForegroundColor Green
    Write-Host ""
    Write-Host "Executable: $exePath" -ForegroundColor Green
    Write-Host ""
    Write-Host "To run: " -NoNewline
    Write-Host ".\dist\ParquetToCSV\ParquetToCSV.exe" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host "ERROR: Build output not found at $exePath" -ForegroundColor Red
    exit 1
}
