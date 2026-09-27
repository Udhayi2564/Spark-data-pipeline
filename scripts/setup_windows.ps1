$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonPath = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonPath)) {
    throw "Project Python was not found at $PythonPath. Create the virtual environment first."
}

Write-Host "Windows Spark prerequisites are ready."
Write-Host "Python: $PythonPath"