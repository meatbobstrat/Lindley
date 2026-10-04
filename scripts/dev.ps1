# Starts the Lindley backend (auto-reload) and the Vite dev server together.
# Ctrl+C stops both.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root 'backend\.venv\Scripts\python.exe'

if (-not (Test-Path $python)) {
    throw "Backend venv not found. Run: cd backend; py -3.13 -m venv .venv; .venv\Scripts\pip install -e "".[dev]"""
}
if (-not (Test-Path (Join-Path $root 'frontend\node_modules'))) {
    throw 'Frontend deps not installed. Run: cd frontend; npm install'
}

$backend = Start-Process -FilePath $python -PassThru -NoNewWindow -WorkingDirectory $root `
    -ArgumentList '-m', 'lindley', '--reload'

try {
    Push-Location (Join-Path $root 'frontend')
    npm run dev
}
finally {
    Pop-Location
    if (-not $backend.HasExited) {
        # uvicorn --reload spawns a child worker; kill the whole tree.
        taskkill /PID $backend.Id /T /F | Out-Null
    }
}
