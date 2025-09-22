# Check if Python 3 is installed
if (-not (Get-Command python3 -ErrorAction SilentlyContinue)) {
    Write-Host "Python 3 is not installed. Please install it to continue."
    exit 1
}

# Check if pip is installed
if (-not (Get-Command pip3 -ErrorAction SilentlyContinue)) {
    Write-Host "pip is not installed. Please install it to continue."
    exit 1
}

# Create a virtual environment if it doesn't exist
if (-not (Test-Path ".venv")) {
    Write-Host "Creating virtual environment..."
    python3 -m venv .venv
}

# Activate the virtual environment
# Note: Activating a virtual environment in PowerShell typically involves running the Activate.ps1 script.
# However, for a simple installation script, we might just call python/pip directly from the .venv/Scripts directory.
# For full activation, a user would run: .\.\venv\Scripts\Activate.ps1

# For this script, we'll ensure pip and python commands use the venv executables directly.
$venv_python = Join-Path (Get-Location) ".venv\Scripts\python.exe"
$venv_pip = Join-Path (Get-Location) ".venv\Scripts\pip.exe"

# Install dependencies
Write-Host "Installing dependencies..."
& $venv_pip install -r requirements.txt

Write-Host "Installation complete. To activate the virtual environment, run: .\.\venv\Scripts\Activate.ps1"
Write-Host "Then you can run the application using: & .venv\Scripts\python.exe main.py (after activating the venv)"