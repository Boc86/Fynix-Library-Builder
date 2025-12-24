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
# Define GitHub raw URL for Dev branch
$repoUrl = 'https://raw.githubusercontent.com/Boc86/Fynix-Library-Builder/Dev'

# Create installation directory in user's profile
$installDir = Join-Path $env:USERPROFILE '.fynix-library-builder'
if (-not (Test-Path $installDir)) { New-Item -ItemType Directory -Path $installDir | Out-Null }

Write-Host "Downloading application files to $installDir"
Invoke-WebRequest -Uri "$repoUrl/main.py" -OutFile (Join-Path $installDir 'main.py') -UseBasicParsing
Invoke-WebRequest -Uri "$repoUrl/backend.py" -OutFile (Join-Path $installDir 'backend.py') -UseBasicParsing
Invoke-WebRequest -Uri "$repoUrl/requirements.txt" -OutFile (Join-Path $installDir 'requirements.txt') -UseBasicParsing

# Download helpers
$helpersDir = Join-Path $installDir 'helpers'
if (-not (Test-Path $helpersDir)) { New-Item -ItemType Directory -Path $helpersDir | Out-Null }
$helperFiles = @(
    'addserver.py','cache_checker.py','cleanmovies.py','cleanseries.py','clear_cache.py',
    'config_manager.py','create_epg_xml.py','create_m3u_playlist.py','create_nfo_files.py',
    'create_series_nfo_files.py','create_series_strm_files.py','create_strm_files.py',
    'clean_metadata.py','defaultepggrabber.py','scheduled_update.py','setupdb.py',
    'updatecats.py','updatelive.py','updatemoviemetadata.py','updatemovies.py',
    'updateseries.py','updateseriesmetadata.py','vacuumdb.py'
)

foreach ($hf in $helperFiles) {
    $uri = "$repoUrl/helpers/$hf"
    $out = Join-Path $helpersDir $hf
    try {
        Invoke-WebRequest -Uri $uri -OutFile $out -UseBasicParsing
    } catch {
        Write-Host "Warning: failed to download $hf"
    }
}

# Download assets
$assetsDir = Join-Path $installDir 'assets'
if (-not (Test-Path $assetsDir)) { New-Item -ItemType Directory -Path $assetsDir | Out-Null }
Invoke-WebRequest -Uri "$repoUrl/assets/FLB.png" -OutFile (Join-Path $assetsDir 'FLB.png') -UseBasicParsing

# Install dependencies via venv pip
& $venv_pip install -r (Join-Path $installDir 'requirements.txt')

Write-Host "Installation complete. To activate the virtual environment, run: .\.\venv\Scripts\Activate.ps1"
Write-Host "Then you can run the application using: & .venv\Scripts\python.exe main.py (after activating the venv)"