# Requires Administrator privileges to install to C:\Program Files
if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "This script must be run with Administrator privileges. Please right-click and select 'Run as Administrator'."
    exit 1
}

$REPO_URL = "https://raw.githubusercontent.com/Boc86/Fynix-Library-Builder/main"
$INSTALL_DIR = "C:\Program Files\Fynix Library Builder"
$VENV_DIR = Join-Path $INSTALL_DIR ".venv"
$START_MENU_DIR = [Environment]::GetFolderPath("Programs")
$SHORTCUT_PATH = Join-Path $START_MENU_DIR "Fynix Library Builder.lnk"

Write-Host "Creating installation directory: $INSTALL_DIR"
New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null

Write-Host "Downloading application files..."
Invoke-WebRequest -Uri "$REPO_URL/main.py" -OutFile (Join-Path $INSTALL_DIR "main.py")
Invoke-WebRequest -Uri "$REPO_URL/backend.py" -OutFile (Join-Path $INSTALL_DIR "backend.py")
Invoke-WebRequest -Uri "$REPO_URL/requirements.txt" -OutFile (Join-Path $INSTALL_DIR "requirements.txt")

Copy-Item -Path "helpers" -Destination "$installDir" -Recurse -Force

Write-Host "Downloading assets..."
$ASSETS_DIR = Join-Path $INSTALL_DIR "assets"
New-Item -ItemType Directory -Path $ASSETS_DIR -Force | Out-Null
Invoke-WebRequest -Uri "$REPO_URL/assets/FLB.png" -OutFile (Join-Path $ASSETS_DIR "FLB.png")

Write-Host "Creating Python virtual environment..."
python -m venv $VENV_DIR

Write-Host "Installing dependencies..."
& "$VENV_DIR\Scripts\pip.exe" install -r (Join-Path $INSTALL_DIR "requirements.txt")

Write-Host "Creating Start Menu shortcut..."
$WshShell = New-Object -ComObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut($SHORTCUT_PATH)
$Shortcut.TargetPath = Join-Path $VENV_DIR "Scripts\pythonw.exe" # Use pythonw.exe for no console window
$Shortcut.Arguments = (Join-Path $INSTALL_DIR "main.py")
$Shortcut.IconLocation = (Join-Path $ASSETS_DIR "FLB.png")
$Shortcut.WorkingDirectory = $INSTALL_DIR
$Shortcut.Description = "Launch Fynix Library Builder"
$Shortcut.Save()

Write-Host "Fynix Library Builder installed successfully!"
Write-Host "You can find it in your Start Menu."
