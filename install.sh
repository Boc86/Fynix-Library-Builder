#!/bin/bash

# Define GitHub repository URL
REPO_URL="https://raw.githubusercontent.com/Boc86/Fynix-Library-Builder/main"

# Create installation directory
INSTALL_DIR="$HOME/.fynix-library-builder"
mkdir -p "$INSTALL_DIR"

# Download application files
wget -O "$INSTALL_DIR/main.py" "$REPO_URL/main.py"
wget -O "$INSTALL_DIR/backend.py" "$REPO_URL/backend.py"
wget -O "$INSTALL_DIR/requirements.txt" "$REPO_URL/requirements.txt"

# Create and download helpers directory and its contents
mkdir -p "$INSTALL_DIR/helpers"
wget -O "$INSTALL_DIR/helpers/addserver.py" "$REPO_URL/helpers/addserver.py"
wget -O "$INSTALL_DIR/helpers/cache_checker.py" "$REPO_URL/helpers/cache_checker.py"
wget -O "$INSTALL_DIR/helpers/cleanmovies.py" "$REPO_URL/helpers/cleanmovies.py"
wget -O "$INSTALL_DIR/helpers/cleanseries.py" "$REPO_URL/helpers/cleanseries.py"
wget -O "$INSTALL_DIR/helpers/clear_cache.py" "$REPO_URL/helpers/clear_cache.py"
wget -O "$INSTALL_DIR/helpers/config_manager.py" "$REPO_URL/helpers/config_manager.py"
wget -O "$INSTALL_DIR/helpers/create_epg_xml.py" "$REPO_URL/helpers/create_epg_xml.py"
wget -O "$INSTALL_DIR/helpers/create_m3u_playlist.py" "$REPO_URL/helpers/create_m3u_playlist.py"
wget -O "$INSTALL_DIR/helpers/create_nfo_files.py" "$REPO_URL/helpers/create_nfo_files.py"
wget -O "$INSTALL_DIR/helpers/create_series_nfo_files.py" "$REPO_URL/helpers/create_series_nfo_files.py"
wget -O "$INSTALL_DIR/helpers/create_series_strm_files.py" "$REPO_URL/helpers/create_series_strm_files.py"
wget -O "$INSTALL_DIR/helpers/create_strm_files.py" "$REPO_URL/helpers/create_strm_files.py"
wget -O "$INSTALL_DIR/helpers/defaultepggrabber.py" "$REPO_URL/helpers/defaultepggrabber.py"
wget -O "$INSTALL_DIR/helpers/scheduled_update.py" "$REPO_URL/helpers/scheduled_update.py"
wget -O "$INSTALL_DIR/helpers/setupdb.py" "$REPO_URL/helpers/setupdb.py"
wget -O "$INSTALL_DIR/helpers/updatecats.py" "$REPO_URL/helpers/updatecats.py"
wget -O "$INSTALL_DIR/helpers/updatelive.py" "$REPO_URL/helpers/updatelive.py"
wget -O "$INSTALL_DIR/helpers/updatemoviemetadata.py" "$REPO_URL/helpers/updatemoviemetadata.py"
wget -O "$INSTALL_DIR/helpers/updatemovies.py" "$REPO_URL/helpers/updatemovies.py"
wget -O "$INSTALL_DIR/helpers/updateseries.py" "$REPO_URL/helpers/updateseries.py"
wget -O "$INSTALL_DIR/helpers/updateseriesmetadata.py" "$REPO_URL/helpers/updateseriesmetadata.py"
wget -O "$INSTALL_DIR/helpers/vacuumdb.py" "$REPO_URL/helpers/vacuumdb.py"


# Create and download assets directory
mkdir -p "$INSTALL_DIR/assets"
wget -O "$INSTALL_DIR/assets/FLB.png" "$REPO_URL/assets/FLB.png"

# Create and activate virtual environment
python3 -m venv "$INSTALL_DIR/.venv"
source "$INSTALL_DIR/.venv/bin/activate"

# Install dependencies
pip install -r "$INSTALL_DIR/requirements.txt"

# Create .desktop file
DESKTOP_FILE="$HOME/.local/share/applications/fynix-library-builder.desktop"
cat << EOF > "$DESKTOP_FILE"
[Desktop Entry]
Name=Fynix Library Builder
Exec=$INSTALL_DIR/.venv/bin/python $INSTALL_DIR/main.py
Icon=$INSTALL_DIR/assets/FLB.png
Terminal=false
Type=Application
Categories=AudioVideo;Player;
Path=$INSTALL_DIR
EOF
chmod +x "$DESKTOP_FILE"

update-desktop-database ~/.local/share/applications/

echo "Fynix Library Builder installed successfully!"
echo "You can find it in your application menu."
