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

# Create and download helpers directory
mkdir -p "$INSTALL_DIR/helpers"
for file in addserver.py cache_checker.py cleanmovies.py cleanseries.py clear_cache.py config_manager.py create_nfo_files.py create_series_nfo_files.py create_series_strm_files.py create_strm_files.py scheduled_update.py setupdb.py updatecats.py updatemoviemetadata.py updatemovies.py updateseries.py updateseriesmetadata.py vacuumdb.py; do
    wget -O "$INSTALL_DIR/helpers/$file" "$REPO_URL/helpers/$file"
done

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
