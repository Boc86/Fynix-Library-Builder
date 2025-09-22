#!/bin/bash

# Store the directory where the script is being executed
SCRIPT_DIR="$(pwd)"

# Clone the Fynix Library Builder repository
git clone https://github.com/fynixmedia/Fynix-Library-Builder.git

# Navigate into the cloned directory
cd Fynix-Library-Builder

# Check if helpers/ directory exists in the cloned repository
if [ ! -d "helpers" ]; then
    echo "Warning: 'helpers/' directory not found in the cloned repository."
    # Check if helpers/ exists in the original script execution directory
    if [ -d "${SCRIPT_DIR}/helpers" ]; then
        echo "Copying 'helpers/' from original directory: ${SCRIPT_DIR}/helpers"
        cp -r "${SCRIPT_DIR}/helpers" .
    else
        echo "Error: 'helpers/' directory not found locally either. Please ensure it's in the repository or available in the script's execution path."
        exit 1
    fi
fi

# Install Python dependencies
pip install -r requirements.txt

echo "Fynix Library Builder installed successfully!"
