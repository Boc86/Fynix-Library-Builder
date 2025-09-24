**Code generated using Gemini CLI**

# Fynix Library Builder

<p align="center">
  <img src="assets/FLB.png" alt="Fynix Library Builder Logo" width="150"/>
</p>

<p align="center">
  A desktop utility for browsing IPTV provider content and building a local `.strm` file library for use in media centers like Kodi, Plex, or Jellyfin.
</p>

---

## Table of Contents

- [Features](#features)
- [How It Works](#how-it-works)
- [Installation](#installation)
- [Usage](#usage)
- [Technical Overview](#technical-overview)
- [Compatability](#compatability)

---

## Features

- **Easy Setup**: A step-by-step setup wizard to configure your IPTV provider details and library folders on the first run.
- **Live TV Integration**: Seamlessly manage live streams, generate M3U playlists, and integrate EPG (Electronic Program Guide) data.
- **Category Management**: Easily enable or disable VOD, Series, and Live TV categories to control what content appears in your library.
- **Manual & Scheduled Updates**: Trigger a library update manually or schedule it to run at a specific time each day, including Live TV data.
- **Server Configuration**: Update your server details at any time from the main settings window.
- **Database Statistics**: See a live count of the total and visible movies, series, episodes, and live streams in your database.
- **System Tray Integration**: The application minimizes to the system tray and can run continuously in the background to perform scheduled updates.
- **Modern UI**: A clean, dark-themed user interface.

## Live TV Features

- **Live Stream Management**: Connects to your IPTV provider to fetch and manage live TV channels. You can enable or disable individual channels and entire live categories.
- **EPG (Electronic Program Guide) Integration**: Grabs EPG data from your provider and generates a standard `epg.xml` file, compatible with media centers like Kodi, Plex, or Jellyfin for a rich TV guide experience.
- **M3U Playlist Generation**: Creates an `M3U8` playlist (`playlist.m3u8`) of your enabled live channels, allowing easy access and playback in various media players.

## How It Works

Fynix Library Builder connects to your IPTV provider's API to fetch lists of available movies and TV series. Based on the categories you have enabled, it then generates a local folder structure containing `.strm` files.

These `.strm` files are simple text files that contain a direct URL to the media stream. For Live TV, the builder generates an `M3U8` playlist (`playlist.m3u8`) and an `epg.xml` file.

You can add the generated folders (e.g., `/path/to/your/movies`, `/path/to/your/series`, and `/path/to/your/live_tv`) as library sources in media center software like Kodi. Your media center will scan these files and import the content as if it were stored locally, fetching metadata and artwork automatically.

This allows you to browse your provider's VOD, series, and Live TV library using the rich interface of a full-fledged media center.

## Installation

**For Linux users:**

```bash
wget -O - https://raw.githubusercontent.com/Boc86/Fynix-Library-Builder/main/install.sh | bash
```

**For Windows users (PowerShell):**

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Boc86/Fynix-Library-Builder/main/install.ps1" -OutFile "$env:TEMP\install.ps1"; Start-Process powershell -Verb RunAs -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File \"$env:TEMP\install.ps1\""
```

**Manual Installation:**

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Boc86/Fynix-Library-Builder.git
    cd Fynix-Library-Builder
    ```

2.  **Create a Python virtual environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the application:**
    ```bash
    python3 main.py
    ```
    On the first run, you will be guided by the Setup Wizard, where you can now also specify your Live TV library path.

**Update:**
- Re-run the instalation script and ignore any errors about the virtual environment not being set up as it already exists.

## Usage

- **Initial Setup**: Follow the wizard to enter your server details and specify where you want your movie, series, and live TV `.strm` and playlist files to be saved.

- **Main Window**:
    - **Server Configuration**: Allows you to view and update your IPTV provider details.
    - **Library Directory Settings**: Configure the paths for your Movie, Series, and Live TV libraries.
    - **Actions**: 
        - `Save All Changes`: Saves any modifications to the server, category, schedule, and directory settings.
        - `Update Library`: Manually triggers the creation of `.strm` files, M3U playlist, and EPG data.
        - `Clear Cache`: Clears all cached metadata.
        - `Process Live TV` checkbox: Enable or disable the processing of live streams and EPG data during library updates.
    - **Auto Update**: Enable the checkbox and set a time to have the library update run automatically each day.
    - **Database Statistics**: View counts of your media library.
    - **Category Panes**: Check or uncheck categories to control which ones are included in your library. Remember to click `Save All Changes` after making modifications.

- **Live Channels Tab**:
    - **Live Categories**: Select which live TV categories to include in your M3U playlist and EPG.
    - **Live Channels**: For a selected category, enable or disable individual live channels.
    - `Save Live Channel Changes`: Saves the visibility settings for live categories and channels.

- **Background Operation**:
    - Closing the window will minimize the application to the system tray.
    - Right-click the tray icon to show the window or quit the application.

## Technical Overview

- **Framework**: Python 3 with PySide6 (Qt6) for the graphical user interface.
- **Database**: SQLite is used to store all metadata related to servers, categories, VOD content, series, live streams (`live_streams` table), and EPG data (`epg_data` table).
- **Configuration**: 
    - `directories.toml`: Stores the paths to your media libraries, including a new `live_tv` key for Live TV content.
    - `schedule.json`: Stores the settings for the auto-update feature.
- **Backend Logic**: The core application logic (database interaction, API calls) is separated in `backend.py`, allowing for a clean separation from the UI code in `main.py`.
- **Helpers**: The `helpers/` directory contains various scripts for database setup and content synchronization.

---

## Compatability
The following have been tested and work, if you use a different system and it works please add to the list
### Linux
- Arch Base
- Mint / Ubuntu
- Nobara (Fedora)

### Window Manager
- Hyprland
- KDE Plasma
- Gnome **minimise to system tray may not work but normal minimise to workspace bar does**
- XFCE **if the app fails to launch ensure libxc-cursor-dev is installed from your package manager** 

---

### Windows
- Untested, needs verification

## License

This project is open-source. Feel free to modify and distribute it as you see fit.
