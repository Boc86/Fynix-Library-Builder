import sqlite3
import os
from pathlib import Path
import sys
import helpers.config_manager as config_manager # New import

def create_m3u_playlist():
    """
    Generates an M3U8 playlist from the live_streams table in the database.
    """
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    db_path = project_root / "database" / "media_player.db"
    
    # Load directory paths from config_manager
    paths = config_manager.load_directories()
    live_tv_path = paths.get("live_tv")

    if not live_tv_path:
        print("ERROR: Live TV directory not configured. Cannot create M3U playlist.", file=sys.stderr)
        return False
    
    # Ensure the live_tv_path exists
    Path(live_tv_path).mkdir(parents=True, exist_ok=True)

    playlist_path = Path(live_tv_path) / "playlist.m3u8" # Modified to use live_tv_path

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return False # Changed to False for consistency

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = """
            SELECT
                ls.name,
                ls.stream_icon,
                ls.epg_channel_id,
                ls.tv_archive,
                c.category_name,
                s.url as server_url,
                s.port,
                s.username,
                s.password,
                ls.stream_id
            FROM
                live_streams ls
            JOIN
                categories c ON ls.category_id = c.category_id
            JOIN
                servers s ON ls.server_id = s.id
            WHERE
                ls.visible = 1 AND c.visible = 1
        """

        cursor.execute(query)
        streams = cursor.fetchall()

        with open(playlist_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")

            for stream in streams:
                extinf_line = f'#EXTINF:-1'
                
                if stream["epg_channel_id"]:
                    extinf_line += f' tvg-id="{stream["epg_channel_id"]}"'
                if stream["name"]:
                    extinf_line += f' tvg-name="{stream["name"]}"'
                if stream["stream_icon"]:
                    extinf_line += f' tvg-logo="{stream["stream_icon"]}"'
                if stream["category_name"]:
                    extinf_line += f' tvg-group="{stream["category_name"]}"'
                if stream["tv_archive"]:
                    extinf_line += f' tvg-archive="{stream["tv_archive"]}"'
                
                extinf_line += f',{stream["name"]}\n'
                f.write(extinf_line)
                
                server_url = stream["server_url"]
                port = stream["port"]
                username = stream["username"]
                password = stream["password"]
                stream_id = stream["stream_id"]

                if server_url.startswith('https'):
                    server_url = server_url.replace('https', 'http', 1)

                if server_url.endswith('/'):
                    server_url = server_url[:-1]
                
                url = f"{server_url}:{port}/live/{username}/{password}/{stream_id}"
                f.write(f'{url}\n')

        print(f"M3U playlist created at {playlist_path}")
        print("DEBUG: create_m3u_playlist returning True", file=sys.stderr) # Added debug print
        return True

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        print("DEBUG: create_m3u_playlist returning False due to error", file=sys.stderr) # Added debug print
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_m3u_playlist()
