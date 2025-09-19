import sqlite3
import logging
from pathlib import Path
import os
import re
import helpers.config_manager as config_manager
from helpers.create_strm_files import _sanitize_name, _extract_year # Reuse functions
from helpers.create_series_nfo_files import create_single_tvshow_nfo_file, create_single_episode_nfo_file # Import the new functions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"

def create_series_strm_files() -> bool:
    logger.info("Starting creation of .strm files for Series.")
    
    config = config_manager.load_directories()
    series_folder = config.get("series")
    
    if not series_folder:
        logger.error("Series folder not configured in directories.toml. Skipping .strm file creation.")
        return False
        
    series_base_path = Path(series_folder)
    series_base_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving series .strm files to: {series_base_path}")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Fetch series data (only from visible categories)
        cursor.execute("""
            SELECT 
                s.*, 
                serv.url, serv.username, serv.password, serv.port,
                c.visible
            FROM 
                series s
            JOIN 
                servers serv ON s.server_id = serv.id
            JOIN
                categories c ON s.category_id = c.id -- Assuming s.category_id links to c.id
            WHERE
                c.visible = 1
        """)
        series_list = cursor.fetchall()
        logger.info(f"Found {len(series_list)} Series to process (visible categories only).")

        for series_data_row in series_list:
            series_data = dict(series_data_row) # Convert Row to dict

            series_id = series_data['series_id']
            series_name = series_data['name']
            release_date = series_data['release_date']
            server_url = series_data['url']
            server_username = series_data['username']
            server_password = series_data['password']
            server_port = series_data['port']

            # Determine series name for folder and filename
            sanitized_series_name = _sanitize_name(series_name)
            year = _extract_year(release_date, series_name)
            
            series_folder_name = f"{sanitized_series_name}"
            if year:
                series_folder_name += f" ({year})"
            
            current_series_path = series_base_path / series_folder_name
            current_series_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created series folder: {current_series_path}")

            # Create tvshow.nfo file for the series
            create_single_tvshow_nfo_file(series_data, current_series_path)

            # Fetch episodes for this series
            episode_cursor = conn.cursor()
            episode_cursor.execute("""
                SELECT 
                    e.*, 
                    serv.url, serv.username, serv.password, serv.port
                FROM 
                    episodes e
                JOIN
                    servers serv ON e.server_id = serv.id
                WHERE 
                    e.series_id = ?
                ORDER BY 
                    e.season_num, e.episode_num
            """, (series_id,))
            episodes_list = episode_cursor.fetchall()
            logger.debug(f"Found {len(episodes_list)} episodes for series {series_name}.")

            # Group episodes by season
            seasons = {}
            for episode in episodes_list:
                season_num = episode['season_num']
                if season_num not in seasons:
                    seasons[season_num] = []
                seasons[season_num].append(episode)
            
            for season_num in sorted(seasons.keys()):
                season_folder_name = f"Season {season_num:02d}"
                current_season_path = current_series_path / season_folder_name
                current_season_path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created season folder: {current_season_path}")

                for episode_data_row in seasons[season_num]:
                    episode_data = dict(episode_data_row) # Convert Row to dict

                    episode_id = episode_data['episode_id']
                    episode_num = episode_data['episode_num']
                    episode_title = episode_data['title']
                    container_extension = episode_data['container_extension']
                    
                    # Construct episode filename: SERIES SXXEYY.strm
                    episode_filename_base = f"{sanitized_series_name} S{season_num:02d}E{episode_num:02d}"
                    strm_filepath = current_season_path / f"{episode_filename_base}.strm"

                    # Only create if file doesn't exist
                    if strm_filepath.exists():
                        logger.debug(f"Skipping existing .strm file: {strm_filepath}")
                        # If STRM exists, assume NFO also exists or was handled. Skip NFO creation too.
                        continue

                    # Construct the full stream URL for series
                    # Ensure the server URL starts with http://
                    processed_server_url = server_url
                    if processed_server_url.startswith("https://"):
                        processed_server_url = "http://" + processed_server_url[len("https://"):]
                    elif not processed_server_url.startswith("http://"):
                        processed_server_url = "http://" + processed_server_url

                    # URL format: serveraddress:port/series/username/password/episode_id.container_extension
                    full_stream_url = f"{processed_server_url}:{server_port}/series/{server_username}/{server_password}/{episode_id}.{container_extension}"
                    
                    try:
                        with open(strm_filepath, "w") as f:
                            f.write(full_stream_url)
                        logger.debug(f"Created .strm file: {strm_filepath}")
                        
                        # Create corresponding .nfo file for the episode
                        create_single_episode_nfo_file(episode_data, episode_filename_base, current_season_path, sanitized_series_name)

                    except Exception as e:
                        logger.error(f"Error creating .strm file for episode {episode_title} ({episode_id}): {e}")
                        # Continue to next episode even if one fails

        logger.info("Finished creating .strm and .nfo files for Series.")
        return True

    except sqlite3.Error as e:
        logger.error(f"Database error during series .strm/.nfo file creation: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during series .strm/.nfo file creation: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main() -> bool:
    """Main function to run the series .strm and .nfo file creation process."""
    return create_series_strm_files()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)