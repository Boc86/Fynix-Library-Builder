import sqlite3
import logging
from pathlib import Path
import os
import re
import helpers.config_manager as config_manager
from helpers.create_nfo_files import create_single_nfo_file # Import the new function

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"

def _sanitize_name(name: str) -> str:
    """Sanitizes a string for use as a filename, removing common prefixes, years, and cleaning up."""
    # Remove 'EN - ' prefix
    if name.startswith("EN - "):
        name = name[5:]

    # Remove common quality/resolution prefixes and other tags in brackets/parentheses
    name = re.sub(r'^(?:4K-D\.-|4K\.-|HD\.-|FHD\.-|SD\.-|4K\s*-\s*|HD\s*-\s*|FHD\s*-\s*|SD\s*-\s*|4K\s*|HD\s*|FHD\s*|SD\s*|\[.*?\]|\(.*?\))\s*', '', name, flags=re.IGNORECASE).strip()

    # Replace dots with spaces (assuming dots are separators, not part of the title itself) and handle multiple spaces
    sanitized = re.sub(r'\.+', ' ', name) # Replace one or more dots with a single space

    # Remove invalid filename characters (keep alphanumeric, underscore, hyphen, space)
    sanitized = re.sub(r'[^a-zA-Z0-9_\- ]', '', sanitized)

    # Reduce multiple spaces to a single space
    sanitized = re.sub(r'\s+', ' ', sanitized).strip()

    return sanitized

def _extract_year(release_date: str, name: str) -> str:
    """Extracts year from release_date or name."""
    year = ""
    
    # Try from release_date (YYYY-MM-DD)
    if release_date and len(release_date) >= 4:
        year = release_date[:4]
        if year.isdigit():
            return year
            
    # Try from name (e.g., 'Movie Title (2023)')
    match = re.search(r'\((\d{4})\)', name)
    if match:
        return match.group(1)
        
    return ""

def create_strm_files() -> bool:
    logger.info("Starting creation of .strm files for VOD streams.")
    
    config = config_manager.load_directories()
    movies_folder = config.get("movies")
    
    if not movies_folder:
        logger.error("Movie folder not configured in directories.toml. Skipping .strm file creation.")
        return False
        
    movies_path = Path(movies_folder)
    movies_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving .strm files to: {movies_path}")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Fetch more data: o_name, release_date, container_extension
        cursor.execute("""
            SELECT 
                vs.*, 
                s.url, s.username, s.password, s.port,
                c.visible -- Fetch category visibility
            FROM 
                vod_streams vs
            JOIN 
                servers s ON vs.server_id = s.id
            JOIN
                categories c ON vs.category_id = c.id -- Join with categories table
            WHERE
                c.visible = 1 -- Filter by visible categories
        """)
        vod_streams = cursor.fetchall()
        logger.info(f"Found {len(vod_streams)} VOD streams to process (visible categories only).")

        for stream in vod_streams:
            # Convert sqlite3.Row to a dictionary
            stream_dict = dict(stream)

            stream_id = stream_dict['stream_id']
            original_name = stream_dict['name'] # Keep original name for year extraction fallback
            o_name = stream_dict['o_name']
            release_date = stream_dict['release_date']
            container_extension = stream_dict['container_extension']
            direct_source = stream_dict['direct_source']
            server_url = stream_dict['url']
            server_username = stream_dict['username']
            server_password = stream_dict['password']
            server_port = stream_dict['port']

            # Determine movie name for filename
            # Prioritize o_name if it exists and is not empty/whitespace after initial sanitization
            name_for_sanitization = original_name # Default to original_name
            if o_name and _sanitize_name(o_name): # Check if o_name can be sanitized to something meaningful
                name_for_sanitization = o_name
            
            sanitized_movie_name = _sanitize_name(name_for_sanitization)

            # Final fallback if sanitization still results in empty string
            if not sanitized_movie_name:
                sanitized_movie_name = f"Unknown_Movie_{stream_id}"
            
            # Determine year for filename
            year = _extract_year(release_date, original_name)
            
            # Construct filename base (without extension)
            filename_base = f"{sanitized_movie_name}"
            if year:
                filename_base += f" ({year})"
            
            strm_filepath = movies_path / f"{filename_base}.strm"

            # Only create if file doesn't exist
            if strm_filepath.exists():
                logger.debug(f"Skipping existing .strm file: {strm_filepath}")
                # If STRM exists, assume NFO also exists or was handled. Skip NFO creation too.
                continue

            # Construct the full stream URL
            # Ensure the server URL starts with http://
            processed_server_url = server_url
            if processed_server_url.startswith("https://"):
                processed_server_url = "http://" + processed_server_url[len("https://"):]
            elif not processed_server_url.startswith("http://"):
                processed_server_url = "http://" + processed_server_url

            # Example: http://serveraddress:port/movie/username/password/stream_id.container_extension
            full_stream_url = f"{processed_server_url}:{server_port}/movie/{server_username}/{server_password}/{stream_id}.{container_extension}"
            
            try:
                with open(strm_filepath, "w") as f:
                    f.write(full_stream_url)
                logger.debug(f"Created .strm file: {strm_filepath}")
                
                # Create corresponding .nfo file
                create_single_nfo_file(stream_dict, filename_base, movies_path)

            except Exception as e:
                logger.error(f"Error creating .strm file for {original_name} ({stream_id}): {e}")
                # Continue to next stream even if one fails

        logger.info("Finished creating .strm and .nfo files.")
        return True

    except sqlite3.Error as e:
        logger.error(f"Database error during .strm/.nfo file creation: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during .strm/.nfo file creation: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main() -> bool:
    """Main function to run the .strm and .nfo file creation process."""
    return create_strm_files()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)