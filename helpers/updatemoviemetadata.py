import sqlite3
import requests
import logging
import time
import pickle
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
import sys
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Paths
DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"
CACHE_DIR = DB_PATH.parent / "cache" / "movie_metadata"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_EXPIRY = 24 * 60 * 60  # 24 hours in seconds

# Rate limit (seconds between API requests)
RATE_LIMIT_DELAY = 0.02

# Thread pool size for fetching metadata
THREAD_WORKERS = 8



# Columns to overwrite
UPDATE_COLUMNS = [
    "custom_sid", "direct_source", "plot", "cast", "director", "genre", "release_date",
    "duration_secs", "duration", "video_quality", "tmdb_id", "o_name", "cover_big",
    "movie_image", "youtube_trailer", "actors", "description", "age", "country",
    "backdrop_path", "bitrate", "status", "runtime"
]

def fetch_metadata_from_api(server, stream_id):
    retries = 3
    delay = 1  # initial delay in seconds
    for i in range(retries):
        try:
            base_url = server['url'].strip()
            if not base_url.startswith(('http://', 'https://')):
                base_url = 'http://' + base_url
            port = server.get('port', 80)
            if port and port not in (80, 443) and ':' not in base_url.split('://', 1)[1]:
                base_url = f"{base_url}:{port}"
            base_url = base_url.rstrip('/')

            url = f"{base_url}/player_api.php?username={server['username']}&password={server['password']}&action=get_vod_info&vod_id={stream_id}"
            
            logger.debug(f"Attempt {i+1}/{retries} to fetch metadata for stream_id {stream_id} from {url}")
            response = requests.get(url, timeout=30, verify=False)
            response.raise_for_status()
            
            json_response = response.json()
            if isinstance(json_response, list):
                logger.warning(f"API response for stream_id {stream_id} is a list, expected a dictionary. Response: {json_response}")
                return None
            return json_response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error fetching metadata for stream_id {stream_id} (attempt {i+1}/{retries}): {e}")
            if i < retries - 1:
                time.sleep(delay)
                delay *= 2 # Exponential backoff
            else:
                return None
        except ValueError as e: # Catches JSON decoding errors
            logger.warning(f"JSON decoding error for stream_id {stream_id} (attempt {i+1}/{retries}): {e}. Response text: {response.text}")
            return None
        except Exception as e:
            logger.warning(f"An unexpected error occurred fetching metadata for stream_id {stream_id} (attempt {i+1}/{retries}): {e}")
            return None
    return None

def load_cache(stream_id):
    cache_file = CACHE_DIR / f"{stream_id}.pkl"
    if cache_file.exists():
        try:
            # Check if cache has expired
            if time.time() - cache_file.stat().st_mtime > CACHE_EXPIRY:
                return None
            with open(cache_file, "rb") as f:
                return pickle.load(f)
        except Exception:
            return None
    return None

def save_cache(stream_id, data):
    cache_file = CACHE_DIR / f"{stream_id}.pkl"
    try:
        with open(cache_file, "wb") as f:
            pickle.dump(data, f)
    except Exception as e:
        logger.warning(f"Failed to save cache for stream_id {stream_id}: {e}")

def get_movies(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT stream_id, server_id FROM vod_streams WHERE tmdb_id IS NULL OR tmdb_id=''")
    movies = cursor.fetchall()
    conn.close()
    return movies

def get_server_info(db_path, server_id):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM servers WHERE id=?", (server_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def normalize_value(value, key):
    """Convert lists to comma-separated strings, None to empty strings (except integers)."""
    if isinstance(value, list):
        return ', '.join(str(v) for v in value)
    if value is None:
        if key in ["duration_secs", "bitrate"]:
            return 0
        return ''
    return value

def update_movie(db_path, stream_id, metadata, db_lock):
    if not isinstance(metadata, dict):
        logger.error(f"Unexpected metadata type for stream_id {stream_id}: {type(metadata)}. Value: {metadata}")
        return 0
    info = metadata.get("info", {})
    movie_data = metadata.get("movie_data", {})
    flattened = {**info, **movie_data}

    # Map API field 'releasedate' to DB column 'release_date'
    if "releasedate" in flattened:
        flattened["release_date"] = flattened.pop("releasedate")

    set_clauses = []
    params = []

    for key in UPDATE_COLUMNS:
        value = normalize_value(flattened.get(key), key)
        set_clauses.append(f"{key}=?")
        params.append(value)

    if not set_clauses:
        logger.warning(f"No columns to update for stream_id {stream_id}")
        return 0

    params.append(stream_id)
    sql_query = f"UPDATE vod_streams SET {', '.join(set_clauses)} WHERE stream_id=?"

    with db_lock:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL") # Add WAL mode for better concurrency
        cursor = conn.cursor()
        cursor.execute(sql_query, params)
        conn.commit()
        conn.close()
    return cursor.rowcount

# Moved process_movie outside main()
def process_movie(index, movie_tuple, total_movies, db_lock):
    stream_id, server_id = movie_tuple
    server = get_server_info(DB_PATH, server_id) # DB_PATH is global
    if not server:
        logger.warning(f"Server {server_id} not found, skipping stream_id {stream_id}")
        return 0

    metadata = load_cache(stream_id)
    # Ensure metadata from cache is a dictionary, otherwise treat as None
    if not isinstance(metadata, dict):
        metadata = None

    if metadata is None:
        metadata = fetch_metadata_from_api(server, stream_id)
        if metadata: # metadata from API is already checked for dict type
            save_cache(stream_id, metadata)
        time.sleep(RATE_LIMIT_DELAY)

    if metadata: # Now metadata is guaranteed to be a dict or None
        if not isinstance(metadata, dict):
            logger.error(f"Unexpected metadata type for stream_id {stream_id}: {type(metadata)}. Value: {metadata}")
            return 0 # Skip update if type is incorrect
        updated = update_movie(DB_PATH, stream_id, metadata, db_lock)
        print(f"[{index}/{total_movies}] Updated stream_id {stream_id}" if updated else f"[{index}/{total_movies}] No update applied for stream_id {stream_id}", flush=True)
        return updated
    else:
        print(f"[{index}/{total_movies}] No metadata available for stream_id {stream_id}", flush=True)
        return 0

def main():
    logger.info("Starting movie metadata update")

    movies = get_movies(DB_PATH)
    total_movies = len(movies)
    print(f"Found {total_movies} movies without TMDB ID", flush=True)

    total_updated = 0
    has_errors = False
    db_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
        # Pass total_movies to process_movie
        futures = {executor.submit(process_movie, i+1, movie, total_movies, db_lock): movie for i, movie in enumerate(movies)}
        for future in as_completed(futures):
            try:
                total_updated += future.result()
            except Exception as e:
                logger.error(f"Error processing movie: {e}")
                has_errors = True

    print(f"Movie metadata update completed: total updated {total_updated}", flush=True)

    return True # Always return True if the main execution flow completes

if __name__ == "__main__":
    main()