import sqlite3
import requests
import logging
import time
import pickle
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configure logging
import sys
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Paths
DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"
CACHE_DIR = DB_PATH.parent / "cache" / "series_metadata"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_EXPIRY = 24 * 60 * 60  # 24 hours in seconds

# Rate limit (seconds between API requests)
RATE_LIMIT_DELAY = 1.0

# Thread pool size for fetching metadata
THREAD_WORKERS = 4



# Columns to update in the series table
SERIES_UPDATE_COLUMNS = [
    "rating_5based", "backdrop_path", "youtube_trailer", "tmdb_id",
    "episode_run_time", "category_id", "category_ids"
]

def fetch_series_metadata(server, series_id):
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
            url = f"{base_url}/player_api.php?username={server['username']}&password={server['password']}&action=get_series_info&series_id={series_id}"
            
            start_time = time.time()
            logger.info(f"Fetching metadata for series_id {series_id} (Attempt {i+1}/{retries}). URL: {url}")
            response = requests.get(url, timeout=30, verify=False)
            end_time = time.time()
            
            response_size = len(response.content)
            logger.info(f"Received response for series_id {series_id} (Attempt {i+1}/{retries}). Status: {response.status_code}, Time: {end_time - start_time:.2f}s, Size: {response_size} bytes")
            response.raise_for_status()
            
            json_response = response.json()
            if isinstance(json_response, list):
                logger.warning(f"API response for series_id {series_id} is a list, expected a dictionary. Response: {json_response}")
                return None
            return json_response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error fetching metadata for series_id {series_id} (attempt {i+1}/{retries}): {e}")
            if i < retries - 1:
                time.sleep(delay)
                delay *= 2 # Exponential backoff
            else:
                return None
        except ValueError as e: # Catches JSON decoding errors
            logger.warning(f"JSON decoding error for series_id {series_id} (attempt {i+1}/{retries}): {e}. Response text: {response.text}")
            return None
        except Exception as e:
            logger.warning(f"An unexpected error occurred fetching metadata for series_id {series_id} (attempt {i+1}/{retries}): {e}")
            return None
    return None

def load_cache(stream_id):
    cache_file = CACHE_DIR / f"{stream_id}.pkl"
    if cache_file.exists():
        try:
            with open(cache_file, "rb") as f:
                return pickle.load(f)
        except Exception:
            return None
    return None

def save_cache(series_id, data):
    cache_file = CACHE_DIR / f"{series_id}.pkl"
    try:
        with open(cache_file, "wb") as f:
            pickle.dump(data, f)
    except Exception as e:
        logger.warning(f"Failed to save cache for series_id {series_id}: {e}")

def get_series(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT series_id, server_id, last_modified FROM series")
    series = cursor.fetchall()
    conn.close()
    return series

def get_server_info(db_path, server_id):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM servers WHERE id=?", (server_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def normalize_value(value, key):
    if isinstance(value, list):
        return ', '.join(str(v) for v in value)
    if value is None:
        if key in ["category_id", "episode_run_time"]:
            return 0
        return ''
    return value

def update_series(db_path, series_id, metadata):
    info = metadata.get("info", {})
    flattened = {
        "rating_5based": info.get("rating_5based"),
        "backdrop_path": info.get("backdrop_path")[0] if info.get("backdrop_path") else "",
        "youtube_trailer": info.get("youtube_trailer"),
        "tmdb_id": str(info.get("tmdb")),
        "episode_run_time": info.get("episode_run_time"),
        "category_id": info.get("category_id"),
        "category_ids": ','.join(str(cid) for cid in info.get("category_ids", []))
    }

    set_clauses = []
    params = []

    for key in SERIES_UPDATE_COLUMNS:
        value = normalize_value(flattened.get(key), key)
        set_clauses.append(f"{key}=?")
        params.append(value)

    params.append(series_id)
    sql_query = f"UPDATE series SET {', '.join(set_clauses)} WHERE series_id=?"

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()
    cursor.execute(sql_query, params)
    conn.commit()
    conn.close()
    return cursor.rowcount

def insert_episode(cursor, server_id, series_id, season_num, episode):
    cursor.execute("SELECT 1 FROM episodes WHERE episode_id=?", (episode["id"],))
    if cursor.fetchone():
        return None  # Return None to indicate no insertion, not 0

    video_codec = episode.get("video", {}).get("codec_name", "")
    audio_channels = episode.get("audio", {}).get("channels", "")

    params = (
        server_id,
        series_id,
        season_num,
        episode["id"],
        episode.get("title", ""),
        episode.get("info", {}).get("plot", ""),
        episode.get("duration", ""),
        episode.get("info", {}).get("air_date", ""),
        episode.get("container_extension", ""),
        episode.get("episode_num", 0),
        episode.get("info", {}).get("rating", 0),
        episode.get("info", {}).get("crew", ""),
        str(episode.get("info", {}).get("id", "")),
        episode.get("info", {}).get("movie_image", ""),
        episode.get("info", {}).get("duration_secs", 0),
        video_codec,
        audio_channels,
        episode.get("bitrate", 0),
        episode.get("custom_sid", ""),
        episode.get("added", ""),
        episode.get("direct_source", ""),
        episode.get("season", season_num)
    )
    return params

def process_series(db_path, index, series_tuple, total_series):
    series_id, server_id, last_modified_str = series_tuple
    server = get_server_info(db_path, server_id)
    if not server:
        logger.warning(f"Server {server_id} not found, skipping series_id {series_id}")
        return 0, 0

    cache_file = CACHE_DIR / f"{series_id}.pkl"
    metadata = None
    use_cache = False

    if cache_file.exists():
        cache_mod_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
        if last_modified_str:
            last_modified = datetime.fromtimestamp(int(last_modified_str))
            if cache_mod_time > last_modified:
                use_cache = True
        else:
            use_cache = True

    if use_cache:
        metadata = load_cache(series_id)

    if metadata is None:
        metadata = fetch_series_metadata(server, series_id)
        if metadata:
            save_cache(series_id, metadata)
        time.sleep(RATE_LIMIT_DELAY)

    updated_series = 0
    inserted_episodes = 0

    if metadata and isinstance(metadata, dict): # Ensure metadata is a dictionary
        updated_series = update_series(db_path, series_id, metadata)

        # Fetch existing episode IDs for this series once
        existing_episode_ids = set()
        conn_check = sqlite3.connect(db_path)
        cursor_check = conn_check.cursor()
        cursor_check.execute("SELECT episode_id FROM episodes WHERE series_id=?", (series_id,))
        for row in cursor_check.fetchall():
            existing_episode_ids.add(row[0])
        conn_check.close()

        # Batch insert episodes
        episodes_to_insert = []
        episodes_data = metadata.get("episodes", {})
        if isinstance(episodes_data, dict):
            for season_num, episodes_list in episodes_data.items():
                for episode in episodes_list:
                    if not isinstance(episode, dict):
                        logger.warning(f"Skipping non-dictionary episode item in series {series_id}: {episode}")
                        continue
                    
                    info = episode.get("info", {})
                    if not isinstance(info, dict):
                        info = {}

                    if "id" in episode and episode["id"] not in existing_episode_ids:
                        video_codec = episode.get("video", {}).get("codec_name", "")
                        audio_channels = episode.get("audio", {}).get("channels", "")

                        params = (
                            server_id,
                            series_id,
                            int(season_num), # Ensure season_num is int
                            episode["id"],
                            episode.get("title", ""),
                            info.get("plot", ""),
                            episode.get("duration", ""),
                            info.get("air_date", ""),
                            episode.get("container_extension", ""),
                            episode.get("episode_num", 0),
                            info.get("rating", 0),
                            info.get("crew", ""),
                            str(info.get("id", "")),
                            info.get("movie_image", ""),
                            info.get("duration_secs", 0),
                            video_codec,
                            audio_channels,
                            episode.get("bitrate", 0),
                            episode.get("custom_sid", ""),
                            episode.get("added", ""),
                            episode.get("direct_source", ""),
                            episode.get("season", int(season_num)) # Ensure season is int
                        )
                        episodes_to_insert.append(params)
        else:
            logger.warning(f"Episodes data for series {series_id} is not a dictionary, skipping episode processing.")

        if episodes_to_insert:
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.cursor()
            try:
                cursor.executemany("""
                    INSERT INTO episodes (
                        server_id, series_id, season_num, episode_id, title, plot, duration, airdate,
                        container_extension, episode_num, rating, crew, tmdb_id, movie_image, duration_secs,
                        video, audio, bitrate, custom_sid, added, direct_source, season
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, episodes_to_insert)
                conn.commit()
                inserted_episodes = cursor.rowcount
            except sqlite3.Error as e:
                logger.error(f"SQLite error during batch episode insert for series_id {series_id}: {e}")
                conn.rollback()
            finally:
                conn.close()

        # Update the last_modified timestamp in the series table
        if updated_series > 0 or inserted_episodes > 0:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("UPDATE series SET last_modified = ? WHERE series_id = ?", (int(datetime.now().timestamp()), series_id))
            conn.commit()
            conn.close()

        logger.info(f"[{index}/{total_series}] Series {series_id} updated: {updated_series}, Episodes added: {inserted_episodes}")
    else:
        logger.info(f"[{index}/{total_series}] No metadata available for series_id {series_id}")

    return updated_series, inserted_episodes

def main():
    logger.info("Starting series metadata update")

    series_list = get_series(DB_PATH)
    total_series = len(series_list)
    print(f"Found {total_series} series to process", flush=True)

    total_updated_series = 0
    total_inserted_episodes = 0
    has_errors = False

    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
        futures = {executor.submit(process_series, DB_PATH, i+1, series_tuple, total_series): series_tuple for i, series_tuple in enumerate(series_list)}
        for future in as_completed(futures):
            series_tuple = futures[future]
            series_id = series_tuple[0]
            try:
                updated_series, inserted_episodes = future.result()
                total_updated_series += updated_series
                total_inserted_episodes += inserted_episodes
            except Exception as e:
                import traceback
                logger.error(f"Error processing series {series_id}: {e}")
                traceback.print_exc()
                has_errors = True

    print(f"Series metadata update completed: total series updated {total_updated_series}, total episodes added {total_inserted_episodes}", flush=True)

    return not has_errors

if __name__ == "__main__":
    main()