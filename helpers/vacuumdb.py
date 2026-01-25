import sqlite3
from pathlib import Path
import os
import logging # Import logging

logger = logging.getLogger(__name__) # Initialize logger

DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"

def get_db_size(db_path):
    return os.path.getsize(db_path) if db_path.exists() else 0

def vacuum_database():
    logger.info(f"Database location: {DB_PATH}")
    size_before = get_db_size(DB_PATH)
    logger.info(f"Size before VACUUM: {size_before / (1024*1024):.2f} MB")
    conn = None # Initialize conn
    try:
        conn = sqlite3.connect(DB_PATH)

        # Clean large, non-essential metadata from tables to reduce DB size
        try:
            logger.info("Removing non-essential metadata from vod_streams and episodes...")
            cur = conn.cursor()

            # Clear large image/text fields from vod_streams while keeping identifiers
            # Clear large fields from vod_streams to minimize DB size; keep ids and tmdb
            cur.execute(
                """
                UPDATE vod_streams SET
                    stream_icon = NULL,
                    rating = NULL,
                    rating_5based = NULL,
                    container_extension = NULL,
                    custom_sid = NULL,
                    direct_source = NULL,
                    plot = NULL,
                    cast = NULL,
                    director = NULL,
                    genre = NULL,
                    release_date = NULL,
                    duration_secs = NULL,
                    duration = NULL,
                    video_quality = NULL,
                    o_name = NULL,
                    cover_big = NULL,
                    movie_image = NULL,
                    youtube_trailer = NULL,
                    actors = NULL,
                    description = NULL,
                    age = NULL,
                    country = NULL,
                    backdrop_path = NULL,
                    bitrate = NULL,
                    status = NULL,
                    runtime = NULL,
                    clearlogo = NULL
                """
            )
            vod_updated = cur.rowcount

            # Clear large fields from episodes while retaining ids and basic info
            cur.execute(
                """
                UPDATE episodes SET
                    plot = NULL,
                    duration = NULL,
                    airdate = NULL,
                    container_extension = NULL,
                    rating = NULL,
                    crew = NULL,
                    movie_image = NULL,
                    duration_secs = NULL,
                    video = NULL,
                    audio = NULL,
                    bitrate = NULL,
                    custom_sid = NULL,
                    direct_source = NULL
                """
            )
            episodes_updated = cur.rowcount

            conn.commit()
            logger.info(f"Cleared metadata: vod_streams rows updated={vod_updated}, episodes rows updated={episodes_updated}")
        except Exception as e:
            logger.warning(f"Failed to clear metadata before VACUUM: {e}")

        # Perform VACUUM to reclaim space after deletions
        conn.execute("VACUUM")
        conn.close()
        size_after = get_db_size(DB_PATH)
        logger.info(f"Size after VACUUM: {size_after / (1024*1024):.2f} MB")
        logger.info(f"Space reclaimed: {(size_before - size_after) / (1024*1024):.2f} MB")
        return True # Return True on success
    except sqlite3.Error as e:
        logger.error(f"Error during VACUUM: {e}")
        return False # Return False on error
    finally:
        if conn: # Ensure connection is closed even if an error occurs
            conn.close()

if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    success = vacuum_database()
    exit(0 if success else 1)