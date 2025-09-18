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