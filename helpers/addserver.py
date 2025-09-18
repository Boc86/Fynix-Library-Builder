import sqlite3
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def add_iptv_server(db_path, name, url, username, password, port):
    """Insert IPTV server if it doesn’t already exist."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if server already exists (by url + username)
        cursor.execute("""
            SELECT id FROM servers WHERE url = ? AND username = ?
        """, (url, username))
        
        if cursor.fetchone():
            logger.info(f"Server {name} already exists, skipping insert.")
            return True
        else:
            cursor.execute("""
                INSERT INTO servers (name, url, username, password, port, status)
                VALUES (?, ?, ?, ?, ?, 'active')
            """, (name, url, username, password, port))
            conn.commit()
            logger.info(f"Added server {name}")
            return True
    except sqlite3.Error as e:
        logger.error(f"Error adding server {name}: {e}")
        return False
    finally:
        if conn:
            conn.close()