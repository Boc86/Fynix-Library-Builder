import os
import sqlite3
import shutil
from pathlib import Path
import logging
import sys
import json

import helpers.setupdb as setupdb
import helpers.addserver as addserver
import helpers.updatecats as updatecats
import helpers.updatemovies as updatemovies
import helpers.updateseries as updateseries
import helpers.updatemoviemetadata as updatemoviemetadata
import helpers.updateseriesmetadata as updateseriesmetadata
import helpers.vacuumdb as vacuumdb
import helpers.config_manager as config_manager
import helpers.create_strm_files as create_strm_files
import helpers.create_series_strm_files as create_series_strm_files

# --- Constants ---
CURRENT_DIR = os.getcwd()
DB_FILEPATH = os.path.join(CURRENT_DIR, "database", "media_player.db")
CACHE_DIR = Path(os.path.join(CURRENT_DIR, "database", "cache"))
SCHEDULE_FILE = os.path.join(CURRENT_DIR, "schedule.json")

# Setup logging
log_file_path = Path('fynix_library_builder.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path),
                        logging.StreamHandler(sys.stdout)
                    ])

# --- Schedule Management ---
def save_schedule(enabled, time_str):
    """Saves the auto-update schedule to a JSON file."""
    schedule = {'enabled': enabled, 'time': time_str}
    try:
        with open(SCHEDULE_FILE, 'w') as f:
            json.dump(schedule, f, indent=4)
        return True
    except IOError as e:
        logging.error(f"Failed to save schedule: {e}")
        return False

def load_schedule():
    """Loads the auto-update schedule from a JSON file."""
    if not os.path.exists(SCHEDULE_FILE):
        return {'enabled': False, 'time': "03:00"} # Default schedule
    try:
        with open(SCHEDULE_FILE, 'r') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load schedule: {e}")
        return {'enabled': False, 'time': "03:00"} # Return default on error

# --- Database Existence Check ---
def database_exists():
    """Checks if the database file exists."""
    return os.path.exists(DB_FILEPATH)

# --- Database Interaction Functions ---
def get_servers():
    """Retrieves all servers from the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, url, username, password, port FROM servers")
        servers = cursor.fetchall()
        return [dict(server) for server in servers]
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve servers: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_server(server_id, name, url, username, password, port):
    """Updates a server's details in the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE servers SET name=?, url=?, username=?, password=?, port=? WHERE id=?",
            (name, url, username, password, port, server_id)
        )
        conn.commit()
        logging.info(f"Server '{name}' updated successfully!")
        return True
    except sqlite3.Error as e:
        logging.error(f"Failed to update server '{name}': {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_categories():
    """Retrieves all categories from the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, category_name, content_type, visible FROM categories ORDER BY content_type, category_name")
        categories = cursor.fetchall()
        return [dict(cat) for cat in categories]
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve categories: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_category_visibility(category_id, visible_status):
    """Updates the visibility of a category."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE categories SET visible=? WHERE id=?",
            (visible_status, category_id)
        )
        conn.commit()
        return True
    except sqlite3.Error as e:
        logging.error(f"Failed to update category ID {category_id} visibility: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_database_statistics():
    """Queries the database to get various content statistics."""
    stats = {
        'total_movies': 0, 'visible_movies': 0,
        'total_series': 0, 'visible_series': 0,
        'total_episodes': 0, 'visible_episodes': 0
    }
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()

        # Total counts
        cursor.execute("SELECT COUNT(id) FROM vod_streams")
        stats['total_movies'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(id) FROM series")
        stats['total_series'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(id) FROM episodes")
        stats['total_episodes'] = cursor.fetchone()[0]

        # Visible counts
        cursor.execute("SELECT COUNT(v.id) FROM vod_streams v JOIN categories c ON v.category_id = c.id WHERE c.visible = 1")
        stats['visible_movies'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(s.id) FROM series s JOIN categories c ON s.category_id = c.id WHERE c.visible = 1")
        stats['visible_series'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(e.id) FROM episodes e JOIN series s ON e.series_id = s.id JOIN categories c ON s.category_id = c.id WHERE c.visible = 1")
        stats['visible_episodes'] = cursor.fetchone()[0]

    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve database statistics: {e}")
    finally:
        if conn:
            conn.close()
    return stats

# --- Core Logic Functions ---

def run_initial_setup(server_details, movie_path, series_path, progress_callback):
    """Runs the entire initial database setup and sync process."""
    server_name, server_url, server_username, server_password, server_port = server_details

    scripts_to_run = [
        ("Setting up database tables", setupdb.main),
        ("Saving library folder configurations", lambda: config_manager.save_directories(movie_path, series_path)),
        ("Adding server information", lambda: addserver.add_iptv_server(DB_FILEPATH, server_name, server_url, server_username, server_password, server_port)),
        ("Updating categories", updatecats.main),
        ("Updating movies", updatemovies.main),
        ("Updating series", updateseries.main),
        ("Updating movie metadata", updatemoviemetadata.main),
        ("Updating series metadata", updateseriesmetadata.main),
        ("Vacuuming database", vacuumdb.vacuum_database),
    ]

    for description, script_func in scripts_to_run:
        progress_callback(f"{description}... ")
        try:
            if not script_func():
                raise Exception(f"{description} failed.")
            progress_callback("DONE\n")
        except Exception as e:
            error_message = f"FAILED: {e}\n"
            progress_callback(error_message)
            logging.error(f"Failed during {description}: {e}")
            return False
    
    progress_callback("All scripts completed successfully!\n")
    return True

def run_library_update(progress_callback):
    """Creates .strm files for movies and series."""
    progress_callback("Updating library...")
    try:
        if not create_strm_files.main():
            raise Exception("Creating movie .strm files failed.")
        
        if not create_series_strm_files.main():
            raise Exception("Creating series .strm files failed.")
            
        progress_callback("Library update complete!")
        return True
    except Exception as e:
        logging.error(f"Error during library update: {e}")
        progress_callback(f"Update failed: {e}")
        return False

def run_clear_cache(progress_callback):
    """Clears all cached metadata."""
    progress_callback("Clearing cache...")
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        for item in CACHE_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        progress_callback("Cache cleared successfully.")
        logging.info("Cache cleared successfully.")
        return True
    except Exception as e:
        logging.error(f"Error clearing cache: {e}")
        progress_callback(f"Error clearing cache: {e}")
        return False
