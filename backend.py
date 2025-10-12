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
import helpers.updatelive as updatelive
import helpers.defaultepggrabber as defaultepggrabber
import helpers.create_m3u_playlist as create_m3u_playlist
import helpers.create_epg_xml as create_epg_xml
import helpers.create_nfo_files as create_nfo_files
import helpers.create_series_nfo_files as create_series_nfo_files

# --- Constants ---
CURRENT_DIR = os.getcwd()
DB_FILEPATH = os.path.join(CURRENT_DIR, "database", "media_player.db")
CACHE_DIR = Path(os.path.join(CURRENT_DIR, "database", "cache"))
SCHEDULE_FILE = os.path.join(CURRENT_DIR, "schedule.json")
PREFERENCES_FILE = os.path.join(CURRENT_DIR, "preferences.json")

# Setup logging
log_file_path = Path('fynix_library_builder.log')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path),
                        logging.StreamHandler(sys.stdout)
                    ])

# --- Preference Management ---
def save_preference(key, value):
    """Saves a user preference to a JSON file."""
    preferences = {}
    if os.path.exists(PREFERENCES_FILE):
        try:
            with open(PREFERENCES_FILE, 'r') as f:
                preferences = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logging.warning(f"Failed to load existing preferences: {e}. Starting with empty preferences.")
    
    preferences[key] = value
    try:
        with open(PREFERENCES_FILE, 'w') as f:
            json.dump(preferences, f, indent=4)
        return True
    except IOError as e:
        logging.error(f"Failed to save preference '{key}': {e}")
        return False

def load_preference(key, default_value):
    """Loads a user preference from a JSON file, or returns a default value."""
    if not os.path.exists(PREFERENCES_FILE):
        return default_value
    try:
        with open(PREFERENCES_FILE, 'r') as f:
            preferences = json.load(f)
            return preferences.get(key, default_value)
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load preference '{key}': {e}. Returning default value.")
        return default_value

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

def check_for_missing_tables():
    """Checks if 'live_streams' or 'epg_data' tables are missing from the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('live_streams', 'epg_data');")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        missing_tables = []
        if 'live_streams' not in existing_tables:
            missing_tables.append('live_streams')
        if 'epg_data' not in existing_tables:
            missing_tables.append('epg_data')
            
        return len(missing_tables) > 0
    except sqlite3.Error as e:
        logging.error(f"Error checking for missing tables: {e}")
        return False # Assume tables are not missing to prevent accidental migration on error
    finally:
        if conn:
            conn.close()

def migrate_database(progress_callback=None):
    """Adds 'live_streams' and 'epg_data' tables to the database if they don't exist."""
    if progress_callback:
        progress_callback("Starting database migration...")
    
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()

        live_streams_sql = '''
            CREATE TABLE IF NOT EXISTS live_streams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id INTEGER NOT NULL,
                category_id INTEGER,
                stream_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                stream_type TEXT DEFAULT 'live',
                stream_icon TEXT,
                epg_channel_id TEXT,
                tv_archive INTEGER DEFAULT 0,
                direct_source TEXT,
                tv_archive_duration INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (server_id) REFERENCES servers (id) ON DELETE CASCADE,
                FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE SET NULL,
                UNIQUE(server_id, stream_id)
            )
        '''
        epg_data_sql = '''
            CREATE TABLE IF NOT EXISTS epg_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                stop_time TIMESTAMP NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                lang TEXT,
                category TEXT,
                icon TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel_id, start_time, title)
            )
        '''
        
        logging.info("Creating table: live_streams")
        cursor.execute(live_streams_sql)
        logging.info("Creating table: epg_data")
        cursor.execute(epg_data_sql)

        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_live_server_category ON live_streams(server_id, category_id)",
            "CREATE INDEX IF NOT EXISTS idx_live_name ON live_streams(name)",
            "CREATE INDEX IF NOT EXISTS idx_live_epg_channel ON live_streams(epg_channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_epg_channel_time ON epg_data(channel_id, start_time)",
            "CREATE INDEX IF NOT EXISTS idx_epg_time_range ON epg_data(start_time, stop_time)",
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
                logging.debug(f"Created index: {index_sql.split('idx_')[1].split(' ON')[0]}")
            except sqlite3.Error as e:
                logging.warning(f"Index creation warning: {e}")

        conn.commit()
        if progress_callback:
            progress_callback("Database migration completed successfully!")
        logging.info("Database migration completed successfully!")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error during database migration: {e}")
        if progress_callback:
            progress_callback(f"Database migration failed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def check_live_streams_visible_column_exists():
    """Checks if the 'visible' column exists in the 'live_streams' table."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(live_streams);")
        columns = [row[1] for row in cursor.fetchall()]
        return 'visible' in columns
    except sqlite3.Error as e:
        logging.error(f"Error checking 'visible' column in 'live_streams': {e}")
        return True # Assume it exists to avoid accidental migration
    finally:
        if conn:
            conn.close()

def migrate_add_visible_column_to_live_streams(progress_callback=None):
    """Adds the 'visible' column to the 'live_streams' table if it doesn't exist."""
    if progress_callback:
        progress_callback("Checking for 'visible' column in 'live_streams' table...")
    
    if check_live_streams_visible_column_exists():
        if progress_callback:
            progress_callback("'visible' column already exists.")
        return True

    if progress_callback:
        progress_callback("Adding 'visible' column to 'live_streams' table...")

    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE live_streams ADD COLUMN visible INTEGER DEFAULT 1;")
        conn.commit()
        if progress_callback:
            progress_callback("'visible' column added successfully!")
        logging.info("'visible' column added successfully to 'live_streams' table.")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error adding 'visible' column: {e}")
        if progress_callback:
            progress_callback(f"Failed to add 'visible' column: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


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

def get_live_categories():
    """Retrieves all live categories from the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, category_id, category_name, visible FROM categories WHERE content_type = 'live' ORDER BY category_name")
        categories = cursor.fetchall()
        return [dict(cat) for cat in categories]
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve live categories: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_live_streams_by_category(category_id):
    """Retrieves all live streams for a given category from the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, visible FROM live_streams WHERE category_id = ? ORDER BY name", (category_id,))
        streams = cursor.fetchall()
        return [dict(stream) for stream in streams]
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve live streams for category {category_id}: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_live_stream_visibility(stream_id, visible):
    """Updates the visibility of a live stream."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE live_streams SET visible = ? WHERE id = ?", (visible, stream_id))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logging.error(f"Failed to update live stream ID {stream_id} visibility: {e}")
        return False
    finally:
        if conn:
            conn.close()

def batch_update_live_stream_visibility(stream_ids, visible):
    """Updates the visibility of multiple live streams."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        cursor.executemany("UPDATE live_streams SET visible = ? WHERE id = ?", [(visible, stream_id) for stream_id in stream_ids])
        conn.commit()
        return True
    except sqlite3.Error as e:
        logging.error(f"Failed to batch update live stream visibility: {e}")
        return False
    finally:
        if conn:
            conn.close()

# --- Core Logic Functions ---

def run_initial_setup(server_details, movie_path, series_path, live_tv_path, progress_callback):
    """Runs the entire initial database setup and sync process."""
    server_name, server_url, server_username, server_password, server_port = server_details

    scripts_to_run = [
        ("Setting up database tables", setupdb.main),
        ("Saving library folder configurations", lambda: config_manager.save_directories(movie_path, series_path, live_tv_path)),
        ("Adding server information", lambda: addserver.add_iptv_server(DB_FILEPATH, server_name, server_url, server_username, server_password, server_port)),
        ("Updating categories", updatecats.main),
        ("Updating live streams", updatelive.main),
        ("Grabbing EPG data", defaultepggrabber.main),
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

def run_library_update(process_live_tv, progress_callback=None):
    """Runs the full library update process."""
    progress_callback("Starting library update...")

    scripts_to_run = [
        ("Updating categories", updatecats.main),
    ]

    if process_live_tv:
        scripts_to_run.extend([
            ("Updating live streams", updatelive.main),
            ("Grabbing EPG data", defaultepggrabber.main),
            ("Generating M3U playlist", create_m3u_playlist.create_m3u_playlist),
            ("Generating EPG XML", create_epg_xml.generate_epg_xml),
        ])

    scripts_to_run.extend([
        ("Updating movies", updatemovies.main),
        ("Updating series", updateseries.main),
        ("Updating movie metadata", updatemoviemetadata.main),
        ("Updating series metadata", updateseriesmetadata.main),
        ("Creating movie .strm files", create_strm_files.main),
        ("Creating series .strm files", create_series_strm_files.main),
        ("Vacuuming database", vacuumdb.vacuum_database),
    ])

    for description, script_func in scripts_to_run:
        progress_callback(f"{description}... ")
        try:
            # For create_epg_xml, it needs the output path
            if script_func == create_epg_xml.generate_epg_xml:
                project_root = Path(CURRENT_DIR)
                output_file = project_root / "epg.xml"
                result = script_func(output_file) # Store result
                if not result: # Check result
                    print(f"ERROR: {description} failed. Result: {result}", file=sys.stderr) # Added
                    raise Exception(f"{description} failed.")
            else:
                result = script_func() # Store result
                if not result: # Check result
                    print(f"ERROR: {description} failed. Result: {result}", file=sys.stderr) # Added
                    raise Exception(f"{description} failed.")
            progress_callback("DONE\n")
        except Exception as e:
            import traceback # Import traceback here
            error_traceback = traceback.format_exc() # Get full traceback
            error_message = f"FAILED: {e}\n{error_traceback}" # Include traceback in message
            progress_callback(error_message)
            logging.error(f"Failed during {description}: {e}")
            print(f"ERROR: {error_message}", file=sys.stderr) # Print to stderr
            return False
    
    progress_callback("Library update completed successfully!")
    return True

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

def run_strm_and_nfo_creation(progress_callback=None):
    """Runs only the .strm and .nfo file creation scripts."""
    progress_callback("Starting .strm and .nfo file creation...")

    scripts_to_run = [
        ("Creating movie .strm files", create_strm_files.main),
        ("Creating series .strm files", create_series_strm_files.main),
        ("Creating movie .nfo files", create_nfo_files.main),
        ("Creating series .nfo files", create_series_nfo_files.main),
    ]

    for description, script_func in scripts_to_run:
        progress_callback(f"{description}... ")
        try:
            if not script_func():
                raise Exception(f"{description} failed.")
            progress_callback("DONE\n")
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            error_message = f"FAILED: {e}\n{error_traceback}"
            progress_callback(error_message)
            logging.error(f"Failed during {description}: {e}")
            print(f"ERROR: {error_message}", file=sys.stderr)
            return False
    
    progress_callback("File creation completed successfully!")
    return True