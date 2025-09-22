import sqlite3
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MediaPlayerDB:
    def __init__(self, db_path="database/media_player.db"):
        """Initialize database connection"""
        self.db_path = Path(db_path)
        # Ensure the directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
        
    def connect(self):
        """Connect to SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
            logger.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def create_tables(self):
        """Create all required tables with proper relationships"""
        
        tables = {
            'servers': '''
                CREATE TABLE IF NOT EXISTS servers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    url TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    port INTEGER DEFAULT 80,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            'categories': '''
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER NOT NULL,
                    category_id INTEGER NOT NULL,
                    category_name TEXT NOT NULL,
                    parent_id INTEGER DEFAULT NULL,
                    content_type TEXT NOT NULL CHECK(content_type IN ('live', 'vod', 'series')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    visible INTEGER DEFAULT 1,
                    FOREIGN KEY (server_id) REFERENCES servers (id) ON DELETE CASCADE,
                    FOREIGN KEY (parent_id) REFERENCES categories (id) ON DELETE SET NULL,
                    UNIQUE(server_id, category_id, content_type)
                )
            ''',
            
            'live_streams': '''
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
                    visible INTEGER DEFAULT 1,
                    FOREIGN KEY (server_id) REFERENCES servers (id) ON DELETE CASCADE,
                    FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE SET NULL,
                    UNIQUE(server_id, stream_id)
                )
            ''',
            
            'vod_streams': '''
                CREATE TABLE IF NOT EXISTS vod_streams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER,
                    category_id INTEGER,
                    stream_id INTEGER,
                    name TEXT,
                    stream_icon TEXT,
                    rating REAL,
                    rating_5based REAL,
                    added TEXT,
                    container_extension TEXT,
                    custom_sid TEXT,
                    direct_source TEXT,
                    plot TEXT,
                    cast TEXT,
                    director TEXT,
                    genre TEXT,
                    release_date TEXT,
                    duration_secs INTEGER,
                    duration TEXT,
                    video_quality TEXT,
                    tmdb_id TEXT,
                    o_name TEXT,
                    cover_big TEXT,
                    movie_image TEXT,
                    youtube_trailer TEXT,
                    actors TEXT,
                    description TEXT,
                    age TEXT,
                    country TEXT,
                    backdrop_path TEXT,
                    bitrate INTEGER,
                    status TEXT,
                    runtime TEXT,
                    clearlogo TEXT
                )
            ''',
            
            'series': '''
                CREATE TABLE IF NOT EXISTS series (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER,
                    series_id INTEGER,
                    name TEXT,
                    cover TEXT,
                    plot TEXT,
                    cast TEXT,
                    director TEXT,
                    genre TEXT,
                    release_date TEXT,
                    last_modified TEXT,
                    rating REAL,
                    rating_5based REAL,
                    backdrop_path TEXT,
                    youtube_trailer TEXT,
                    tmdb_id TEXT,
                    episode_run_time TEXT,
                    category_id INTEGER,
                    category_ids TEXT,
                    clearlogo TEXT
                )
            ''',
            'episodes': '''
                CREATE TABLE IF NOT EXISTS episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER,
                    series_id INTEGER,
                    season_num INTEGER,
                    episode_id INTEGER,
                    title TEXT,
                    plot TEXT,
                    duration TEXT,
                    airdate TEXT,
                    container_extension TEXT,
                    episode_num INTEGER,
                    rating REAL,
                    crew TEXT,
                    tmdb_id TEXT,
                    movie_image TEXT,
                    duration_secs INTEGER,
                    video TEXT,
                    audio TEXT,
                    bitrate INTEGER,
                    custom_sid TEXT,
                    added TEXT,
                    direct_source TEXT,
                    season INTEGER
                )
            ''',
            
            'epg_data': '''
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
            ''',
            
            'user_preferences': '''
                CREATE TABLE IF NOT EXISTS user_preferences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT DEFAULT 'default',
                    preference_key TEXT NOT NULL,
                    preference_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, preference_key)
                )
            '''
        }
        
        try:
            cursor = self.conn.cursor()
            
            for table_name, create_sql in tables.items():
                logger.info(f"Creating table: {table_name}")
                cursor.execute(create_sql)
            
            # Create indexes for better performance
            self.create_indexes(cursor)
            
            self.conn.commit()
            logger.info("All tables created successfully")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            self.conn.rollback()
            return False
    
    def create_indexes(self, cursor):
        """Create indexes for better query performance"""
        
        indexes = [
            # Live streams indexes
            "CREATE INDEX IF NOT EXISTS idx_live_server_category ON live_streams(server_id, category_id)",
            "CREATE INDEX IF NOT EXISTS idx_live_name ON live_streams(name)",
            "CREATE INDEX IF NOT EXISTS idx_live_epg_channel ON live_streams(epg_channel_id)",
            
            # VOD streams indexes  
            "CREATE INDEX IF NOT EXISTS idx_vod_server_category ON vod_streams(server_id, category_id)",
            "CREATE INDEX IF NOT EXISTS idx_vod_name ON vod_streams(name)",
            "CREATE INDEX IF NOT EXISTS idx_vod_genre ON vod_streams(genre)",
            "CREATE INDEX IF NOT EXISTS idx_vod_rating ON vod_streams(rating)",
            "CREATE INDEX IF NOT EXISTS idx_vod_release_date ON vod_streams(release_date)",
            
            # Series indexes
            "CREATE INDEX IF NOT EXISTS idx_series_server_category ON series(server_id, category_id)",
            "CREATE INDEX IF NOT EXISTS idx_series_name ON series(name)",
            "CREATE INDEX IF NOT EXISTS idx_series_genre ON series(genre)",
            "CREATE INDEX IF NOT EXISTS idx_series_rating ON series(rating)",
            
            # Episodes indexes
            "CREATE INDEX IF NOT EXISTS idx_episodes_series ON episodes(series_id)",
            "CREATE INDEX IF NOT EXISTS idx_episodes_season ON episodes(series_id, season_num)",
            
            # Categories indexes
            "CREATE INDEX IF NOT EXISTS idx_categories_server_type ON categories(server_id, content_type)",
            "CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories(parent_id)",
            
            # EPG indexes
            "CREATE INDEX IF NOT EXISTS idx_epg_channel_time ON epg_data(channel_id, start_time)",
            "CREATE INDEX IF NOT EXISTS idx_epg_time_range ON epg_data(start_time, stop_time)",
            
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
                logger.debug(f"Created index: {index_sql.split('idx_')[1].split(' ON')[0]}")
            except sqlite3.Error as e:
                logger.warning(f"Index creation warning: {e}")
    
    def check_database_exists(self):
        """Check if database file exists"""
        return self.db_path.exists()
    
    def get_table_count(self):
        """Get count of tables in database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table'")
            return cursor.fetchone()[0]
        except sqlite3.Error:
            return 0
    
    def setup_database(self):
        """Main setup method"""
        db_exists = self.check_database_exists()
        
        if not self.connect():
            return False
        
        table_count = self.get_table_count()
        
        if db_exists and table_count > 0:
            logger.info(f"Database already exists with {table_count} tables")
            return True
        
        logger.info("Setting up new database...")
        success = self.create_tables()
        
        if success:
            logger.info("Database setup completed successfully!")
        else:
            logger.error("Database setup failed!")
        
        return success


def main():
    """Main function to run the database setup"""
    # Get the project root directory (parent of helpers folder)
    script_dir = Path(__file__).parent  # helpers folder
    project_root = script_dir.parent    # Media Player folder
    database_dir = project_root / "database"
    
    # Create database directory if it doesn't exist
    database_dir.mkdir(exist_ok=True)
    
    db_path = database_dir / "media_player.db"
    
    logger.info("Starting Media Player Database Setup")
    logger.info(f"Database location: {db_path}")
    
    db = MediaPlayerDB(db_path)
    
    try:
        success = db.setup_database()
        
        if success:
            # Display some basic info about the database
            table_count = db.get_table_count()
            db_size = db.db_path.stat().st_size if db.db_path.exists() else 0
            
            logger.info(f"Setup complete!")
            logger.info(f"Tables created: {table_count}")
            logger.info(f"Database size: {db_size} bytes")
            
        return success
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        db.close()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)