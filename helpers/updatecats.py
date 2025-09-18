import sqlite3
import requests
import json
import logging
import time
import hashlib
import pickle
import urllib3
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime, timedelta

# Disable SSL warnings for IPTV servers with invalid certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class XtreamCategoriesDownloader:
    def __init__(self, db_path: str = None, cache_hours: int = 24):
        """Initialize the categories downloader with caching support"""
        if db_path is None:
            # Default path for project structure
            script_dir = Path(__file__).parent
            project_root = script_dir.parent
            db_path = project_root / "database" / "media_player.db"
        
        self.db_path = Path(db_path)
        self.cache_hours = cache_hours
        self.conn = None
        
        # Setup requests session with SSL handling for IPTV servers
        self.session = requests.Session()
        self.session.timeout = 30
        self.session.verify = False  # Disable SSL verification for IPTV servers
        
        # Set user agent to avoid blocking
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Setup cache directory
        self.cache_dir = self.db_path.parent / "cache" / "categories"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir}")
        logger.info("SSL verification disabled for IPTV server compatibility")
    
    def _generate_cache_key(self, server: Dict, content_type: str) -> str:
        """Generate a unique cache key for server + content type combination"""
        # Create a unique identifier from server details and content type
        key_data = f"{server['url']}_{server['username']}_{content_type}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cache_file_path(self, cache_key: str) -> Path:
        """Get the cache file path for a given cache key"""
        return self.cache_dir / f"{cache_key}.pkl"
    
    def _is_cache_valid(self, cache_file: Path) -> bool:
        """Check if cache file exists and is not older than cache_hours"""
        if not cache_file.exists():
            return False
        
        file_modified = datetime.fromtimestamp(cache_file.stat().st_mtime)
        expiry_time = datetime.now() - timedelta(hours=self.cache_hours)
        
        is_valid = file_modified > expiry_time
        if is_valid:
            logger.debug(f"Cache valid: {cache_file.name} (modified: {file_modified})")
        else:
            logger.debug(f"Cache expired: {cache_file.name} (modified: {file_modified}, expires after: {expiry_time})")
        
        return is_valid
    
    def _save_to_cache(self, cache_key: str, data: List[Dict]) -> bool:
        """Save API response data to cache"""
        try:
            cache_file = self._get_cache_file_path(cache_key)
            cache_data = {
                'timestamp': datetime.now(),
                'data': data
            }
            
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.debug(f"Saved {len(data)} items to cache: {cache_file.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to cache {cache_key}: {e}")
            return False
    
    def _load_from_cache(self, cache_key: str) -> Optional[List[Dict]]:
        """Load data from cache if valid"""
        try:
            cache_file = self._get_cache_file_path(cache_key)
            
            if not self._is_cache_valid(cache_file):
                return None
            
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            
            data = cache_data.get('data', [])
            timestamp = cache_data.get('timestamp')
            
            logger.info(f"Loaded {len(data)} items from cache: {cache_file.name} (cached: {timestamp})")
            return data
            
        except Exception as e:
            logger.warning(f"Error loading from cache {cache_key}: {e}")
            return None
    
    def _clear_expired_cache(self):
        """Remove expired cache files"""
        try:
            expired_count = 0
            for cache_file in self.cache_dir.glob("*.pkl"):
                if not self._is_cache_valid(cache_file):
                    cache_file.unlink()
                    expired_count += 1
                    logger.debug(f"Removed expired cache file: {cache_file.name}")
            
            if expired_count > 0:
                logger.info(f"Cleaned up {expired_count} expired cache files")
                
        except Exception as e:
            logger.warning(f"Error clearing expired cache: {e}")
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        try:
            cache_files = list(self.cache_dir.glob("*.pkl"))
            valid_files = [f for f in cache_files if self._is_cache_valid(f)]
            expired_files = [f for f in cache_files if not self._is_cache_valid(f)]
            
            total_size = sum(f.stat().st_size for f in cache_files)
            
            return {
                'total_files': len(cache_files),
                'valid_files': len(valid_files),
                'expired_files': len(expired_files),
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / 1024 / 1024, 2),
                'cache_directory': str(self.cache_dir)
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}
        
    def connect_db(self) -> bool:
        """Connect to the database"""
        try:
            if not self.db_path.exists():
                logger.error(f"Database not found: {self.db_path}")
                logger.error("Please run database_setup.py first")
                return False
                
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Enable dict-like access
            logger.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def get_servers(self) -> List[Dict]:
        """Get all active servers from database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id, name, url, username, password, port 
                FROM servers 
                WHERE status = 'active'
            """)
            servers = [dict(row) for row in cursor.fetchall()]
            logger.info(f"Found {len(servers)} active servers")
            return servers
        except sqlite3.Error as e:
            logger.error(f"Error fetching servers: {e}")
            return []
    
    def build_api_url(self, server: Dict, action: str = "") -> str:
        """Build Xtream API URL"""
        base_url = server['url']
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"
        
        port = server.get('port', 80)
        if port and port != 80:
            if ':' not in base_url.split('://', 1)[1]:
                base_url = f"{base_url}:{port}"
        
        api_url = urljoin(base_url, '/player_api.php')
        
        params = f"username={server['username']}&password={server['password']}"
        if action:
            params += f"&action={action}"
            
        return f"{api_url}?{params}"
    
    def test_server_connection(self, server: Dict) -> bool:
        """Test if server is reachable and credentials are valid"""
        try:
            url = self.build_api_url(server)
            logger.info(f"Testing server: {server['name']}")
            
            # Try with SSL verification disabled and increased timeout
            response = self.session.get(url, timeout=20, verify=False)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if response contains user info (indicates valid credentials)
            if 'user_info' in data and data['user_info']:
                logger.info(f"Server {server['name']} is accessible")
                return True
            else:
                logger.warning(f"Server {server['name']} returned invalid response or bad credentials")
                return False
                
        except requests.exceptions.SSLError as e:
            logger.error(f"SSL error for server {server['name']}: {e}")
            logger.info(f"Retrying {server['name']} with SSL verification disabled...")
            
            # Retry with explicit SSL disabled (double-check)
            try:
                response = self.session.get(url, timeout=20, verify=False)
                response.raise_for_status()
                data = response.json()
                
                if 'user_info' in data and data['user_info']:
                    logger.info(f"Server {server['name']} is accessible (SSL verification bypassed)")
                    return True
                else:
                    logger.warning(f"Server {server['name']} bad credentials even with SSL bypass")
                    return False
                    
            except Exception as retry_e:
                logger.error(f"Server {server['name']} still failing after SSL retry: {retry_e}")
                return False
                
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout connecting to server {server['name']}: {e}")
            return False
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for server {server['name']}: {e}")
            return False
        except requests.RequestException as e:
            logger.error(f"Request error for server {server['name']}: {e}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from server {server['name']}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error testing server {server['name']}: {e}")
            return False
    
    def download_categories(self, server: Dict, content_type: str) -> List[Dict]:
        """Download categories for specific content type with caching support"""
        action_map = {
            'live': 'get_live_categories',
            'vod': 'get_vod_categories', 
            'series': 'get_series_categories'
        }
        
        if content_type not in action_map:
            logger.error(f"Invalid content type: {content_type}")
            return []
        
        # Generate cache key and check cache first
        cache_key = self._generate_cache_key(server, content_type)
        cached_data = self._load_from_cache(cache_key)
        
        if cached_data is not None:
            logger.info(f"Using cached {content_type} categories for {server['name']} ({len(cached_data)} items)")
            return cached_data
        
        # Cache miss or expired - fetch from API
        try:
            url = self.build_api_url(server, action_map[content_type])
            logger.info(f"Fetching {content_type} categories from {server['name']} (cache miss)")
            
            response = self.session.get(url, timeout=30, verify=False)
            response.raise_for_status()
            
            categories = response.json()
            
            if isinstance(categories, list):
                logger.info(f"Downloaded {len(categories)} {content_type} categories from {server['name']}")
                
                # Save to cache
                if self._save_to_cache(cache_key, categories):
                    logger.debug(f"Cached {content_type} categories for {server['name']}")
                
                return categories
            else:
                logger.warning(f"Unexpected response format for {content_type} categories from {server['name']}")
                return []
                
        except requests.exceptions.SSLError as e:
            logger.warning(f"SSL error downloading {content_type} from {server['name']}: {e}")
            logger.info("SSL verification already disabled, this might be a server issue")
            return []
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout downloading {content_type} categories from {server['name']}: {e}")
            return []
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error downloading {content_type} categories from {server['name']}: {e}")
            return []
        except requests.RequestException as e:
            logger.error(f"Error downloading {content_type} categories from {server['name']}: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response for {content_type} categories from {server['name']}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error downloading {content_type} categories from {server['name']}: {e}")
            return []
    
    def category_exists(self, server_id: int, category_id: int, content_type: str) -> bool:
        """Check if category already exists in database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT 1 FROM categories 
                WHERE server_id = ? AND category_id = ? AND content_type = ?
            """, (server_id, category_id, content_type))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking category existence: {e}")
            return False
    
    def insert_category(self, server_id: int, category_data: Dict, content_type: str) -> bool:
        """Insert category into database"""
        try:
            cursor = self.conn.cursor()
            
            # Extract category information
            category_id = category_data.get('category_id')
            category_name = category_data.get('category_name', 'Unknown')
            parent_id = category_data.get('parent_id')
            
            # Handle parent_id - some APIs might send 0 or empty string instead of null
            if parent_id in (0, '0', '', None):
                parent_id = None
            
            cursor.execute("""
                INSERT INTO categories (server_id, category_id, category_name, parent_id, content_type)
                VALUES (?, ?, ?, ?, ?)
            """, (server_id, category_id, category_name, parent_id, content_type))
            
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error inserting category {category_data.get('category_name', 'Unknown')}: {e}")
            return False
    
    def process_categories_for_server(self, server: Dict) -> Tuple[int, int, int]:
        """Process all category types for a single server"""
        total_downloaded = 0
        total_new = 0
        total_existing = 0
        
        content_types = ['live', 'vod', 'series']
        
        try:
            for content_type in content_types:
                logger.info(f"Processing {content_type} categories for server: {server['name']}")
                
                categories = self.download_categories(server, content_type)
                if not categories:
                    logger.warning(f"No {content_type} categories found for server: {server['name']}")
                    continue
                
                downloaded_count = len(categories)
                new_count = 0
                existing_count = 0
                
                # Process each category
                for category_data in categories:
                    if not isinstance(category_data, dict):
                        logger.warning(f"Invalid category data format: {category_data}")
                        continue
                    
                    category_id = category_data.get('category_id')
                    if category_id is None:
                        logger.warning(f"Category missing category_id: {category_data}")
                        continue
                    
                    # Check if category already exists
                    if self.category_exists(server['id'], category_id, content_type):
                        existing_count += 1
                        logger.debug(f"Category already exists: {category_data.get('category_name', 'Unknown')}")
                        continue
                    
                    # Insert new category
                    if self.insert_category(server['id'], category_data, content_type):
                        new_count += 1
                        logger.debug(f"Inserted new category: {category_data.get('category_name', 'Unknown')}")
                
                # Commit after each content type
                self.conn.commit()
                
                logger.info(f"Server {server['name']} - {content_type}: {downloaded_count} downloaded, {new_count} new, {existing_count} existing")
                
                total_downloaded += downloaded_count
                total_new += new_count
                total_existing += existing_count
                
                # Small delay between requests to be respectful
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"Error processing categories for server {server['name']}: {e}")
            self.conn.rollback()
        
        return total_downloaded, total_new, total_existing
    
    def download_all_categories(self) -> bool:
        """Download categories from all active servers with caching support"""
        if not self.connect_db():
            return False
        
        try:
            # Clean up expired cache files first
            self._clear_expired_cache()
            
            servers = self.get_servers()
            if not servers:
                logger.warning("No active servers found in database")
                return False
            
            total_servers = len(servers)
            successful_servers = 0
            grand_total_downloaded = 0
            grand_total_new = 0
            grand_total_existing = 0
            cache_hits = 0
            api_calls = 0
            
            logger.info(f"Starting category download for {total_servers} servers")
            
            # Show cache stats
            cache_stats = self.get_cache_stats()
            if cache_stats:
                logger.info(f"Cache status: {cache_stats['valid_files']} valid files, {cache_stats['expired_files']} expired, {cache_stats['total_size_mb']} MB")
            
            for i, server in enumerate(servers, 1):
                logger.info(f"Processing server {i}/{total_servers}: {server['name']}")
                
                # Test server connection first (but only if we don't have valid cache for all content types)
                content_types = ['live', 'vod', 'series']
                has_all_cached = all(
                    self._load_from_cache(self._generate_cache_key(server, ct)) is not None 
                    for ct in content_types
                )
                
                if not has_all_cached and not self.test_server_connection(server):
                    logger.error(f"Skipping server {server['name']} due to connection issues")
                    continue
                
                # Process categories for this server
                downloaded, new, existing = self.process_categories_for_server(server)
                
                # Count cache hits vs API calls for this server
                server_cache_hits = 0
                server_api_calls = 0
                
                for content_type in content_types:
                    cache_key = self._generate_cache_key(server, content_type)
                    if self._load_from_cache(cache_key) is not None:
                        server_cache_hits += 1
                    else:
                        server_api_calls += 1
                
                cache_hits += server_cache_hits
                api_calls += server_api_calls
                
                if downloaded > 0:
                    successful_servers += 1
                    grand_total_downloaded += downloaded
                    grand_total_new += new
                    grand_total_existing += existing
                    
                    cache_info = f"({server_cache_hits} cached, {server_api_calls} API calls)"
                    logger.info(f"Server {server['name']} completed: {downloaded} total, {new} new, {existing} existing {cache_info}")
                else:
                    logger.warning(f"No categories downloaded from server: {server['name']}")
                
                # Shorter delay between servers when using cache
                if i < total_servers:
                    delay = 0.5 if server_cache_hits > 0 else 2
                    time.sleep(delay)
            
            # Final summary with cache statistics
            logger.info("="*70)
            logger.info("CATEGORY DOWNLOAD SUMMARY")
            logger.info("="*70)
            logger.info(f"Servers processed: {successful_servers}/{total_servers}")
            logger.info(f"Total categories downloaded: {grand_total_downloaded}")
            logger.info(f"New categories added: {grand_total_new}")
            logger.info(f"Existing categories skipped: {grand_total_existing}")
            logger.info(f"Cache performance: {cache_hits} hits, {api_calls} API calls")
            if cache_hits + api_calls > 0:
                cache_hit_rate = (cache_hits / (cache_hits + api_calls)) * 100
                logger.info(f"Cache hit rate: {cache_hit_rate:.1f}%")
            
            return successful_servers > 0
            
        except Exception as e:
            logger.error(f"Unexpected error during category download: {e}")
            return False
        
        finally:
            self.close_db()
    
    def get_category_stats(self) -> Dict:
        """Get statistics about categories in database"""
        if not self.connect_db():
            return {}
        
        try:
            cursor = self.conn.cursor()
            
            stats = {}
            
            # Total categories by content type
            cursor.execute("""
                SELECT content_type, COUNT(*) as count 
                FROM categories 
                GROUP BY content_type
            """)
            for row in cursor.fetchall():
                stats[f"{row['content_type']}_categories"] = row['count']
            
            # Total categories
            cursor.execute("SELECT COUNT(*) as total FROM categories")
            stats['total_categories'] = cursor.fetchone()['total']
            
            # Categories by server
            cursor.execute("""
                SELECT s.name, COUNT(c.id) as count
                FROM servers s
                LEFT JOIN categories c ON s.id = c.server_id
                GROUP BY s.id, s.name
            """)
            server_stats = {}
            for row in cursor.fetchall():
                server_stats[row['name']] = row['count']
            stats['categories_by_server'] = server_stats
            
            return stats
            
        except sqlite3.Error as e:
            logger.error(f"Error getting category stats: {e}")
            return {}
        finally:
            self.close_db()


def main():
    """Main function to run the categories downloader with caching"""
    logger.info("Starting Xtream API Categories Download with Caching")
    
    downloader = XtreamCategoriesDownloader(cache_hours=24)
    
    # Show cache statistics
    logger.info("Cache statistics:")
    cache_stats = downloader.get_cache_stats()
    if cache_stats:
        for key, value in cache_stats.items():
            logger.info(f"  {key}: {value}")
    else:
        logger.info("  No cache data available")
    
    # Show current category statistics
    logger.info("\nCurrent category statistics:")
    current_stats = downloader.get_category_stats()
    if current_stats:
        for key, value in current_stats.items():
            if key == 'categories_by_server':
                logger.info("Categories by server:")
                for server, count in value.items():
                    logger.info(f"  {server}: {count}")
            else:
                logger.info(f"  {key}: {value}")
    
    # Download categories (with caching)
    success = downloader.download_all_categories()
    
    if success:
        logger.info("Category download completed successfully!")
        
        # Show updated stats
        logger.info("\nUpdated category statistics:")
        updated_stats = downloader.get_category_stats()
        if updated_stats:
            for key, value in updated_stats.items():
                if key == 'categories_by_server':
                    logger.info("Categories by server:")
                    for server, count in value.items():
                        logger.info(f"  {server}: {count}")
                else:
                    logger.info(f"  {key}: {value}")
        
        # Show final cache stats
        logger.info("\nFinal cache statistics:")
        final_cache_stats = downloader.get_cache_stats()
        if final_cache_stats:
            for key, value in final_cache_stats.items():
                logger.info(f"  {key}: {value}")
    else:
        logger.error("Category download failed!")
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)