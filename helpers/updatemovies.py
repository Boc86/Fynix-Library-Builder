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
from datetime import datetime, timedelta

# Disable SSL warnings for IPTV servers with invalid certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class XtreamVODStreamsDownloader:
    def __init__(self, db_path: str = None, cache_hours: int = 24):
        """Initialize the VOD streams downloader with caching support"""
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
        self.cache_dir = self.db_path.parent / "cache" / "vod_streams"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir}")
        logger.info("SSL verification disabled for IPTV server compatibility")
    
    def _generate_cache_key(self, server: Dict, category_id: str = "all") -> str:
        """Generate a unique cache key for server + category combination"""
        key_data = f"{server['url']}_{server['username']}_vod_streams_{category_id}"
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
    
    def build_api_url(self, server: Dict, action: str = "", category_id: str = "") -> str:
        """Build Xtream API URL with proper Xtream formatting"""
        base_url = server['url']
        
        # Ensure URL has protocol
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"
        
        # Handle port
        port = server.get('port', 80)
        if port and port != 80 and port != 443:
            # Only add port if it's not already in URL
            if ':' not in base_url.split('://', 1)[1]:
                base_url = f"{base_url}:{port}"
        
        # Remove trailing slash if present
        base_url = base_url.rstrip('/')
        
        # Build the full API URL - Xtream format
        api_url = f"{base_url}/player_api.php"
        
        # Build parameters in Xtream API format
        params = f"username={server['username']}&password={server['password']}"
        
        if action:
            params += f"&action={action}"
            
        if category_id:
            params += f"&category_id={category_id}"
        
        full_url = f"{api_url}?{params}"
        
        logger.debug(f"Built API URL: {full_url}")
        return full_url
    
    def test_server_connection(self, server: Dict) -> bool:
        """Test if server is reachable and credentials are valid"""
        try:
            # First test basic connection without action
            url = self.build_api_url(server)
            logger.info(f"Testing server: {server['name']}")
            logger.debug(f"Test URL: {url}")
            
            response = self.session.get(url, timeout=20, verify=False)
            
            logger.info(f"Server response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Server {server['name']} returned status: {response.status_code}")
                return False
            
            # Try to parse JSON response
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Server {server['name']} returned invalid JSON: {e}")
                return False
            
            # Check if response contains user info or server info
            if isinstance(data, dict):
                if 'user_info' in data or 'server_info' in data:
                    logger.info(f"Server {server['name']} is accessible")
                    return True
                else:
                    logger.info(f"Server {server['name']} returned valid response")
                    return True
            else:
                logger.warning(f"Server {server['name']} returned non-dict response")
                return False
                
        except Exception as e:
            logger.error(f"Error testing server {server['name']}: {e}")
            return False
    
    def download_vod_streams(self, server: Dict, category_id: str = "") -> List[Dict]:
        """Download VOD streams for specific category with caching support"""
        
        # Generate cache key and check cache first
        cache_key = self._generate_cache_key(server, category_id or "all")
        cached_data = self._load_from_cache(cache_key)
        
        if cached_data is not None:
            category_info = f" (category: {category_id})" if category_id else " (all categories)"
            logger.info(f"Using cached VOD streams for {server['name']}{category_info} ({len(cached_data)} items)")
            return cached_data
        
        # Cache miss or expired - fetch from API
        try:
            url = self.build_api_url(server, "get_vod_streams", category_id)
            category_info = f" (category: {category_id})" if category_id else " (all categories)"
            logger.info(f"Fetching VOD streams from {server['name']}{category_info} (cache miss)")
            
            response = self.session.get(url, timeout=90, verify=False)  # Longer timeout for large VOD lists
            response.raise_for_status()
            
            streams = response.json()
            
            if isinstance(streams, list):
                logger.info(f"Downloaded {len(streams)} VOD streams from {server['name']}{category_info}")
                
                # Save to cache
                if self._save_to_cache(cache_key, streams):
                    logger.debug(f"Cached VOD streams for {server['name']}{category_info}")
                
                return streams
            else:
                logger.warning(f"Unexpected response format for VOD streams from {server['name']}")
                logger.debug(f"Response type: {type(streams)}, Content: {streams}")
                return []
                
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout downloading VOD streams from {server['name']}: {e}")
            return []
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error downloading VOD streams from {server['name']}: {e}")
            return []
        except requests.RequestException as e:
            logger.error(f"Error downloading VOD streams from {server['name']}: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response for VOD streams from {server['name']}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error downloading VOD streams from {server['name']}: {e}")
            return []
    
    def get_category_mapping(self, server_id: int) -> Dict[int, int]:
        """Get category ID mapping from database for VOD content (keys are integers)"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT category_id, id FROM categories 
                WHERE server_id = ? AND content_type = 'vod'
            """, (server_id,))
            # Keep integer keys to match API category_id as integer
            mapping = {int(row['category_id']): row['id'] for row in cursor.fetchall()}
            logger.info(f"Category mapping for server {server_id}: {mapping}")
            return mapping
        except sqlite3.Error as e:
            logger.error(f"Error getting category mapping: {e}")
            return {}
    
    def stream_exists(self, server_id: int, stream_id: int) -> bool:
        """Check if VOD stream already exists in database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT 1 FROM vod_streams 
                WHERE server_id = ? AND stream_id = ?
            """, (server_id, stream_id))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking stream existence: {e}")
            return False
    
    def parse_date(self, date_string: str) -> Optional[str]:
        """Parse date string and return in YYYY-MM-DD format"""
        if not date_string:
            return None
        
        try:
            # Handle common date formats
            if isinstance(date_string, str):
                # Remove extra whitespace
                date_string = date_string.strip()
                
                # Try different date formats
                from datetime import datetime as dt
                
                # Common formats in VOD data
                formats = [
                    '%Y-%m-%d',           # 2023-01-15
                    '%Y',                 # 2023
                    '%d-%m-%Y',           # 15-01-2023
                    '%m/%d/%Y',           # 01/15/2023
                    '%Y-%m-%d %H:%M:%S'   # 2023-01-15 12:00:00
                ]
                
                for fmt in formats:
                    try:
                        parsed = dt.strptime(date_string, fmt)
                        return parsed.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
                
                # If all formats fail, return original string
                return date_string[:10] if len(date_string) >= 10 else date_string
            
            return str(date_string)
            
        except Exception:
            return None
    
    def parse_duration(self, duration_str: str) -> Tuple[Optional[int], Optional[str]]:
        """Parse duration string and return seconds and formatted duration"""
        if not duration_str:
            return None, None
        
        try:
            duration_str = str(duration_str).strip()
            
            # If it's already in seconds
            if duration_str.isdigit():
                seconds = int(duration_str)
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                secs = seconds % 60
                formatted = f"{hours:02d}:{minutes:02d}:{secs:02d}"
                return seconds, formatted
            
            # If it's in HH:MM:SS format
            if ':' in duration_str:
                parts = duration_str.split(':')
                if len(parts) >= 2:
                    try:
                        hours = int(parts[0]) if len(parts) > 2 else 0
                        minutes = int(parts[-2])
                        seconds = int(parts[-1])
                        total_seconds = (hours * 3600) + (minutes * 60) + seconds
                        return total_seconds, duration_str
                    except ValueError:
                        pass
            
            return None, duration_str
            
        except Exception:
            return None, str(duration_str)
    
    
    def insert_stream(self, server_id: int, stream_data: Dict, category_mapping: Dict[int, int]) -> bool:
        """Insert VOD stream into database"""
        try:
            cursor = self.conn.cursor()
            
            # Extract stream information
            stream_id = stream_data.get('stream_id')
            if stream_id is None:
                logger.warning(f"Stream missing stream_id: {stream_data.get('name', 'Unknown')}")
                return False
            
            name = stream_data.get('name', 'Unknown')
            stream_icon = stream_data.get('stream_icon', '')
            rating = float(stream_data.get('rating', 0.0)) if stream_data.get('rating') else 0.0
            rating_5based = float(stream_data.get('rating_5based', 0.0)) if stream_data.get('rating_5based') else 0.0
            
            # Parse added timestamp
            added_str = stream_data.get('added', '')
            added = None
            if added_str:
                try:
                    added = datetime.fromtimestamp(int(added_str)).strftime('%Y-%m-%d %H:%M:%S') if added_str.isdigit() else added_str
                except Exception:
                    added = None
            
            container_extension = stream_data.get('container_extension', '')
            custom_sid = stream_data.get('custom_sid', '')
            direct_source = stream_data.get('direct_source', '')
            plot = stream_data.get('plot', '')
            cast = stream_data.get('cast', '')
            director = stream_data.get('director', '')
            genre = stream_data.get('genre', '')
            
            release_date = self.parse_date(stream_data.get('releaseDate', ''))
            duration_secs, duration = self.parse_duration(stream_data.get('duration', ''))
            video_quality = stream_data.get('quality', '')
            
            # --- FIXED CATEGORY MAPPING ---
            category_id_api = stream_data.get('category_id')
            db_category_id = None
            if category_id_api is not None:
                try:
                    db_category_id = category_mapping.get(int(category_id_api))
                except Exception:
                    db_category_id = None
            
            if db_category_id is None:
                logger.warning(f"No matching category in database for API category_id {category_id_api}. Using default ID 0.")
                db_category_id = 0  # Optional: use a default "Uncategorized" category ID

            # Insert into database
            cursor.execute("""
                INSERT INTO vod_streams (
                    server_id, category_id, stream_id, name, stream_icon, rating, rating_5based,
                    added, container_extension, custom_sid, direct_source, plot, cast, director,
                    genre, release_date, duration_secs, duration, video_quality
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                server_id, db_category_id, stream_id, name, stream_icon, rating, rating_5based,
                added, container_extension, custom_sid, direct_source, plot, cast, director,
                genre, release_date, duration_secs, duration, video_quality
            ))
            
            return True
        
        except sqlite3.Error as e:
            logger.error(f"Error inserting VOD stream {stream_data.get('name', 'Unknown')}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error inserting VOD stream {stream_data.get('name', 'Unknown')}: {e}")
            return False
    
    def process_streams_for_server(self, server: Dict) -> Tuple[int, int, int]:
        """Process all VOD streams for a single server"""
        total_downloaded = 0
        total_new = 0
        total_existing = 0
        
        try:
            logger.info(f"Processing VOD streams for server: {server['name']}")
            
            # Get category mapping for this server
            category_mapping = self.get_category_mapping(server['id'])
            logger.info(f"Found {len(category_mapping)} VOD categories for server {server['name']}")
            
            # Download all VOD streams
            streams = self.download_vod_streams(server)
            if not streams:
                logger.warning(f"No VOD streams found for server: {server['name']}")
                return 0, 0, 0
            
            downloaded_count = len(streams)
            new_count = 0
            existing_count = 0
            
            # Process streams in batches for better performance
            batch_size = 1000
            total_batches = (len(streams) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(streams))
                batch_streams = streams[start_idx:end_idx]
                
                logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_streams)} streams)")
                
                batch_new = 0
                batch_existing = 0
                
                # Process each stream in the batch
                for stream_data in batch_streams:
                    if not isinstance(stream_data, dict):
                        logger.warning(f"Invalid stream data format: {stream_data}")
                        continue
                    
                    stream_id = stream_data.get('stream_id')
                    if stream_id is None:
                        logger.warning(f"Stream missing stream_id: {stream_data}")
                        continue
                    
                    # Check if stream already exists
                    if self.stream_exists(server['id'], stream_id):
                        batch_existing += 1
                        logger.debug(f"Stream already exists: {stream_data.get('name', 'Unknown')}")
                        continue
                    
                    # Insert new stream
                    if self.insert_stream(server['id'], stream_data, category_mapping):
                        batch_new += 1
                        logger.debug(f"Inserted new stream: {stream_data.get('name', 'Unknown')}")
                
                # Commit after each batch
                self.conn.commit()
                
                new_count += batch_new
                existing_count += batch_existing
                
                logger.info(f"Batch {batch_num + 1} completed: {batch_new} new, {batch_existing} existing")
                
                # Short pause between batches
                if batch_num < total_batches - 1:
                    time.sleep(0.1)
            
            logger.info(f"Server {server['name']} - VOD streams: {downloaded_count} downloaded, {new_count} new, {existing_count} existing")
            
            total_downloaded = downloaded_count
            total_new = new_count
            total_existing = existing_count
        
        except Exception as e:
            logger.error(f"Error processing VOD streams for server {server['name']}: {e}")
            self.conn.rollback()
        
        return total_downloaded, total_new, total_existing
    
    def download_all_streams(self) -> bool:
        """Download VOD streams from all active servers with caching support"""
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
            
            logger.info(f"Starting VOD streams download for {total_servers} servers")
            
            # Show cache stats
            cache_stats = self.get_cache_stats()
            if cache_stats:
                logger.info(f"Cache status: {cache_stats['valid_files']} valid files, {cache_stats['expired_files']} expired, {cache_stats['total_size_mb']} MB")
            
            for i, server in enumerate(servers, 1):
                logger.info(f"Processing server {i}/{total_servers}: {server['name']}")
                
                # Check if we have cached data
                cache_key = self._generate_cache_key(server, "all")
                has_cached = self._load_from_cache(cache_key) is not None
                
                # Test server connection if no cache available
                if not has_cached and not self.test_server_connection(server):
                    logger.error(f"Skipping server {server['name']} due to connection issues")
                    continue
                
                # Process streams for this server
                downloaded, new, existing = self.process_streams_for_server(server)
                
                # Count cache hits vs API calls
                if has_cached:
                    cache_hits += 1
                else:
                    api_calls += 1
                
                if downloaded > 0:
                    successful_servers += 1
                    grand_total_downloaded += downloaded
                    grand_total_new += new
                    grand_total_existing += existing
                    
                    cache_info = "(cached)" if has_cached else "(API call)"
                    logger.info(f"Server {server['name']} completed: {downloaded} total, {new} new, {existing} existing {cache_info}")
                else:
                    logger.warning(f"No VOD streams downloaded from server: {server['name']}")
                
                # Longer delay between servers for VOD (larger datasets)
                if i < total_servers:
                    delay = 1 if has_cached else 5  # Longer delay for API calls due to large responses
                    time.sleep(delay)
            
            # Final summary with cache statistics
            logger.info("="*70)
            logger.info("VOD STREAMS DOWNLOAD SUMMARY")
            logger.info("="*70)
            logger.info(f"Servers processed: {successful_servers}/{total_servers}")
            logger.info(f"Total VOD streams downloaded: {grand_total_downloaded}")
            logger.info(f"New streams added: {grand_total_new}")
            logger.info(f"Existing streams skipped: {grand_total_existing}")
            logger.info(f"Cache performance: {cache_hits} hits, {api_calls} API calls")
            if cache_hits + api_calls > 0:
                cache_hit_rate = (cache_hits / (cache_hits + api_calls)) * 100
                logger.info(f"Cache hit rate: {cache_hit_rate:.1f}%")
            
            return successful_servers > 0
            
        except Exception as e:
            logger.error(f"Unexpected error during VOD streams download: {e}")
            return False
        
        finally:
            self.close_db()
    
    def get_stream_stats(self) -> Dict:
        """Get statistics about VOD streams in database"""
        if not self.connect_db():
            return {}
        
        try:
            cursor = self.conn.cursor()
            
            stats = {}
            
            # Total VOD streams
            cursor.execute("SELECT COUNT(*) as total FROM vod_streams")
            stats['total_streams'] = cursor.fetchone()['total']
            
            # Streams by server
            cursor.execute("""
                SELECT s.name, COUNT(vs.id) as count
                FROM servers s
                LEFT JOIN vod_streams vs ON s.id = vs.server_id
                GROUP BY s.id, s.name
            """)
            server_stats = {}
            for row in cursor.fetchall():
                server_stats[row['name']] = row['count']
            stats['streams_by_server'] = server_stats
            
            # Streams by genre (top 10)
            cursor.execute("""
                SELECT genre, COUNT(*) as count 
                FROM vod_streams 
                WHERE genre != '' AND genre IS NOT NULL
                GROUP BY genre 
                ORDER BY count DESC 
                LIMIT 10
            """)
            genre_stats = {}
            for row in cursor.fetchall():
                genre_stats[row['genre']] = row['count']
            stats['top_genres'] = genre_stats
            
            # Streams with ratings
            cursor.execute("SELECT COUNT(*) as count FROM vod_streams WHERE rating > 0")
            stats['streams_with_ratings'] = cursor.fetchone()['count']
            
            # Streams with release dates
            cursor.execute("SELECT COUNT(*) as count FROM vod_streams WHERE release_date IS NOT NULL")
            stats['streams_with_release_date'] = cursor.fetchone()['count']
            
            # Average rating
            cursor.execute("SELECT AVG(rating) as avg_rating FROM vod_streams WHERE rating > 0")
            avg_rating = cursor.fetchone()['avg_rating']
            stats['average_rating'] = round(avg_rating, 2) if avg_rating else 0
            
            return stats
            
        except sqlite3.Error as e:
            logger.error(f"Error getting stream stats: {e}")
            return {}
        finally:
            self.close_db()


def main():
    """Main function to run the VOD streams downloader with caching"""
    logger.info("Starting Xtream API VOD Streams Download with Caching")
    
    downloader = XtreamVODStreamsDownloader(cache_hours=24)
    
    # Show cache statistics
    logger.info("Cache statistics:")
    cache_stats = downloader.get_cache_stats()
    if cache_stats:
        for key, value in cache_stats.items():
            logger.info(f"  {key}: {value}")
    else:
        logger.info("  No cache data available")
    
    # Show current stream statistics
    logger.info("\nCurrent VOD streams statistics:")
    current_stats = downloader.get_stream_stats()
    if current_stats:
        for key, value in current_stats.items():
            if key in ['streams_by_server', 'top_genres']:
                logger.info(f"{key.replace('_', ' ').title()}:")
                for item, count in value.items():
                    logger.info(f"  {item}: {count}")
            else:
                logger.info(f"  {key}: {value}")
    
    # Download VOD streams (with caching)
    success = downloader.download_all_streams()
    
    if success:
        logger.info("VOD streams download completed successfully!")
        
        # Show updated stats
        logger.info("\nUpdated VOD streams statistics:")
        updated_stats = downloader.get_stream_stats()
        if updated_stats:
            for key, value in updated_stats.items():
                if key in ['streams_by_server', 'top_genres']:
                    logger.info(f"{key.replace('_', ' ').title()}:")
                    for item, count in value.items():
                        logger.info(f"  {item}: {count}")
                else:
                    logger.info(f"  {key}: {value}")
        
        # Show final cache stats
        logger.info("\nFinal cache statistics:")
        final_cache_stats = downloader.get_cache_stats()
        if final_cache_stats:
            for key, value in final_cache_stats.items():
                logger.info(f"  {key}: {value}")
    else:
        logger.error("VOD streams download failed!")
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)