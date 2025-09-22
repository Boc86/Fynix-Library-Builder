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

class XtreamLiveStreamsDownloader:
    def __init__(self, db_path: str = None, cache_hours: int = 24):
        """Initialize the live streams downloader with caching support"""
        if db_path is None:
            script_dir = Path(__file__).parent
            project_root = script_dir.parent
            db_path = project_root / "database" / "media_player.db"

        self.db_path = Path(db_path)
        self.cache_hours = cache_hours
        self.conn = None

        # Requests session
        self.session = requests.Session()
        self.session.timeout = 30
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        # Cache directory
        self.cache_dir = self.db_path.parent / "cache" / "live_streams"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir}")
        logger.info("SSL verification disabled for IPTV server compatibility")

    def _generate_cache_key(self, server: Dict, category_id: str = "all") -> str:
        key_data = f"{server['url']}_{server['username']}_live_streams_{category_id}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def _get_cache_file_path(self, cache_key: str) -> Path:
        return self.cache_dir / f"{cache_key}.pkl"

    def _is_cache_valid(self, cache_file: Path) -> bool:
        if not cache_file.exists():
            return False
        file_modified = datetime.fromtimestamp(cache_file.stat().st_mtime)
        expiry_time = datetime.now() - timedelta(hours=self.cache_hours)
        return file_modified > expiry_time

    def _save_to_cache(self, cache_key: str, data: List[Dict]) -> bool:
        try:
            cache_file = self._get_cache_file_path(cache_key)
            cache_data = {
                'timestamp': datetime.utcnow(),
                'data': data
            }
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            return True
        except Exception as e:
            logger.error(f"Error saving to cache {cache_key}: {e}")
            return False

    def _load_from_cache(self, cache_key: str) -> Optional[List[Dict]]:
        try:
            cache_file = self._get_cache_file_path(cache_key)
            if not self._is_cache_valid(cache_file):
                return None
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            return cache_data.get('data', [])
        except Exception as e:
            logger.warning(f"Error loading from cache {cache_key}: {e}")
            return None

    def _clear_expired_cache(self):
        try:
            for cache_file in self.cache_dir.glob("*.pkl"):
                if not self._is_cache_valid(cache_file):
                    cache_file.unlink()
        except Exception as e:
            logger.warning(f"Error clearing expired cache: {e}")

    def connect_db(self) -> bool:
        try:
            if not self.db_path.exists():
                logger.error(f"Database not found: {self.db_path}")
                return False
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return False

    def close_db(self):
        if self.conn:
            self.conn.close()

    def get_servers(self) -> List[Dict]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id, name, url, username, password, port 
                FROM servers 
                WHERE status = 'active'
            """)
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error fetching servers: {e}")
            return []

    def build_api_url(self, server: Dict, action: str = "", category_id: str = "") -> str:
        base_url = server['url']
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"
        port = server.get('port', 80)
        if port not in [80, 443] and ':' not in base_url.split('://', 1)[1]:
            base_url = f"{base_url}:{port}"
        base_url = base_url.rstrip('/')
        api_url = f"{base_url}/player_api.php"
        params = f"username={server['username']}&password={server['password']}"
        if action:
            params += f"&action={action}"
        if category_id:
            params += f"&category_id={category_id}"
        return f"{api_url}?{params}"

    def test_server_connection(self, server: Dict) -> bool:
        try:
            url = self.build_api_url(server)
            response = self.session.get(url, timeout=20, verify=False)
            response.raise_for_status()
            data = response.json()
            return isinstance(data, dict) and ('user_info' in data or 'server_info' in data)
        except Exception as e:
            logger.error(f"Error testing server {server['name']}: {e}")
            return False

    def download_live_streams(self, server: Dict, category_id: str = "") -> List[Dict]:
        cache_key = self._generate_cache_key(server, category_id or "all")
        cached_data = self._load_from_cache(cache_key)
        if cached_data is not None:
            return cached_data
        try:
            url = self.build_api_url(server, "get_live_streams", category_id)
            response = self.session.get(url, timeout=60, verify=False)
            response.raise_for_status()
            streams = response.json()
            if isinstance(streams, list):
                self._save_to_cache(cache_key, streams)
                return streams
            return []
        except Exception as e:
            logger.error(f"Error downloading live streams from {server['name']}: {e}")
            return []

    def get_category_mapping(self, server_id: int) -> Dict[str, str]:
        """Return mapping of API category_id -> database categories.category_id"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT category_id 
                FROM categories 
                WHERE server_id = ? AND content_type = 'live'
            """, (server_id,))
            mapping = {}
            for row in cursor.fetchall():
                cat_id = str(row['category_id'])
                mapping[cat_id] = cat_id
            return mapping
        except sqlite3.Error as e:
            logger.error(f"Error getting category mapping: {e}")
            return {}

    def stream_exists(self, server_id: int, stream_id: int) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM live_streams WHERE server_id = ? AND stream_id = ?", (server_id, stream_id))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking stream existence: {e}")
            return False

    def insert_stream(self, server_id: int, stream_data: Dict, category_mapping: Dict) -> bool:
        try:
            cursor = self.conn.cursor()
            stream_id = stream_data.get('stream_id')
            if stream_id is None:
                return False

            name = stream_data.get('name', 'Unknown')
            stream_type = stream_data.get('stream_type', 'live')
            stream_icon = stream_data.get('stream_icon', '')
            epg_channel_id = stream_data.get('epg_channel_id', '')
            tv_archive = stream_data.get('tv_archive', 0)
            direct_source = stream_data.get('direct_source', '')
            tv_archive_duration = stream_data.get('tv_archive_duration', 0)

            category_id_str = str(stream_data.get('category_id', ''))
            db_category_id = category_mapping.get(category_id_str)

            if not db_category_id:
                # fallback to first live category or '0'
                cursor.execute("""
                    SELECT category_id FROM categories
                    WHERE server_id = ? AND content_type = 'live'
                    LIMIT 1
                """, (server_id,))
                row = cursor.fetchone()
                db_category_id = row['category_id'] if row else '0'

            cursor.execute("""
                INSERT INTO live_streams (
                    server_id, category_id, stream_id, name, stream_type,
                    stream_icon, epg_channel_id, tv_archive, direct_source, tv_archive_duration
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                server_id, db_category_id, stream_id, name, stream_type,
                stream_icon, epg_channel_id, tv_archive, direct_source, tv_archive_duration
            ))

            return True
        except sqlite3.Error as e:
            logger.error(f"Error inserting stream {stream_data.get('name', 'Unknown')}: {e}")
            return False

    def process_streams_for_server(self, server: Dict) -> Tuple[int, int, int]:
        total_downloaded, total_new, total_existing = 0, 0, 0
        try:
            category_mapping = self.get_category_mapping(server['id'])
            streams = self.download_live_streams(server)
            if not streams:
                return 0, 0, 0

            for stream_data in streams:
                if not isinstance(stream_data, dict):
                    continue
                stream_id = stream_data.get('stream_id')
                if stream_id is None:
                    continue
                if self.stream_exists(server['id'], stream_id):
                    total_existing += 1
                    continue
                if self.insert_stream(server['id'], stream_data, category_mapping):
                    total_new += 1
            self.conn.commit()
            total_downloaded = len(streams)
        except Exception as e:
            logger.error(f"Error processing streams for server {server['name']}: {e}")
            self.conn.rollback()
        return total_downloaded, total_new, total_existing

    def download_all_streams(self) -> bool:
        if not self.connect_db():
            return False
        try:
            self._clear_expired_cache()
            servers = self.get_servers()
            if not servers:
                return False
            for server in servers:
                self.process_streams_for_server(server)
            return True
        finally:
            self.close_db()


def main():
    downloader = XtreamLiveStreamsDownloader(cache_hours=24)
    downloader.download_all_streams()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
