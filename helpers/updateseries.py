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
from urllib.parse import urljoin

# Disable SSL warnings for IPTV servers with invalid certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class XtreamSeriesDownloader:
    def __init__(self, db_path: str = None, cache_hours: int = 24):
        if db_path is None:
            script_dir = Path(__file__).parent
            project_root = script_dir.parent
            db_path = project_root / "database" / "media_player.db"
        self.db_path = Path(db_path)
        self.cache_hours = cache_hours
        self.conn = None
        self.session = requests.Session()
        self.session.timeout = 30
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.cache_dir = self.db_path.parent / "cache" / "series"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir}")
        logger.info("SSL verification disabled for IPTV server compatibility")

    def _generate_cache_key(self, server: Dict) -> str:
        key_data = f"{server['url']}_{server['username']}_series"
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

    def connect_db(self) -> bool:
        try:
            if not self.db_path.exists():
                logger.error(f"Database not found: {self.db_path}")
                return False
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return False

    def close_db(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

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

    def build_api_url(self, server: Dict) -> str:
        base_url = server['url']
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"
        port = server.get('port', 80)
        if port and port != 80:
            if ':' not in base_url.split('://', 1)[1]:
                base_url = f"{base_url}:{port}"
        api_url = urljoin(base_url, '/player_api.php')
        params = f"username={server['username']}&password={server['password']}&action=get_series"
        return f"{api_url}?{params}"

    def test_server_connection(self, server: Dict) -> bool:
        try:
            url = self.build_api_url(server)
            logger.info(f"Testing server: {server['name']}")
            logger.debug(f"Server request URL: {url}")
            response = self.session.get(url, timeout=20, verify=False)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                logger.info(f"Server {server['name']} is accessible and returned {len(data)} series entries")
                return True
            else:
                logger.warning(f"Server {server['name']} returned empty or invalid series list")
                return False
        except Exception as e:
            logger.error(f"Error testing server {server['name']}: {e}")
            return False

    def get_category_mapping(self, server_id: int) -> Dict[int, int]:
        """
        Returns a mapping of Xtream category_id -> local category_id in the database
        """
        mapping = {}
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, xtream_id FROM categories WHERE server_id = ?", (server_id,))
            rows = cursor.fetchall()
            mapping = {row['xtream_id']: row['id'] for row in rows if row['xtream_id'] is not None}
        except sqlite3.Error as e:
            logger.error(f"Error fetching category mapping for server {server_id}: {e}")
        return mapping

    def series_exists(self, server_id: int, series_id: int) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM series WHERE server_id = ? AND series_id = ?", (server_id, series_id))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking series existence: {e}")
            return False

    def insert_series(self, server_id: int, series_data: Dict, category_mapping: Dict[int, int]) -> bool:
        try:
            cursor = self.conn.cursor()
            series_id = series_data.get('series_id')
            name = series_data.get('name', 'Unknown')
            cover = series_data.get('cover', '')
            plot = series_data.get('plot', '')
            cast = series_data.get('cast', '')
            director = series_data.get('director', '')
            genre = series_data.get('genre', '')
            rating = float(series_data.get('rating', 0.0)) if series_data.get('rating') else 0.0
            release_date = series_data.get('releaseDate', '')
            last_modified = series_data.get('last_modified', '')
            xtream_category_id = series_data.get('category_id', None)
            category_id = category_mapping.get(xtream_category_id) if xtream_category_id else None

            cursor.execute("""
                INSERT INTO series (
                    server_id, series_id, name, cover, plot, cast, director, genre, rating, release_date, last_modified, category_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                server_id, series_id, name, cover, plot, cast, director, genre, rating, release_date, last_modified, category_id
            ))
            return True
        except sqlite3.Error as e:
            logger.error(f"Error inserting series {series_data.get('name', 'Unknown')}: {e}")
            return False

    def process_series_for_server(self, server: Dict) -> Tuple[int, int, int]:
        total_downloaded = 0
        total_new = 0
        total_existing = 0
        try:
            logger.info(f"Processing series for server: {server['name']}")
            cache_key = self._generate_cache_key(server)
            series_list = self._load_from_cache(cache_key)
            category_mapping = self.get_category_mapping(server['id'])  # <-- fix here

            if series_list is not None:
                logger.info(f"Using cached series for {server['name']} ({len(series_list)} items)")
            else:
                if not self.test_server_connection(server):
                    logger.error(f"Skipping server {server['name']} due to connection issues")
                    return 0, 0, 0
                url = self.build_api_url(server)
                logger.info(f"Fetching series from {server['name']} (cache miss)")
                try:
                    response = self.session.get(url, timeout=90, verify=False)
                    response.raise_for_status()
                    series_list = response.json()
                    if isinstance(series_list, list):
                        logger.info(f"Downloaded {len(series_list)} series from {server['name']}")
                        self._save_to_cache(cache_key, series_list)
                    else:
                        logger.warning(f"Unexpected response format for series from {server['name']}")
                        return 0, 0, 0
                except Exception as e:
                    logger.error(f"Error downloading series from {server['name']}: {e}")
                    return 0, 0, 0

            if not series_list:
                logger.warning(f"No series found for server: {server['name']}")
                return 0, 0, 0

            downloaded_count = len(series_list)
            new_count = 0
            existing_count = 0
            batch_size = 1000
            total_batches = (downloaded_count + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, downloaded_count)
                batch_series = series_list[start_idx:end_idx]
                logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_series)} series)")
                batch_new = 0
                batch_existing = 0

                for series_data in batch_series:
                    if not isinstance(series_data, dict):
                        logger.warning(f"Invalid series data format: {series_data}")
                        continue
                    series_id = series_data.get('series_id')
                    if series_id is None:
                        logger.warning(f"Series missing series_id: {series_data}")
                        continue
                    if self.series_exists(server['id'], series_id):
                        batch_existing += 1
                        continue
                    if self.insert_series(server['id'], series_data, category_mapping):
                        batch_new += 1

                self.conn.commit()
                new_count += batch_new
                existing_count += batch_existing
                logger.info(f"Batch {batch_num + 1} completed: {batch_new} new, {batch_existing} existing")
                if batch_num < total_batches - 1:
                    time.sleep(0.1)

            logger.info(f"Server {server['name']} - series: {downloaded_count} downloaded, {new_count} new, {existing_count} existing")
            total_downloaded = downloaded_count
            total_new = new_count
            total_existing = existing_count

        except Exception as e:
            logger.error(f"Error processing series for server {server['name']}: {e}")
            self.conn.rollback()

        return total_downloaded, total_new, total_existing

    def download_all_series(self) -> bool:
        if not self.connect_db():
            return False
        try:
            servers = self.get_servers()
            if not servers:
                logger.warning("No active servers found in database")
                return False
            total_servers = len(servers)
            successful_servers = 0
            grand_total_downloaded = 0
            grand_total_new = 0
            grand_total_existing = 0
            logger.info(f"Starting series download for {total_servers} servers")

            for i, server in enumerate(servers, 1):
                logger.info(f"Processing server {i}/{total_servers}: {server['name']}")
                downloaded, new, existing = self.process_series_for_server(server)
                if downloaded > 0:
                    successful_servers += 1
                    grand_total_downloaded += downloaded
                    grand_total_new += new
                    grand_total_existing += existing
                else:
                    logger.warning(f"No series downloaded from server: {server['name']}")
                if i < total_servers:
                    time.sleep(1)

            logger.info("="*70)
            logger.info("SERIES DOWNLOAD SUMMARY")
            logger.info("="*70)
            logger.info(f"Servers processed: {successful_servers}/{total_servers}")
            logger.info(f"Total series downloaded: {grand_total_downloaded}")
            logger.info(f"New series added: {grand_total_new}")
            logger.info(f"Existing series skipped: {grand_total_existing}")
            return successful_servers > 0
        except Exception as e:
            logger.error(f"Unexpected error during series download: {e}")
            return False
        finally:
            self.close_db()


def main():
    logger.info("Starting Xtream API Series Download with Caching")
    downloader = XtreamSeriesDownloader(cache_hours=24)
    success = downloader.download_all_series()
    if success:
        logger.info("Series download completed successfully!")
    else:
        logger.error("Series download failed!")
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
