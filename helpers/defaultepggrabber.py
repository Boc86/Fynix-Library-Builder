import sqlite3
import requests
import logging
from pathlib import Path
import xml.etree.ElementTree as ET
from requests.exceptions import RequestException
from urllib.parse import urljoin
import urllib3
import hashlib
import pickle
from datetime import datetime, timedelta
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"

# Disable SSL warnings for IPTV servers with invalid certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CACHE_HOURS = 24
CACHE_DIR = Path(DB_PATH).parent / "cache" / "epg_xml"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def build_epg_url(server: dict) -> str:
    base_url = server['url']
    if not base_url.startswith(('http://', 'https://')):
        base_url = f"http://{base_url}"
    port = server.get('port', 80)
    if port and port != 80:
        if ':' not in base_url.split('://', 1)[1]:
            base_url = f"{base_url}:{port}"
    api_url = urljoin(base_url, '/xmltv.php')
    params = f"username={server['username']}&password={server['password']}"
    return f"{api_url}?{params}"


def test_server_connection(url):
    logger.info(f"Testing EPG URL: {url}")
    try:
        resp = requests.head(url, timeout=30, verify=False)
        resp.raise_for_status()
        logger.info("Server is reachable and responded to HEAD request.")
        return True
    except RequestException as e:
        logger.error(f"Server connection test failed: {e}")
        return False


def fetch_epg_xml(url):
    try:
        resp = requests.get(url, timeout=60, verify=False)
        resp.raise_for_status()
        return resp.content
    except RequestException as e:
        logger.error(f"Failed to download EPG XML: {e}")
        return None


def xmltv_to_sqlite_timestamp(xmltv_str):
    match = re.match(r"(\d{14}) ?([+-]\d{4})?", xmltv_str)
    if not match:
        return xmltv_str
    dt_str, _ = match.groups()
    dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_epg_xml(xml_data):
    epg_entries = []
    try:
        tree = ET.fromstring(xml_data)
        for prog in tree.findall('programme'):
            channel_id = prog.get('channel')
            start_time = xmltv_to_sqlite_timestamp(prog.get('start'))
            stop_time = xmltv_to_sqlite_timestamp(prog.get('stop'))
            title = prog.findtext('title')
            desc = prog.findtext('desc')
            lang = prog.findtext('title', default='en')
            category = prog.findtext('category')
            icon_elem = prog.find('icon')
            icon = icon_elem.get('src') if icon_elem is not None else None
            epg_entries.append({
                'channel_id': channel_id,
                'start_time': start_time,
                'stop_time': stop_time,
                'title': title,
                'description': desc,
                'lang': lang,
                'category': category,
                'icon': icon
            })
        logger.info(f"Parsed {len(epg_entries)} EPG entries from XML.")
    except Exception as e:
        logger.error(f"Error parsing EPG XML: {e}")
    return epg_entries


def insert_epg_entries(conn, epg_entries):
    cursor = conn.cursor()
    # Clear existing EPG data first
    cursor.execute("DELETE FROM epg_data")
    conn.commit()
    logger.info("Cleared old EPG data.")

    for entry in epg_entries:
        cursor.execute("""
            INSERT OR IGNORE INTO epg_data
            (channel_id, start_time, stop_time, title, description, lang, category, icon)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry['channel_id'], entry['start_time'], entry['stop_time'], entry['title'],
            entry['description'], entry['lang'], entry['category'], entry['icon']
        ))
    conn.commit()
    logger.info(f"Inserted {len(epg_entries)} new EPG entries.")


def _generate_cache_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def _get_cache_file_path(cache_key: str) -> Path:
    return CACHE_DIR / f"{cache_key}.pkl"


def _is_cache_valid(cache_file: Path) -> bool:
    if not cache_file.exists():
        return False
    file_modified = datetime.fromtimestamp(cache_file.stat().st_mtime)
    expiry_time = datetime.now() - timedelta(hours=CACHE_HOURS)
    return file_modified > expiry_time


def _save_to_cache(cache_key: str, xml_data: bytes) -> bool:
    try:
        cache_file = _get_cache_file_path(cache_key)
        cache_data = {'timestamp': datetime.now(), 'xml': xml_data}
        with open(cache_file, 'wb') as f:
            pickle.dump(cache_data, f)
        return True
    except Exception as e:
        logger.error(f"Error saving EPG XML to cache {cache_key}: {e}")
        return False


def _load_from_cache(cache_key: str) -> bytes:
    try:
        cache_file = _get_cache_file_path(cache_key)
        if not _is_cache_valid(cache_file):
            return None
        with open(cache_file, 'rb') as f:
            cache_data = pickle.load(f)
        return cache_data.get('xml', None)
    except Exception as e:
        logger.warning(f"Error loading EPG XML from cache {cache_key}: {e}")
        return None


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT url, username, password, port FROM servers WHERE status='active' LIMIT 1")
    row = cursor.fetchone()
    if not row:
        logger.error("No active server found in database.")
        return

    server = dict(row)
    url = build_epg_url(server)
    logger.info(f"Downloading EPG from: {url}")

    if not test_server_connection(url):
        logger.error("Server not reachable. Aborting EPG download.")
        return

    cache_key = _generate_cache_key(url)
    xml_data = _load_from_cache(cache_key)
    if xml_data:
        logger.info(f"Using cached EPG XML for URL: {url}")
    else:
        xml_data = fetch_epg_xml(url)
        if not xml_data:
            logger.error("No EPG XML downloaded.")
            return
        _save_to_cache(cache_key, xml_data)
        logger.info(f"Downloaded and cached EPG XML for URL: {url}")

    epg_entries = parse_epg_xml(xml_data)
    if not epg_entries:
        logger.error("No EPG entries parsed.")
        return

    insert_epg_entries(conn, epg_entries)
    conn.close()
    logger.info("EPG import completed.")


if __name__ == "__main__":
    main()
