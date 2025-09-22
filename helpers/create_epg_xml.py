import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
import logging
import helpers.config_manager as config_manager # New import

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"

def sqlite_to_xmltv_timestamp(sqlite_timestamp: str) -> str:
    """
    Converts SQLite timestamp (YYYY-MM-DD HH:MM:S) to XMLTV timestamp (YYYYMMDDHHMMSS ±HHMM).
    Assumes UTC for simplicity, as timezone info is not in the DB.
    """
    if not sqlite_timestamp:
        return ""
    dt_obj = datetime.strptime(sqlite_timestamp, "%Y-%m-%d %H:%M:%S")
    # Assuming UTC, so timezone offset is +0000
    return dt_obj.strftime("%Y%m%d%H%M%S +0000")

def generate_epg_xml(output_path: Path):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Fetch EPG data for visible channels
        query = """
            SELECT
                ed.channel_id,
                ed.start_time,
                ed.stop_time,
                ed.title,
                ed.description,
                ed.lang,
                ed.category,
                ed.icon,
                ls.name as stream_name,
                ls.stream_icon as stream_logo,
                ls.visible as stream_visible,
                c.visible as category_visible
            FROM
                epg_data ed
            LEFT JOIN
                live_streams ls ON ed.channel_id = ls.epg_channel_id
            LEFT JOIN
                categories c ON ls.category_id = c.category_id
            WHERE
                (ls.visible IS NULL OR ls.visible = 1) AND (c.visible IS NULL OR c.visible = 1);
        """
        cursor.execute(query)
        epg_entries = cursor.fetchall()
        logger.info(f"Fetched {len(epg_entries)} EPG entries for visible channels.")

        # Load directory paths from config_manager
        paths = config_manager.load_directories()
        live_tv_path = paths.get("live_tv")

        if not live_tv_path:
            logger.error("Live TV directory not configured. Cannot generate EPG XML.")
            return False
        
        # Ensure the live_tv_path exists
        Path(live_tv_path).mkdir(parents=True, exist_ok=True)

        # Modify output_path to be inside the live_tv_path
        output_path = Path(live_tv_path) / output_path.name # Modified

        # Create XMLTV root element
        tv_root = ET.Element("tv", {
            "source-info-url": "",
            "source-info-name": "Fynix Player EPG",
            "generator-info-name": "Fynix Player",
            "generator-info-url": ""
        })

        # Collect unique channels
        channels = {}
        for entry in epg_entries:
            channel_id = entry['channel_id']
            if channel_id not in channels:
                channels[channel_id] = {
                    'display-name': entry['stream_name'] if entry['stream_name'] else channel_id,
                    'icon': entry['stream_logo'] if entry['stream_logo'] else ""
                }
        logger.info(f"Identified {len(channels)} unique visible channels.")

        # Add channel elements
        for channel_id, data in channels.items():
            channel_elem = ET.SubElement(tv_root, "channel", {"id": channel_id})
            ET.SubElement(channel_elem, "display-name").text = data['display-name']
            if data['icon']:
                ET.SubElement(channel_elem, "icon", {"src": data['icon']})

        # Add programme elements
        for entry in epg_entries:
            programme_elem = ET.SubElement(tv_root, "programme", {
                "start": sqlite_to_xmltv_timestamp(entry['start_time']),
                "stop": sqlite_to_xmltv_timestamp(entry['stop_time']),
                "channel": entry['channel_id']
            })
            ET.SubElement(programme_elem, "title", {"lang": entry['lang'] or "en"}).text = entry['title']
            if entry['description']:
                ET.SubElement(programme_elem, "desc", {"lang": entry['lang'] or "en"}).text = entry['description']
            if entry['category']:
                ET.SubElement(programme_elem, "category", {"lang": entry['lang'] or "en"}).text = entry['category']
            # Add other optional elements as needed, e.g., <date>, <episode-num>, <audio>, etc.

        # Write to file
        tree = ET.ElementTree(tv_root)
        # Pretty print the XML
        ET.indent(tree, space="  ", level=0)
        tree.write(output_path, encoding="ISO-8859-1", xml_declaration=True)
        logger.info(f"EPG XML generated successfully at {output_path}")
        return True

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    output_file = project_root / "epg.xml"
    generate_epg_xml(output_file)
