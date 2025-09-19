import sqlite3
import logging
from pathlib import Path
import os
import re
from xml.etree import ElementTree as ET
from xml.dom import minidom # For pretty printing XML
import json # For parsing video/audio JSON

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"

def _sanitize_name(name: str) -> str:
    """Sanitizes a string for use as a filename, removing common prefixes, years, and cleaning up."""
    # Remove prefix up to and including the first ' - '
    if ' - ' in name:
        name = name.split(' - ', 1)[1]

    # Remove year and other tags in parentheses
    name = re.sub(r'\s*\([^)]*\)', '', name)

    # Remove common quality/resolution prefixes and other tags in brackets
    name = re.sub(r'^(?:4K-D\.-|4K\.-|HD\.-|FHD\.-|SD\.-|4K\s*-\s*|HD\s*-\s*|FHD\s*-\s*|SD\s*-\s*|4K\s*|HD\s*|FHD\s*|SD\s*|\[.*?\])\s*', '', name, flags=re.IGNORECASE).strip()

    # Replace dots with spaces (assuming dots are separators, not part of the title itself) and handle multiple spaces
    sanitized = re.sub(r'\.+', ' ', name) # Replace one or more dots with a single space

    # Remove invalid filename characters (keep alphanumeric, underscore, hyphen, space)
    sanitized = re.sub(r'[^a-zA-Z0-9_\- ]', '', sanitized)

    # Reduce multiple spaces to a single space
    sanitized = re.sub(r'\s+', ' ', sanitized).strip()

    return sanitized

def _extract_year(release_date: str, name: str) -> str:
    """Extracts year from release_date or name."""
    year = ""
    
    # Try from release_date (YYYY-MM-DD)
    if release_date and len(release_date) >= 4:
        year = release_date[:4]
        if year.isdigit():
            return year
            
    # Try from name (e.g., 'Movie Title (2023)')
    match = re.search(r'\((\d{4})\)', name)
    if match:
        return match.group(1)
        
    return ""

def _pretty_print_xml(elem: ET.Element) -> str:
    """Returns a pretty-printed XML string for the ElementTree element."""
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="    ")

def _create_tvshow_nfo_xml(series_data: dict) -> str:
    """Generates the XML content for a tvshow.nfo file from series data."""
    tvshow = ET.Element("tvshow")

    # Helper to add text element if value exists
    def add_text_element(parent, tag, value):
        if value is not None and str(value).strip() != '':
            ET.SubElement(parent, tag).text = str(value)

    # Title and Original Title
    title = series_data['name'] if 'name' in series_data else ''
    sanitized_title = _sanitize_name(title)
    add_text_element(tvshow, "title", sanitized_title)
    add_text_element(tvshow, "originaltitle", sanitized_title)
    add_text_element(tvshow, "showtitle", sanitized_title)

    # Plot/Outline
    plot = series_data['plot'] if 'plot' in series_data else ''
    add_text_element(tvshow, "outline", plot)
    add_text_element(tvshow, "plot", plot)

    # Runtime (assuming episode_run_time is in minutes or can be converted)
    episode_run_time = series_data['episode_run_time'] if 'episode_run_time' in series_data else None
    if episode_run_time is not None:
        try:
            add_text_element(tvshow, "runtime", str(int(episode_run_time))) # Assuming it's already in minutes or can be cast
        except (ValueError, TypeError):
            logger.warning(f"Invalid episode_run_time for series {series_data.get('series_id', '')}: {episode_run_time}")

    # Year and Premiered
    release_date = series_data['release_date'] if 'release_date' in series_data else ''
    year = _extract_year(release_date, series_data['name'] if 'name' in series_data else '')
    add_text_element(tvshow, "premiered", release_date)
    add_text_element(tvshow, "year", year)

    # Ratings
    rating = series_data['rating'] if 'rating' in series_data else None
    rating_5based = series_data['rating_5based'] if 'rating_5based' in series_data else None
    tmdb_id = series_data['tmdb_id'] if 'tmdb_id' in series_data else None

    ratings_elem = ET.SubElement(tvshow, "ratings")
    if rating is not None:
        rating_elem = ET.SubElement(ratings_elem, "rating", name="generic", max="10", default="true")
        try:
            ET.SubElement(rating_elem, "value").text = f"{float(rating):.6f}"
        except (ValueError, TypeError):
            logger.warning(f"Invalid rating value for series {series_data.get('series_id', '')}: {rating}")

    if rating_5based is not None:
        rating_elem = ET.SubElement(ratings_elem, "rating", name="5based", max="10")
        try:
            ET.SubElement(rating_elem, "value").text = f"{float(rating_5based):.6f}"
        except (ValueError, TypeError):
            logger.warning(f"Invalid rating_5based value for series {series_data.get('series_id', '')}: {rating_5based}")

    # Unique IDs
    if tmdb_id:
        add_text_element(tvshow, "id", str(tmdb_id))
        ET.SubElement(tvshow, "uniqueid", type="tmdb", default="true").text = str(tmdb_id)

    # Genre
    genre_str = series_data['genre'] if 'genre' in series_data else ''
    if genre_str:
        for g in [g.strip() for g in genre_str.split(',') if g.strip()]:
            add_text_element(tvshow, "genre", g)

    # Director
    director_str = series_data['director'] if 'director' in series_data else ''
    if director_str:
        for d in [d.strip() for d in director_str.split(',') if d.strip()]:
            add_text_element(tvshow, "director", d)

    # Actors
    cast_str = series_data['cast'] if 'cast' in series_data else ''
    if cast_str:
        for actor_name in [a.strip() for a in cast_str.split(',') if a.strip()]:
            actor_elem = ET.SubElement(tvshow, "actor")
            add_text_element(actor_elem, "name", actor_name)

    # Thumbs (images)
    thumb_elements = []
    if 'cover' in series_data and series_data['cover']:
        thumb_elements.append(('cover', series_data['cover'], 'poster'))
    if 'backdrop_path' in series_data and series_data['backdrop_path']:
        # Fanart is a special case, it has a nested thumb
        fanart_elem = ET.SubElement(tvshow, "fanart")
        ET.SubElement(fanart_elem, "thumb", preview=series_data['backdrop_path']).text = series_data['backdrop_path']
    if 'clearlogo' in series_data and series_data['clearlogo']:
        thumb_elements.append(('clearlogo', series_data['clearlogo'], 'clearlogo'))

    for _, url, aspect in thumb_elements:
        ET.SubElement(tvshow, "thumb", aspect=aspect).text = url

    # MPAA
    mpaa = series_data['age'] if 'age' in series_data else ''
    if mpaa:
        add_text_element(tvshow, "mpaa", mpaa)

    # Trailer
    trailer = series_data['youtube_trailer'] if 'youtube_trailer' in series_data else ''
    if trailer:
        add_text_element(tvshow, "trailer", trailer)

    # Status
    status = series_data['status'] if 'status' in series_data else ''
    if status:
        add_text_element(tvshow, "status", status)

    # Other fields (empty or placeholders for now)
    add_text_element(tvshow, "userrating", "0") # Placeholder
    add_text_element(tvshow, "top250", "0")
    add_text_element(tvshow, "playcount", "0")
    add_text_element(tvshow, "lastplayed", "")
    add_text_element(tvshow, "code", "")
    add_text_element(tvshow, "aired", "")
    add_text_element(tvshow, "studio", "")

    # Convert to pretty-printed string
    return _pretty_print_xml(tvshow)

def _create_episodedetails_nfo_xml(episode_data: dict, series_name: str) -> str:
    """Generates the XML content for an episodedetails.nfo file from episode data."""
    episodedetails = ET.Element("episodedetails")

    # Helper to add text element if value exists
    def add_text_element(parent, tag, value):
        if value is not None and str(value).strip() != '':
            ET.SubElement(parent, tag).text = str(value)

    # Title and Showtitle
    title = episode_data['title'] if 'title' in episode_data else ''
    sanitized_series_name = _sanitize_name(series_name)
    add_text_element(episodedetails, "title", title)
    add_text_element(episodedetails, "showtitle", sanitized_series_name)

    # Plot/Outline
    plot = episode_data['plot'] if 'plot' in episode_data else ''
    add_text_element(episodedetails, "outline", plot)
    add_text_element(episodedetails, "plot", plot)

    # Season and Episode numbers
    season_num = episode_data['season_num'] if 'season_num' in episode_data else None
    episode_num = episode_data['episode_num'] if 'episode_num' in episode_data else None
    if season_num is not None:
        add_text_element(episodedetails, "season", str(season_num))
    if episode_num is not None:
        add_text_element(episodedetails, "episode", str(episode_num))

    # Runtime (assuming duration_secs is in seconds, convert to minutes)
    duration_secs = episode_data['duration_secs'] if 'duration_secs' in episode_data else None
    if duration_secs is not None:
        try:
            add_text_element(episodedetails, "runtime", str(int(duration_secs / 60))) # Convert to minutes
        except (ValueError, TypeError):
            logger.warning(f"Invalid duration_secs for episode {episode_data.get('episode_id', '')}: {duration_secs}")

    # Year and Premiered/Aired
    airdate = episode_data['airdate'] if 'airdate' in episode_data else ''
    add_text_element(episodedetails, "aired", airdate)
    add_text_element(episodedetails, "premiered", airdate) # Often same as aired for episodes
    
    # Extract year from airdate for <year> tag
    year = _extract_year(airdate, '') # Pass empty string for name as it's not relevant here
    add_text_element(episodedetails, "year", year)

    # Ratings
    rating = episode_data['rating'] if 'rating' in episode_data else None
    tmdb_id = episode_data['tmdb_id'] if 'tmdb_id' in episode_data else None

    ratings_elem = ET.SubElement(episodedetails, "ratings")
    if rating is not None:
        rating_elem = ET.SubElement(ratings_elem, "rating", name="generic", max="10", default="true")
        try:
            ET.SubElement(rating_elem, "value").text = f"{float(rating):.6f}"
        except (ValueError, TypeError):
            logger.warning(f"Invalid rating value for episode {episode_data.get('episode_id', '')}: {rating}")

    # Unique IDs
    if tmdb_id:
        add_text_element(episodedetails, "id", str(tmdb_id))
        ET.SubElement(episodedetails, "uniqueid", type="tmdb", default="true").text = str(tmdb_id)

    # Genre (not typically in episode NFOs, but if needed)
    # genre_str = episode_data['genre'] if 'genre' in episode_data else ''
    # if genre_str:
    #     for g in [g.strip() for g in genre_str.split(',') if g.strip()]:
    #         add_text_element(episodedetails, "genre", g)

    # Director/Crew
    crew_str = episode_data['crew'] if 'crew' in episode_data else ''
    if crew_str:
        for crew_member in [c.strip() for c in crew_str.split(',') if c.strip()]:
            add_text_element(episodedetails, "director", crew_member) # Assuming crew is director
            add_text_element(episodedetails, "credits", crew_member) # Assuming crew is credits

    # Thumbs (images)
    if 'movie_image' in episode_data and episode_data['movie_image']:
        ET.SubElement(episodedetails, "thumb", aspect="thumb").text = episode_data['movie_image']

    # Other fields (empty or placeholders)
    add_text_element(episodedetails, "userrating", "0")
    add_text_element(episodedetails, "top250", "0")
    add_text_element(episodedetails, "playcount", "0")
    add_text_element(episodedetails, "lastplayed", "")
    add_text_element(episodedetails, "displayseason", "-1")
    add_text_element(episodedetails, "displayepisode", "-1")
    add_text_element(episodedetails, "tagline", "")
    add_text_element(episodedetails, "studio", "")
    add_text_element(episodedetails, "trailer", "")
    add_text_element(episodedetails, "dateadded", episode_data['added'] if 'added' in episode_data else '')

    # Fileinfo (complex, often not fully populated from basic API data)
    fileinfo_elem = ET.SubElement(episodedetails, "fileinfo")
    streamdetails_elem = ET.SubElement(fileinfo_elem, "streamdetails")

    # Video stream details
    video_data = None
    if 'video' in episode_data and episode_data['video']:
        try:
            video_data = json.loads(episode_data['video'])
        except json.JSONDecodeError:
            logger.warning(f"Could not parse video JSON for episode {episode_data.get('episode_id', '')}")
    
    if video_data:
        video_elem = ET.SubElement(streamdetails_elem, "video")
        add_text_element(video_elem, "codec", video_data.get("codec_name"))
        add_text_element(video_elem, "width", video_data.get("width"))
        add_text_element(video_elem, "height", video_data.get("height"))
        add_text_element(video_elem, "durationinseconds", episode_data['duration_secs'] if 'duration_secs' in episode_data else '')
        # aspect, stereomode, hdrtype not directly available

    # Audio stream details
    audio_data = None
    if 'audio' in episode_data and episode_data['audio']:
        try:
            audio_data = json.loads(episode_data['audio'])
        except json.JSONDecodeError:
            logger.warning(f"Could not parse audio JSON for episode {episode_data.get('episode_id', '')}")

    if audio_data:
        audio_elem = ET.SubElement(streamdetails_elem, "audio")
        add_text_element(audio_elem, "codec", audio_data.get("codec_name"))
        add_text_element(audio_elem, "language", audio_data.get("language"))
        add_text_element(audio_elem, "channels", audio_data.get("channels"))

    # Subtitle stream details (not directly available)
    # ET.SubElement(streamdetails_elem, "subtitle")

    # Convert to pretty-printed string
    return _pretty_print_xml(episodedetails)

def create_single_tvshow_nfo_file(series_data: dict, series_folder_path: Path) -> bool:
    """Creates a single tvshow.nfo file for a given series."""
    nfo_filepath = series_folder_path / "tvshow.nfo"

    logger.debug(f"Attempting to create tvshow.nfo file: {nfo_filepath}")

    # Only create if file doesn't exist
    if nfo_filepath.exists():
        logger.debug(f"Skipping existing tvshow.nfo file: {nfo_filepath}")
        return True # Consider it a success if it already exists

    try:
        nfo_content = _create_tvshow_nfo_xml(series_data)
        with open(nfo_filepath, "w", encoding="utf-8") as f:
            f.write(nfo_content)
        logger.debug(f"Created tvshow.nfo file: {nfo_filepath}")
        return True
    except Exception as e:
        logger.error(f"Error creating tvshow.nfo for {series_data.get('name', 'Unknown')} ({series_data.get('series_id', '')}): {e}")
        return False

def create_single_episode_nfo_file(episode_data: dict, filename_base: str, season_path: Path, series_name: str) -> bool:
    """Creates a single .nfo file for a given episode."""
    nfo_filepath = season_path / f"{filename_base}.nfo"

    logger.debug(f"Attempting to create episode .nfo file: {nfo_filepath}")

    # Only create if file doesn't exist
    if nfo_filepath.exists():
        logger.debug(f"Skipping existing episode .nfo file: {nfo_filepath}")
        return True # Consider it a success if it already exists

    try:
        nfo_content = _create_episodedetails_nfo_xml(episode_data, series_name)
        with open(nfo_filepath, "w", encoding="utf-8") as f:
            f.write(nfo_content)
        logger.debug(f"Created episode .nfo file: {nfo_filepath}")
        return True
    except Exception as e:
        logger.error(f"Error creating episode .nfo for {episode_data.get('title', 'Unknown')} ({episode_data.get('episode_id', '')}): {e}")
        return False

def main() -> bool:
    """Main function to run the series .nfo file creation process."""
    logger.info("This script is primarily designed to be called from create_series_strm_files.py.")
    return True

if __name__ == "__main__":
    # This block is for standalone testing of create_single_tvshow_nfo_file or create_single_episode_nfo_file if needed
    logger.info("This script is primarily designed to be called from create_series_strm_files.py.")
    exit(0)