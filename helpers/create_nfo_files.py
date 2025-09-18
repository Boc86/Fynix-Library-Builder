import sqlite3
import logging
from pathlib import Path
import os
import re
from xml.etree import ElementTree as ET
from xml.dom import minidom # For pretty printing XML

import helpers.config_manager as config_manager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"

def _sanitize_name(name: str) -> str:
    """Sanitizes a string for use as a filename, removing common prefixes, years, and cleaning up."""
    # Remove 'EN - ' prefix
    if name.startswith("EN - "):
        name = name[5:]

    # Remove common quality/resolution prefixes and other tags in brackets/parentheses
    name = re.sub(r'^(?:4K-D\.-|4K\.-|HD\.-|FHD\.-|SD\.-|4K\s*-\s*|HD\s*-\s*|FHD\s*-\s*|SD\s*-\s*|4K\s*|HD\s*|FHD\s*|SD\s*|\[.*?\]|\(.*?\))\s*', '', name, flags=re.IGNORECASE).strip()

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

def _create_nfo_xml(stream_data: dict) -> str:
    """Generates the XML content for an .nfo file from stream data."""
    movie = ET.Element("movie")

    # Helper to add text element if value exists
    def add_text_element(parent, tag, value):
        if value is not None and str(value).strip() != '': # Check for None and empty string
            ET.SubElement(parent, tag).text = str(value)

    # Title and Original Title
    title = stream_data['name'] if 'name' in stream_data else ''
    original_title = stream_data['o_name'] if 'o_name' in stream_data and stream_data['o_name'] else title
    add_text_element(movie, "title", title)
    add_text_element(movie, "originaltitle", original_title)

    # Plot/Outline
    plot = stream_data['plot'] if 'plot' in stream_data and stream_data['plot'] else (stream_data['description'] if 'description' in stream_data else '')
    add_text_element(movie, "outline", plot)
    add_text_element(movie, "plot", plot)

    # Runtime (assuming duration_secs is in seconds, convert to minutes for NFO if needed)
    duration_secs = stream_data['duration_secs'] if 'duration_secs' in stream_data else None
    if duration_secs is not None:
        try:
            add_text_element(movie, "runtime", str(int(duration_secs / 60))) # Convert to minutes
        except (ValueError, TypeError):
            logger.warning(f"Invalid duration_secs for stream {stream_data.get('stream_id', '')}: {duration_secs}")

    # Year and Premiered
    release_date = stream_data['release_date'] if 'release_date' in stream_data else ''
    year = _extract_year(release_date, stream_data['name'] if 'name' in stream_data else '')
    add_text_element(movie, "premiered", release_date)
    add_text_element(movie, "year", year)

    # Ratings
    rating = stream_data['rating'] if 'rating' in stream_data else None
    rating_5based = stream_data['rating_5based'] if 'rating_5based' in stream_data else None
    tmdb_id = stream_data['tmdb_id'] if 'tmdb_id' in stream_data else None

    ratings_elem = ET.SubElement(movie, "ratings")
    if rating is not None:
        rating_elem = ET.SubElement(ratings_elem, "rating", name="generic", max="10", default="true")
        try:
            ET.SubElement(rating_elem, "value").text = f"{float(rating):.6f}"
        except (ValueError, TypeError):
            logger.warning(f"Invalid rating value for stream {stream_data.get('stream_id', '')}: {rating}")
        # votes not available

    if rating_5based is not None:
        rating_elem = ET.SubElement(ratings_elem, "rating", name="5based", max="10")
        try:
            ET.SubElement(rating_elem, "value").text = f"{float(rating_5based):.6f}"
        except (ValueError, TypeError):
            logger.warning(f"Invalid rating_5based value for stream {stream_data.get('stream_id', '')}: {rating_5based}")

    # Unique IDs
    if tmdb_id:
        add_text_element(movie, "id", str(tmdb_id))
        ET.SubElement(movie, "uniqueid", type="tmdb", default="true").text = str(tmdb_id)

    # Genre
    genre_str = stream_data['genre'] if 'genre' in stream_data else ''
    if genre_str:
        for g in [g.strip() for g in genre_str.split(',') if g.strip()]:
            add_text_element(movie, "genre", g)

    # Country
    country_str = stream_data['country'] if 'country' in stream_data else ''
    if country_str:
        for c in [c.strip() for c in country_str.split(',') if c.strip()]:
            add_text_element(movie, "country", c)

    # Director
    director_str = stream_data['director'] if 'director' in stream_data else ''
    if director_str:
        for d in [d.strip() for d in director_str.split(',') if d.strip()]:
            add_text_element(movie, "director", d)

    # Actors
    cast_str = stream_data['cast'] if 'cast' in stream_data else (stream_data['actors'] if 'actors' in stream_data else '')
    if cast_str:
        for actor_name in [a.strip() for a in cast_str.split(',') if a.strip()]:
            actor_elem = ET.SubElement(movie, "actor")
            add_text_element(actor_elem, "name", actor_name)
            # role, order, thumb not available

    # Thumbs (images)
    thumb_elements = []
    if 'stream_icon' in stream_data and stream_data['stream_icon']:
        thumb_elements.append(('stream_icon', stream_data['stream_icon'], 'poster'))
    if 'cover_big' in stream_data and stream_data['cover_big']:
        thumb_elements.append(('cover_big', stream_data['cover_big'], 'poster'))
    if 'movie_image' in stream_data and stream_data['movie_image']:
        thumb_elements.append(('movie_image', stream_data['movie_image'], 'thumb'))
    if 'backdrop_path' in stream_data and stream_data['backdrop_path']:
        # Fanart is a special case, it has a nested thumb
        fanart_elem = ET.SubElement(movie, "fanart")
        ET.SubElement(fanart_elem, "thumb", preview=stream_data['backdrop_path']).text = stream_data['backdrop_path']
    if 'clearlogo' in stream_data and stream_data['clearlogo']:
        thumb_elements.append(('clearlogo', stream_data['clearlogo'], 'clearlogo'))

    for _, url, aspect in thumb_elements:
        ET.SubElement(movie, "thumb", aspect=aspect).text = url

    # MPAA
    mpaa = stream_data['age'] if 'age' in stream_data else ''
    if mpaa:
        add_text_element(movie, "mpaa", mpaa)

    # Trailer
    trailer = stream_data['youtube_trailer'] if 'youtube_trailer' in stream_data else ''
    if trailer:
        add_text_element(movie, "trailer", trailer)

    # Other fields (empty or placeholders for now)
    add_text_element(movie, "sorttitle", "")
    add_text_element(movie, "tagline", "")
    add_text_element(movie, "top250", "0")
    add_text_element(movie, "playcount", "0")
    add_text_element(movie, "lastplayed", "")
    add_text_element(movie, "status", "")
    add_text_element(movie, "code", "")
    add_text_element(movie, "aired", "")
    add_text_element(movie, "studio", "")
    add_text_element(movie, "userrating", "0") # Placeholder

    # Convert to pretty-printed string
    return _pretty_print_xml(movie)

def create_single_nfo_file(stream_data: dict, filename_base: str, movies_path: Path) -> bool:
    """Creates a single .nfo file for a given stream."""
    nfo_filepath = movies_path / f"{filename_base}.nfo"

    logger.debug(f"Attempting to create .nfo file: {nfo_filepath}")

    # Only create if file doesn't exist
    if nfo_filepath.exists():
        logger.debug(f"Skipping existing .nfo file: {nfo_filepath}")
        return True # Consider it a success if it already exists

    try:
        nfo_content = _create_nfo_xml(stream_data)
        with open(nfo_filepath, "w", encoding="utf-8") as f:
            f.write(nfo_content)
        logger.debug(f"Created .nfo file: {nfo_filepath}")
        return True
    except Exception as e:
        logger.error(f"Error creating .nfo file for {stream_data.get('name', 'Unknown')} ({stream_data.get('stream_id', '')}): {e}")
        return False

def create_nfo_files() -> bool:
    """This function is now a placeholder or can be removed if not used elsewhere."""
    logger.info("create_nfo_files() called. This function is now a placeholder as NFOs are created with STRMs.")
    return True # Always return True as the actual work is done elsewhere

def main() -> bool:
    """Main function to run the .nfo file creation process."""
    # This main function is now a placeholder as NFOs are created with STRMs.
    return True

if __name__ == "__main__":
    # This block is for standalone testing of create_single_nfo_file if needed
    # For actual use, create_single_nfo_file will be called from create_strm_files.py
    logger.info("This script is primarily designed to be called from create_strm_files.py.")
    # Example usage (requires a dummy stream_data and movies_path)
    # dummy_stream_data = {
    #     'name': 'Test Movie', 'o_name': 'Original Test Movie', 'release_date': '2023-01-01',
    #     'stream_id': 123, 'container_extension': 'mp4', 'plot': 'A test plot.',
    #     'rating': 7.5, 'rating_5based': 3.75, 'tmdb_id': '456',
    #     'genre': 'Action, Adventure', 'country': 'USA', 'director': 'Test Director',
    #     'cast': 'Actor One, Actor Two', 'stream_icon': 'http://example.com/icon.jpg',
    #     'duration_secs': 7200, 'age': 'PG-13', 'youtube_trailer': 'https://youtube.com/test'
    # }
    # dummy_movies_path = Path("./test_movies")
    # dummy_movies_path.mkdir(exist_ok=True)
    # success = create_single_nfo_file(dummy_stream_data, "Test Movie (2023)", dummy_movies_path)
    # print(f"Dummy NFO creation success: {success}")
    exit(0)
