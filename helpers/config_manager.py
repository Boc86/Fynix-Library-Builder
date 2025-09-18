import toml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

CONFIG_FILE = Path("directories.toml")

def save_directories(movie_path: str, series_path: str) -> bool:
    """Saves movie and series directory paths to directories.toml."""
    config_data = {
        "paths": {
            "movies": movie_path,
            "series": series_path
        }
    }
    try:
        with open(CONFIG_FILE, "w") as f:
            toml.dump(config_data, f)
        logger.info(f"Saved directory paths to {CONFIG_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error saving directories to {CONFIG_FILE}: {e}")
        return False

def load_directories() -> dict:
    """Loads movie and series directory paths from directories.toml."""
    if not CONFIG_FILE.exists():
        logger.info(f"{CONFIG_FILE} not found.")
        return {"movies": "", "series": ""}
    try:
        with open(CONFIG_FILE, "r") as f:
            config_data = toml.load(f)
        movie_path = config_data.get("paths", {}).get("movies", "")
        series_path = config_data.get("paths", {}).get("series", "")
        logger.info(f"Loaded directory paths from {CONFIG_FILE}")
        return {"movies": movie_path, "series": series_path}
    except Exception as e:
        logger.error(f"Error loading directories from {CONFIG_FILE}: {e}")
        return {"movies": "", "series": ""}

