import logging
from pathlib import Path
import time
import pickle

logger = logging.getLogger(__name__)

# Define cache directories and their expiry times (in seconds)
# These should ideally be exposed by the respective helper modules or centralized
# For now, I'll hardcode them based on my understanding of the other files.
CACHE_CONFIGS = {
    "categories": {
        "dir": Path(__file__).parent.parent / "database" / "cache" / "categories",
        "expiry_seconds": 24 * 60 * 60 # 24 hours
    },
    "movie_metadata": {
        "dir": Path(__file__).parent.parent / "database" / "cache" / "movie_metadata",
        "expiry_seconds": 24 * 60 * 60 # 24 hours
    },
    "series_metadata": {
        "dir": Path(__file__).parent.parent / "database" / "cache" / "series_metadata",
        "expiry_seconds": 24 * 60 * 60 # 24 hours
    },
    "vod_streams": { # From updatemovies.py
        "dir": Path(__file__).parent.parent / "database" / "cache" / "vod_streams",
        "expiry_seconds": 24 * 60 * 60 # 24 hours
    },
    "series_data": { # From updateseries.py
        "dir": Path(__file__).parent.parent / "database" / "cache" / "series",
        "expiry_seconds": 24 * 60 * 60 # 24 hours
    }
}

def is_cache_expired(cache_name: str) -> bool:
    """Checks if the cache for a given name is expired."""
    config = CACHE_CONFIGS.get(cache_name)
    if not config:
        logger.warning(f"Cache configuration for '{cache_name}' not found.")
        return True # Default to expired if config is missing

    cache_dir = config["dir"]
    expiry_seconds = config["expiry_seconds"]

    if not cache_dir.exists():
        logger.debug(f"Cache directory '{cache_dir}' does not exist. Cache is expired.")
        return True # If cache dir doesn't exist, consider cache expired

    # Check if any file in the directory is expired
    # Or if the directory is empty (implies no valid cache)
    has_files = False
    for cache_file in cache_dir.glob("*.pkl"): # Assuming all caches are .pkl files
        has_files = True
        if time.time() - cache_file.stat().st_mtime > expiry_seconds:
            logger.debug(f"Cache file '{cache_file.name}' in '{cache_name}' is expired.")
            return True # Found an expired file

    if not has_files:
        logger.debug(f"Cache directory '{cache_dir}' is empty. Cache is expired.")
        return True # If directory is empty, cache is expired

    logger.debug(f"Cache for '{cache_name}' is valid.")
    return False # No expired files found, and directory is not empty

def any_main_cache_expired() -> bool:
    """Checks if any of the main data caches are expired."""
    logger.info("Checking if any main data cache is expired...")
    for cache_name in CACHE_CONFIGS.keys():
        if is_cache_expired(cache_name):
            logger.info(f"Cache for '{cache_name}' is expired. A full update is needed.")
            return True
    logger.info("All main data caches are valid. A light update is sufficient.")
    return False
