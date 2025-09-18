import logging
from pathlib import Path
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
CACHE_BASE_PATH = Path(__file__).parent.parent / "database" / "cache"

def clear_all_caches() -> bool:
    logger.info(f"Attempting to clear all cache files in: {CACHE_BASE_PATH}")
    
    if not CACHE_BASE_PATH.exists():
        logger.info(f"Cache directory does not exist: {CACHE_BASE_PATH}. Nothing to clear.")
        return True

    success = True
    for item in CACHE_BASE_PATH.iterdir():
        try:
            if item.is_file():
                item.unlink() # Delete file
                logger.debug(f"Deleted file: {item}")
            elif item.is_dir():
                shutil.rmtree(item) # Delete directory and its contents
                logger.debug(f"Deleted directory: {item}")
        except Exception as e:
            logger.error(f"Error deleting {item}: {e}")
            success = False
            
    if success:
        logger.info("All cache files cleared successfully.")
    else:
        logger.error("Failed to clear all cache files.")
        
    return success

def main() -> bool:
    """Main function to run the cache clearing process."""
    return clear_all_caches()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
