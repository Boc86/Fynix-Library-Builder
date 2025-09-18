import sqlite3
import pickle
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"
CACHE_DIR = Path(DB_PATH).parent / "cache" / "series_metadata"

def get_cached_tmdb_ids():
    cached_ids = set()
    if not CACHE_DIR.exists():
        print(f"Cache directory not found: {CACHE_DIR}")
        return cached_ids
    debug_printed = False
    for pkl_file in CACHE_DIR.glob("*.pkl"):
        try:
            with open(pkl_file, "rb") as f:
                cache = pickle.load(f)
                if not debug_printed:
                    print(f"DEBUG: First cache file structure: {type(cache)}")
                    if isinstance(cache, dict):
                        print(f"DEBUG: Keys: {list(cache.keys())}")
                        if 'data' in cache:
                            print(f"DEBUG: Type of cache['data']: {type(cache['data'])}")
                            if isinstance(cache['data'], dict):
                                first_key = next(iter(cache['data']), None)
                                if first_key:
                                    print(f"DEBUG: First key in cache['data']: {first_key}")
                                    print(f"DEBUG: First value in cache['data']: {cache['data'][first_key]}")
                            elif isinstance(cache['data'], list) and cache['data']:
                                print(f"DEBUG: First item in cache['data']: {cache['data'][0]}")
                    elif isinstance(cache, list) and cache:
                        print(f"DEBUG: First item in cache: {cache[0]}")
                    debug_printed = True
                # Handle cache['data'] as dict of series info dicts
                if isinstance(cache, dict) and isinstance(cache.get('data'), dict):
                    for series_info in cache['data'].values():
                        if isinstance(series_info, dict):
                            tmdb_id = series_info.get('tmdb')
                            if tmdb_id and tmdb_id != '0':
                                cached_ids.add(str(tmdb_id))
                # Handle cache['data'] as list of series info dicts
                elif isinstance(cache, dict) and isinstance(cache.get('data'), list):
                    for series_info in cache['data']:
                        if isinstance(series_info, dict):
                            tmdb_id = series_info.get('tmdb')
                            if tmdb_id and tmdb_id != '0':
                                cached_ids.add(str(tmdb_id))
                # Handle cache as a list of series info dicts
                elif isinstance(cache, list):
                    for series_info in cache:
                        if isinstance(series_info, dict):
                            tmdb_id = series_info.get('tmdb')
                            if tmdb_id and tmdb_id != '0':
                                cached_ids.add(str(tmdb_id))
        except Exception as e:
            print(f"Error reading cache file {pkl_file}: {e}")
    return cached_ids

def get_db_tmdb_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT series_id, tmdb_id FROM series")
    return {str(row[1]): row[0] for row in cursor.fetchall() if row[1]}


def main():
    conn = sqlite3.connect(DB_PATH)
    cached_ids = get_cached_tmdb_ids()
    db_series = get_db_tmdb_ids(conn)
    print(f"Number of unique tmdb_ids in cache: {len(cached_ids)}")
    print(f"Number of unique tmdb_ids in database: {len(db_series)}")
    missing_ids = [db_series[tmdb_id] for tmdb_id in db_series if tmdb_id not in cached_ids]
    if not missing_ids:
        print("No orphaned series found in the database.")
        return
    print(f"Found {len(missing_ids)} series in the database not present in the latest cache.")
    choice = input("Delete these series and their episodes from the database? (y/N): ").strip().lower()
    if choice == 'y':
        cursor = conn.cursor()
        cursor.executemany("DELETE FROM series WHERE series_id = ?", [(sid,) for sid in missing_ids])
        cursor.executemany("DELETE FROM episodes WHERE series_id = ?", [(sid,) for sid in missing_ids])
        conn.commit()
        print(f"Deleted {len(missing_ids)} series and their episodes from the database.")
    else:
        print("No series were deleted.")
    conn.close()

if __name__ == "__main__":
    main()
