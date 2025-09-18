import sqlite3
import pickle
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"
CACHE_DIR = Path(DB_PATH).parent / "cache" / "vod_metadata"


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
                    debug_printed = True
                # Handle cache['data'] as dict of movie info dicts
                if isinstance(cache, dict) and isinstance(cache.get('data'), dict):
                    for movie_info in cache['data'].values():
                        tmdb_id = movie_info.get('tmdb_id') or movie_info.get('tmdb')
                        if tmdb_id:
                            cached_ids.add(str(tmdb_id))
        except Exception as e:
            print(f"Error reading cache file {pkl_file}: {e}")
    return cached_ids


def get_db_tmdb_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT stream_id, tmdb_id FROM vod_streams")
    return {str(row[1]): row[0] for row in cursor.fetchall() if row[1]}


def main():
    conn = sqlite3.connect(DB_PATH)
    cached_ids = get_cached_tmdb_ids()
    db_movies = get_db_tmdb_ids(conn)
    print(f"Number of unique tmdb_ids in cache: {len(cached_ids)}")
    print(f"Number of unique tmdb_ids in database: {len(db_movies)}")
    missing_ids = [db_movies[tmdb_id] for tmdb_id in db_movies if tmdb_id not in cached_ids]
    if not missing_ids:
        print("No orphaned movies found in the database.")
        return
    print(f"Found {len(missing_ids)} movies in the database not present in the latest cache.")
    choice = input("Delete these movies from the database? (y/N): ").strip().lower()
    if choice == 'y':
        cursor = conn.cursor()
        cursor.executemany("DELETE FROM vod_streams WHERE stream_id = ?", [(mid,) for mid in missing_ids])
        conn.commit()
        print(f"Deleted {len(missing_ids)} movies from the database.")
    else:
        print("No movies were deleted.")
    conn.close()

if __name__ == "__main__":
    main()
