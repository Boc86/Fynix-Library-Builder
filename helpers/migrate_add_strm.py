import re
import shutil
import sqlite3
import logging
from pathlib import Path
import os
import time
from typing import Dict, Set
import helpers.config_manager as config_manager

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"
BACKUP_DIR = Path(__file__).parent.parent / "database" / "backups"

MOVIES_CLEAR_FIELDS = [
    "stream_icon",
    "rating",
    "rating_5based",
    "container_extension",
    "custom_sid",
    "direct_source",
    "plot",
    "cast",
    "director",
    "genre",
    "release_date",
    "duration_secs",
    "duration",
    "video_quality",
    "o_name",
    "cover_big",
    "movie_image",
    "youtube_trailer",
    "actors",
    "description",
    "age",
    "country",
    "backdrop_path",
    "bitrate",
    "status",
    "runtime",
    "clearlogo",
]

EPISODES_CLEAR_FIELDS = [
    "plot",
    "duration",
    "airdate",
    "container_extension",
    "rating",
    "crew",
    "movie_image",
    "duration_secs",
    "video",
    "audio",
    "bitrate",
    "custom_sid",
    "direct_source",
]


def _ensure_backup():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    backup_path = BACKUP_DIR / f"media_player.db.bak.{ts}"
    shutil.copy2(DB_PATH, backup_path)
    logger.info(f"Database backed up to {backup_path}")
    return backup_path


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.row_factory = sqlite3.Row
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info('{table}')")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols


def _add_column(conn: sqlite3.Connection, table: str, column: str):
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
    conn.commit()
    logger.info(f"Added column {column} to {table}")


def _scan_strm_files(folder: Path) -> Dict[str, Set[int]]:
    """Scans for .strm files under folder (recursively) and extracts movie and episode ids from their contents.
    Returns dict with keys 'movies' and 'episodes' mapping to sets of IDs found.
    """
    movies: Set[int] = set()
    episodes: Set[int] = set()

    if not folder.exists():
        logger.warning(f"Configured folder does not exist: {folder}")
        return {"movies": movies, "episodes": episodes}

    for path in folder.rglob("*.strm"):
        try:
            with open(path, "r") as f:
                line = f.readline().strip()
        except Exception as e:
            logger.debug(f"Failed to read {path}: {e}")
            continue

        if not line:
            continue

        # Look for movie URL pattern: /movie/<username>/<password>/<stream_id>.<ext>
        m = re.search(r"/movie/[^/]+/[^/]+/(\d+)\.[a-zA-Z0-9]+", line)
        if m:
            try:
                movies.add(int(m.group(1)))
                continue
            except ValueError:
                pass

        # Look for series/episode pattern: /series/<username>/<password>/<episode_id>.<ext>
        m2 = re.search(r"/series/[^/]+/[^/]+/(\d+)\.[a-zA-Z0-9]+", line)
        if m2:
            try:
                episodes.add(int(m2.group(1)))
                continue
            except ValueError:
                pass

        # If no id found, ignore; filename-based matching is unreliable compared to URL parsing
    return {"movies": movies, "episodes": episodes}


def migrate_mark_strm(dry_run: bool = True) -> Dict[str, int]:
    """Performs migration:
    - backups DB
    - adds `strm` columns to `vod_streams` and `episodes` if missing
    - scans configured directories for .strm files and parses IDs
    - if not dry_run, updates DB rows setting strm='yes' and clears large metadata fields

    Returns a summary dict with counts.
    """
    summary = {"movies_found": 0, "episodes_found": 0, "movies_marked": 0, "episodes_marked": 0}

    if not DB_PATH.exists():
        logger.error(f"Database not found at {DB_PATH}")
        return summary

    # Backup first
    try:
        backup_path = _ensure_backup()
    except Exception as e:
        logger.error(f"Failed to backup DB before migration: {e}")
        return summary

    # Load configured directories
    config = config_manager.load_directories()
    movies_dir = Path(config.get("movies") or "")
    series_dir = Path(config.get("series") or "")

    # Scan both directories
    movies_ids = set()
    episodes_ids = set()
    if movies_dir and movies_dir.exists():
        res = _scan_strm_files(movies_dir)
        movies_ids |= res["movies"]
        episodes_ids |= res["episodes"]

    if series_dir and series_dir.exists():
        res2 = _scan_strm_files(series_dir)
        movies_ids |= res2["movies"]
        episodes_ids |= res2["episodes"]

    summary["movies_found"] = len(movies_ids)
    summary["episodes_found"] = len(episodes_ids)

    logger.info(f"Found {len(movies_ids)} movie stream ids and {len(episodes_ids)} episode ids from .strm files")

    # Open DB and ensure columns exist
    conn = None
    try:
        conn = _connect()
        if not _column_exists(conn, "vod_streams", "strm"):
            logger.info("Adding 'strm' column to vod_streams")
            _add_column(conn, "vod_streams", "strm")
        else:
            logger.info("'strm' column already exists on vod_streams")

        if not _column_exists(conn, "episodes", "strm"):
            logger.info("Adding 'strm' column to episodes")
            _add_column(conn, "episodes", "strm")
        else:
            logger.info("'strm' column already exists on episodes")

        cur = conn.cursor()

        if not dry_run:
            # Apply updates for movies
            for sid in sorted(movies_ids):
                try:
                    # Build SET clause to clear fields and set strm
                    clear_clause = ", ".join([f"{f} = NULL" for f in MOVIES_CLEAR_FIELDS])
                    sql = f"UPDATE vod_streams SET strm = 'yes', {clear_clause} WHERE stream_id = ?"
                    cur.execute(sql, (sid,))
                    if cur.rowcount > 0:
                        summary["movies_marked"] += cur.rowcount
                except sqlite3.OperationalError as e:
                    logger.warning(f"OperationalError updating vod_streams for stream_id {sid}: {e}")
            conn.commit()

            # Apply updates for episodes
            for eid in sorted(episodes_ids):
                try:
                    clear_clause = ", ".join([f"{f} = NULL" for f in EPISODES_CLEAR_FIELDS])
                    sql = f"UPDATE episodes SET strm = 'yes', {clear_clause} WHERE episode_id = ?"
                    cur.execute(sql, (eid,))
                    if cur.rowcount > 0:
                        summary["episodes_marked"] += cur.rowcount
                except sqlite3.OperationalError as e:
                    logger.warning(f"OperationalError updating episodes for episode_id {eid}: {e}")
            conn.commit()

        else:
            logger.info("Dry run: not applying DB changes. Use apply mode to write changes.")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        if conn:
            conn.close()

    logger.info(f"Migration summary: {summary}")
    return summary


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    p = argparse.ArgumentParser(description="Add strm columns and mark existing .strm files in DB")
    p.add_argument("--apply", action="store_true", help="Apply changes to the database (default is dry-run)")
    args = p.parse_args()

    res = migrate_mark_strm(dry_run=not args.apply)
    if not args.apply:
        print("Dry run complete. Summary:", res)
    else:
        print("Migration applied. Summary:", res)
    exit(0)
