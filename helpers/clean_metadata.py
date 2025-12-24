import sqlite3
from pathlib import Path
import logging
import re
import helpers.config_manager as config_manager

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "database" / "media_player.db"


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _clear_vod_metadata_for_stream(conn, stream_id: int):
    """Keep minimal columns for a VOD stream and clear heavy metadata."""
    # Columns to preserve: server_id, category_id, stream_id, name, container_extension, tmdb_id
    # Clear others to NULL/empty
    cols_to_clear = [
        'stream_icon','rating','rating_5based','added','custom_sid','direct_source','plot',
        'cast','director','genre','release_date','duration_secs','duration','video_quality',
        'o_name','cover_big','movie_image','youtube_trailer','actors','description','age',
        'country','backdrop_path','bitrate','status','runtime','clearlogo'
    ]
    set_clause = ', '.join(f"{c}=NULL" for c in cols_to_clear)
    sql = f"UPDATE vod_streams SET {set_clause} WHERE stream_id = ?"
    cur = conn.cursor()
    cur.execute(sql, (stream_id,))
    return cur.rowcount


def _clear_episode_metadata(conn, episode_id: int):
    """Keep minimal columns for an episode and clear heavy metadata."""
    cols_to_clear = [
        'title','plot','duration','airdate','container_extension','rating','crew',
        'movie_image','duration_secs','video','audio','bitrate','custom_sid','added','direct_source'
    ]
    # We keep episode_id, server_id, series_id, season_num, episode_num, tmdb_id
    set_clause = ', '.join(f"{c}=NULL" for c in cols_to_clear)
    sql = f"UPDATE episodes SET {set_clause} WHERE episode_id = ?"
    cur = conn.cursor()
    cur.execute(sql, (episode_id,))
    return cur.rowcount


def _clear_series_metadata(conn, series_id: int):
    """Keep minimal columns for series and clear heavy metadata."""
    cols_to_clear = [
        'cover','plot','cast','director','genre','release_date','last_modified',
        'backdrop_path','youtube_trailer','episode_run_time','category_ids','clearlogo'
    ]
    set_clause = ', '.join(f"{c}=NULL" for c in cols_to_clear)
    sql = f"UPDATE series SET {set_clause} WHERE series_id = ?"
    cur = conn.cursor()
    cur.execute(sql, (series_id,))
    return cur.rowcount


def clean_for_vod_stream(stream_id: int):
    """Public: clear metadata for a single VOD stream ID."""
    try:
        conn = _connect()
        changed = _clear_vod_metadata_for_stream(conn, int(stream_id))
        conn.commit()
        conn.close()
        logger.info(f"Cleared metadata for VOD stream {stream_id} (rows affected: {changed})")
        return True
    except Exception as e:
        logger.exception(f"Failed to clear metadata for VOD stream {stream_id}: {e}")
        return False


def clean_for_episode(episode_id: int):
    """Public: clear metadata for a single episode ID."""
    try:
        conn = _connect()
        changed = _clear_episode_metadata(conn, int(episode_id))
        conn.commit()
        conn.close()
        logger.info(f"Cleared metadata for episode {episode_id} (rows affected: {changed})")
        return True
    except Exception as e:
        logger.exception(f"Failed to clear metadata for episode {episode_id}: {e}")
        return False


def clean_for_series(series_id: int):
    """Public: clear metadata for a single series ID."""
    try:
        conn = _connect()
        changed = _clear_series_metadata(conn, int(series_id))
        conn.commit()
        conn.close()
        logger.info(f"Cleared metadata for series {series_id} (rows affected: {changed})")
        return True
    except Exception as e:
        logger.exception(f"Failed to clear metadata for series {series_id}: {e}")
        return False


def _extract_id_from_strm_content(content: str):
    """Try to extract numeric stream/episode id from typical STRM URL patterns."""
    # common patterns: /movie/.../<id>.<ext> or /series/.../<id>.<ext> or /live/.../<id>
    m = re.search(r"/movie/.+?/([0-9]+)\b", content)
    if m:
        return ('vod', int(m.group(1)))
    m = re.search(r"/series/.+?/([0-9]+)\b", content)
    if m:
        return ('episode', int(m.group(1)))
    m = re.search(r"/live/.+?/([0-9]+)\b", content)
    if m:
        return ('live', int(m.group(1)))
    return (None, None)


def clean_all(progress_callback=None):
    """Scan configured directories and clean metadata for entries that have .strm files.

    This will process VOD movie .strm files and series episode .strm files.
    """
    dirs = config_manager.load_directories()
    movies_path = dirs.get('movies')
    series_path = dirs.get('series')

    processed = 0
    # Process movies
    if movies_path:
        for p in Path(movies_path).rglob('*.strm'):
            try:
                content = p.read_text(errors='ignore')
            except Exception:
                continue
            kind, idv = _extract_id_from_strm_content(content)
            if kind == 'vod' and idv:
                if progress_callback:
                    progress_callback(f"Cleaning VOD metadata for stream {idv}")
                clean_for_vod_stream(idv)
                processed += 1

    # Process series
    if series_path:
        for p in Path(series_path).rglob('*.strm'):
            try:
                content = p.read_text(errors='ignore')
            except Exception:
                continue
            kind, idv = _extract_id_from_strm_content(content)
            if kind == 'episode' and idv:
                if progress_callback:
                    progress_callback(f"Cleaning episode metadata for episode {idv}")
                clean_for_episode(idv)
                processed += 1

    if progress_callback:
        progress_callback(f"Metadata cleaning completed. Processed {processed} items.")
    logger.info(f"Metadata cleaning completed. Processed {processed} items.")
    return True


def preview_clean_all():
    """Scan configured directories and report what would be cleaned without modifying the DB.

    Returns a summary dict with counts and sample IDs.
    """
    dirs = config_manager.load_directories()
    movies_path = dirs.get('movies')
    series_path = dirs.get('series')

    summary = {
        'vod_files_found': 0,
        'vod_items_with_metadata': 0,
        'vod_sample_ids': [],
        'episode_files_found': 0,
        'episode_items_with_metadata': 0,
        'episode_sample_ids': [],
        'series_items_checked': 0,
        'series_items_with_metadata': 0,
        'series_sample_ids': []
    }

    conn = None
    try:
        conn = _connect()
        cur = conn.cursor()

        # Helper to check non-null columns
        def _count_non_null(row, cols):
            cnt = 0
            for c in cols:
                val = row.get(c)
                if val is not None and val != '' and val != 0:
                    cnt += 1
            return cnt

        vod_cols = [
            'stream_icon','rating','rating_5based','added','custom_sid','direct_source','plot',
            'cast','director','genre','release_date','duration_secs','duration','video_quality',
            'o_name','cover_big','movie_image','youtube_trailer','actors','description','age',
            'country','backdrop_path','bitrate','status','runtime','clearlogo'
        ]

        ep_cols = [
            'title','plot','duration','airdate','container_extension','rating','crew',
            'movie_image','duration_secs','video','audio','bitrate','custom_sid','added','direct_source'
        ]

        series_cols = [
            'cover','plot','cast','director','genre','release_date','last_modified',
            'backdrop_path','youtube_trailer','episode_run_time','category_ids','clearlogo'
        ]

        # Scan movies
        if movies_path:
            for p in Path(movies_path).rglob('*.strm'):
                summary['vod_files_found'] += 1
                try:
                    content = p.read_text(errors='ignore')
                except Exception:
                    continue
                kind, idv = _extract_id_from_strm_content(content)
                if kind == 'vod' and idv:
                    cur.execute("SELECT " + ",".join(vod_cols) + " FROM vod_streams WHERE stream_id = ?", (idv,))
                    row = cur.fetchone()
                    if row:
                        non_null = _count_non_null(dict(row), vod_cols)
                        if non_null > 0:
                            summary['vod_items_with_metadata'] += 1
                            if len(summary['vod_sample_ids']) < 20:
                                summary['vod_sample_ids'].append(idv)

        # Scan series episodes
        if series_path:
            for p in Path(series_path).rglob('*.strm'):
                summary['episode_files_found'] += 1
                try:
                    content = p.read_text(errors='ignore')
                except Exception:
                    continue
                kind, idv = _extract_id_from_strm_content(content)
                if kind == 'episode' and idv:
                    cur.execute("SELECT " + ",".join(ep_cols) + " FROM episodes WHERE episode_id = ?", (idv,))
                    row = cur.fetchone()
                    if row:
                        non_null = _count_non_null(dict(row), ep_cols)
                        if non_null > 0:
                            summary['episode_items_with_metadata'] += 1
                            if len(summary['episode_sample_ids']) < 20:
                                summary['episode_sample_ids'].append(idv)

        # Scan series table for heavy metadata (all series rows)
        cur.execute("SELECT series_id, " + ",".join(series_cols) + " FROM series")
        rows = cur.fetchall()
        for r in rows:
            summary['series_items_checked'] += 1
            non_null = _count_non_null(dict(r), series_cols)
            if non_null > 0:
                summary['series_items_with_metadata'] += 1
                if len(summary['series_sample_ids']) < 20:
                    summary['series_sample_ids'].append(r['series_id'])

        return summary

    except Exception as e:
        logger.exception(f"Error during preview scan: {e}")
        return summary
    finally:
        if conn:
            conn.close()
