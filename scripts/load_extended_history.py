"""
Load Spotify Extended Streaming History JSON files into DuckDB
"""

import json
import glob
import os
import logging
import pandas as pd
import duckdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_extended_streaming_history():
    """
    Load Spotify extended streaming history JSON files into DuckDB.
    Skips if data already loaded. Returns early if no files found.
    """
    # Paths (use env vars with fallbacks)
    extended_history_dir = os.getenv('EXTENDED_HISTORY_DIR', '/opt/airflow/data/extended_history')
    duckdb_path = os.getenv('DUCKDB_PATH', '/opt/airflow/data/duckdb/spotify.duckdb')

    # Check if extended history directory exists
    if not os.path.exists(extended_history_dir):
        logger.info(f"Extended history directory not found: {extended_history_dir} (this is OK if you haven't exported history)")
        return

    # Find all JSON files
    json_files = glob.glob(f"{extended_history_dir}/Streaming_History_Audio_*.json")

    if not json_files:
        logger.info(f"No streaming history JSON files found in {extended_history_dir}")
        return

    logger.info(f"Found {len(json_files)} extended history files")

    # Connect to DuckDB
    try:
        conn = duckdb.connect(duckdb_path)
    except duckdb.Error as e:
        logger.error(f"Failed to connect to DuckDB at {duckdb_path}: {e}")
        raise
    
    try:
        # Check if table exists and has data
        table_exists = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'raw_spotify_extended_history'
        """).fetchone()[0] > 0
        
        if table_exists:
            result = conn.execute("""
                SELECT COUNT(*) as count 
                FROM raw_spotify_extended_history
            """).fetchone()
            
            if result and result[0] > 0:
                logger.info(f"Extended history already loaded ({result[0]:,} records)")
                logger.info("Skipping load - data already exists")
                return
        
        # Create raw extended history table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_spotify_extended_history (
                ts TIMESTAMP,
                platform VARCHAR,
                ms_played INTEGER,
                conn_country VARCHAR,
                ip_addr VARCHAR,
                master_metadata_track_name VARCHAR,
                master_metadata_album_artist_name VARCHAR,
                master_metadata_album_album_name VARCHAR,
                spotify_track_uri VARCHAR,
                episode_name VARCHAR,
                episode_show_name VARCHAR,
                spotify_episode_uri VARCHAR,
                audiobook_title VARCHAR,
                audiobook_uri VARCHAR,
                audiobook_chapter_uri VARCHAR,
                audiobook_chapter_title VARCHAR,
                reason_start VARCHAR,
                reason_end VARCHAR,
                shuffle BOOLEAN,
                skipped BOOLEAN,
                offline BOOLEAN,
                offline_timestamp BIGINT,
                incognito_mode BOOLEAN,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        total_records = 0
        
        # Load each JSON file with batch inserts
        for json_file in sorted(json_files):
            logger.info(f"Loading {os.path.basename(json_file)}...")

            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {json_file}: {e}")
                continue  # Skip this file, try the next one
            except IOError as e:
                logger.error(f"Failed to read {json_file}: {e}")
                continue

            # Filter out episodes and audiobooks (keep only music tracks)
            music_data = [
                record for record in data
                if record.get('spotify_track_uri') and record['spotify_track_uri'].startswith('spotify:track:')
            ]

            logger.info(f"Found {len(data)} total records, {len(music_data)} music tracks")

            if not music_data:
                continue
            
            # Convert to DataFrame for batch insert
            df = pd.DataFrame(music_data)
            
            # Register DataFrame as temporary table to avoid DuckDB IndexError bug
            # DuckDB's direct DataFrame handling (FROM df) has issues with certain DataFrame states,
            # so registering it as a virtual table ensures reliable batch inserts
            conn.register('temp_extended_history', df)
            
            # Batch insert from temporary table
            conn.execute("""
                INSERT INTO raw_spotify_extended_history 
                (ts, platform, ms_played, conn_country, ip_addr,
                 master_metadata_track_name, master_metadata_album_artist_name,
                 master_metadata_album_album_name, spotify_track_uri,
                 episode_name, episode_show_name, spotify_episode_uri,
                 audiobook_title, audiobook_uri, audiobook_chapter_uri,
                 audiobook_chapter_title, reason_start, reason_end,
                 shuffle, skipped, offline, offline_timestamp, incognito_mode,
                 loaded_at)
                SELECT 
                    ts::TIMESTAMP,
                    platform,
                    ms_played,
                    conn_country,
                    ip_addr,
                    master_metadata_track_name,
                    master_metadata_album_artist_name,
                    master_metadata_album_album_name,
                    spotify_track_uri,
                    episode_name,
                    episode_show_name,
                    spotify_episode_uri,
                    audiobook_title,
                    audiobook_uri,
                    audiobook_chapter_uri,
                    audiobook_chapter_title,
                    reason_start,
                    reason_end,
                    shuffle,
                    skipped,
                    offline,
                    offline_timestamp,
                    incognito_mode,
                    CURRENT_TIMESTAMP
                FROM temp_extended_history
            """)
            
            # Unregister temporary table to free memory
            conn.unregister('temp_extended_history')
            
            total_records += len(music_data)
            logger.info(f"Loaded {len(music_data)} records from {os.path.basename(json_file)}")
        
        logger.info(f"Total records loaded: {total_records:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    load_extended_streaming_history()