"""
Load Spotify Extended Streaming History JSON files into DuckDB
"""

import json
import glob
import os
import logging
import pandas as pd
from duckdb_connection import get_duckdb_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_extended_streaming_history():
    # Paths
    extended_history_dir = '/opt/airflow/data/extended_history'
    duckdb_path = '/opt/airflow/data/duckdb/spotify.duckdb'
    
    # Check if extended history directory exists
    if not os.path.exists(extended_history_dir):
        logger.warning(f"Extended history directory not found: {extended_history_dir}")
        return
    
    # Find all JSON files
    json_files = glob.glob(f"{extended_history_dir}/Streaming_History_Audio_*.json")
    
    if not json_files:
        logger.warning(f"No streaming history JSON files found in {extended_history_dir}")
        return
    
    logger.info(f"Found {len(json_files)} extended history files")
    
    # Use single connection for check and load
    with get_duckdb_connection(duckdb_path) as conn:
        # Check if data already exists
        try:
            result = conn.execute("""
                SELECT COUNT(*) as count 
                FROM raw_spotify_extended_history
            """).fetchone()
            
            if result and result[0] > 0:
                logger.info(f"Extended history already loaded ({result[0]:,} records)")
                logger.info("Skipping load - data already exists")
                return
        except Exception:
            # Table doesn't exist yet, continue
            pass
        
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
            
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
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
            
            # Batch insert - MUCH FASTER than row-by-row
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
                FROM df
            """)
            
            total_records += len(music_data)
            logger.info(f"Loaded {len(music_data)} records from {os.path.basename(json_file)}")
        
        logger.info(f"Total records loaded: {total_records:,}")


if __name__ == "__main__":
    load_extended_streaming_history()
