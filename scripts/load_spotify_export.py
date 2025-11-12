"""
Load Spotify Extended Streaming History Export into DuckDB
"""

import json
import pandas as pd
import duckdb
import glob
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_streaming_history(export_dir='data/raw/spotify_export'):
    # Find all streaming history files
    json_files = glob.glob(f"{export_dir}/Streaming_History_Audio_*.json")
    
    if not json_files:
        json_files = glob.glob(f"{export_dir}/endsong_*.json")  # Alternative naming
    
    if not json_files:
        logger.error(f"No streaming history files found in {export_dir}")
        return None
    
    # Load all JSON files
    all_plays = []
    for file in sorted(json_files):
        logger.info(f"Loading {file}...")
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            all_plays.extend(data)
    
    logger.info(f"Loaded {len(all_plays):,} total plays from export")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_plays)
    
    # Standardize column names (Spotify uses different naming in exports)
    column_mapping = {
        'ts': 'played_at',
        'master_metadata_track_name': 'track_name',
        'master_metadata_album_artist_name': 'artist_name',
        'master_metadata_album_album_name': 'album_name',
        'spotify_track_uri': 'track_uri',
        'ms_played': 'duration_ms'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Extract track_id from URI
    if 'track_uri' in df.columns:
        df['track_id'] = df['track_uri'].str.split(':').str[-1]
    
    # Convert timestamp to datetime
    if 'played_at' in df.columns:
        df['played_at'] = pd.to_datetime(df['played_at'])
    
    # Filter out skipped songs (played less than 30 seconds)
    min_play_duration = 30000  # 30 seconds in ms
    if 'duration_ms' in df.columns:
        before_count = len(df)
        df = df[df['duration_ms'] >= min_play_duration]
    
    # Select relevant columns
    columns_to_keep = [
        'played_at', 'track_id', 'track_name', 'artist_name', 
        'album_name', 'duration_ms', 'track_uri'
    ]
    
    available_columns = [col for col in columns_to_keep if col in df.columns]
    df = df[available_columns]
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Sort by date
    df = df.sort_values('played_at')
    
    logger.info(f"Processed {len(df):,} valid plays")
    return df


def save_to_csv(df, output_file='data/raw/spotify_export_processed.csv'):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    logger.info(f"Saved to {output_file}")
    return output_file


def load_to_duckdb(df, db_path='/opt/airflow/data/duckdb/spotify.duckdb'):
    conn = duckdb.connect(db_path)
    
    # Create table if not exists (similar to raw_spotify_tracks)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_spotify_tracks_historical (
            played_at TIMESTAMP,
            track_id VARCHAR,
            track_name VARCHAR,
            artist_name VARCHAR,
            album_name VARCHAR,
            duration_ms INTEGER,
            track_uri VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (track_id, played_at)
        )
    """)
    
    # Count before
    count_before = conn.execute(
        "SELECT COUNT(*) FROM raw_spotify_tracks_historical"
    ).fetchone()[0]
    
    # Insert data
    conn.execute("""
        INSERT INTO raw_spotify_tracks_historical
        SELECT 
            played_at::TIMESTAMP,
            track_id,
            track_name,
            artist_name,
            album_name,
            duration_ms,
            track_uri,
            CURRENT_TIMESTAMP as loaded_at
        FROM df
        ON CONFLICT DO NOTHING
    """)
    
    # Count after
    count_after = conn.execute(
        "SELECT COUNT(*) FROM raw_spotify_tracks_historical"
    ).fetchone()[0]
    
    new_records = count_after - count_before
    logger.info(f"Inserted {new_records:,} new historical records")
    logger.info(f"Total historical records: {count_after:,}")
    
    conn.close()
    
    return new_records


def main():
    export_dir = 'data/raw/spotify_export'
    
    # Check if directory exists
    if not os.path.exists(export_dir):
        print(f"Directory not found: {export_dir}")
        return
    
    # Load data
    df = load_streaming_history(export_dir)
    
    if df is None or df.empty:
        return
    
    # Save processed CSV
    csv_file = save_to_csv(df)
    
    # Load to DuckDB
    load_to_duckdb(df)
    
    print("Historical data loaded successfully.")

if __name__ == "__main__":
    main()