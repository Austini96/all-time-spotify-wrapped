"""
Load Spotify Extended Streaming History Export into DuckDB

Instructions:
1. Request your data from https://www.spotify.com/account/privacy/
2. Wait for email (5-30 days)
3. Download and extract the ZIP file
4. Place JSON files in data/raw/spotify_export/
5. Run this script: python scripts/load_spotify_export.py
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
    """
    Load Spotify extended streaming history JSON files
    
    Args:
        export_dir: Directory containing Streaming_History_Audio_*.json files
    """
    
    # Find all streaming history files
    json_files = glob.glob(f"{export_dir}/Streaming_History_Audio_*.json")
    
    if not json_files:
        json_files = glob.glob(f"{export_dir}/endsong_*.json")  # Alternative naming
    
    if not json_files:
        logger.error(f"No streaming history files found in {export_dir}")
        logger.info("Expected files: Streaming_History_Audio_*.json or endsong_*.json")
        logger.info("Please extract your Spotify data export to this directory")
        return None
    
    logger.info(f"Found {len(json_files)} streaming history files")
    
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
    
    # Extract track_id from URI (spotify:track:3n3Ppam7vgaVa1iaRUc9Lp)
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
        logger.info(f"Filtered out {before_count - len(df):,} skipped songs (< 30s)")
    
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
    logger.info(f"Date range: {df['played_at'].min()} to {df['played_at'].max()}")
    
    return df


def save_to_csv(df, output_file='data/raw/spotify_export_processed.csv'):
    """Save processed data to CSV"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    logger.info(f"Saved to {output_file}")
    return output_file


def load_to_duckdb(df, db_path='data/duckdb/spotify.duckdb'):
    """Load historical data into DuckDB"""
    
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
    logger.info("Inserting into DuckDB...")
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
    
    # Show stats
    stats = conn.execute("""
        SELECT 
            MIN(played_at) as earliest_play,
            MAX(played_at) as latest_play,
            COUNT(DISTINCT track_id) as unique_tracks,
            COUNT(DISTINCT artist_name) as unique_artists,
            COUNT(*) as total_plays
        FROM raw_spotify_tracks_historical
    """).fetchdf()
    
    print("\n" + "="*60)
    print("HISTORICAL DATA LOADED")
    print("="*60)
    print(stats.to_string(index=False))
    print("="*60 + "\n")
    
    conn.close()
    
    return new_records


def main():
    """Main function"""
    print("\n🎵 Spotify Extended History Loader 🎵\n")
    
    export_dir = 'data/raw/spotify_export'
    
    # Check if directory exists
    if not os.path.exists(export_dir):
        print(f"❌ Directory not found: {export_dir}")
        print("\nTo get your historical data:")
        print("1. Go to: https://www.spotify.com/account/privacy/")
        print("2. Click 'Download your data'")
        print("3. Request 'Extended streaming history'")
        print("4. Wait for email (5-30 days)")
        print("5. Extract ZIP to: data/raw/spotify_export/")
        print("\nCreating directory for you...")
        os.makedirs(export_dir, exist_ok=True)
        print(f"✓ Created {export_dir}")
        print("Please add your JSON files here and run again.")
        return
    
    # Load data
    df = load_streaming_history(export_dir)
    
    if df is None or df.empty:
        return
    
    # Save processed CSV
    csv_file = save_to_csv(df)
    
    # Load to DuckDB
    load_to_duckdb(df)
    
    print("\n✅ Historical data loaded successfully!")
    print("\nYou can now query this data:")
    print("  duckdb data/duckdb/spotify.duckdb")
    print("  SELECT * FROM raw_spotify_tracks_historical LIMIT 10;")
    print("\nOr combine with recent data in your dbt models!")


if __name__ == "__main__":
    main()

