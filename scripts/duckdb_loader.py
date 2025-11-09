"""
DuckDB data loading module
"""
import glob
import os
import logging
from duckdb_connection import get_duckdb_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_tables(conn):
    # Raw tracks table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_spotify_tracks (
            played_at TIMESTAMP,
            track_id VARCHAR,
            track_name VARCHAR,
            artist_id VARCHAR,
            artist_name VARCHAR,
            album_id VARCHAR,
            album_name VARCHAR,
            album_release_date VARCHAR,
            duration_ms INTEGER,
            popularity INTEGER,
            explicit BOOLEAN,
            track_uri VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (track_id, played_at)
        )
    """)
    
    # Raw artists table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_spotify_artists (
            artist_id VARCHAR PRIMARY KEY,
            artist_name VARCHAR,
            genres VARCHAR,
            popularity INTEGER,
            followers INTEGER,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Raw playlists table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_spotify_playlists (
            playlist_id VARCHAR PRIMARY KEY,
            playlist_name VARCHAR,
            owner_id VARCHAR,
            is_owner BOOLEAN,
            is_public BOOLEAN,
            is_collaborative BOOLEAN,
            total_tracks INTEGER,
            description VARCHAR,
            snapshot_id VARCHAR,
            extracted_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Raw playlist tracks junction table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_spotify_playlist_tracks (
            playlist_id VARCHAR,
            track_id VARCHAR,
            added_at TIMESTAMP,
            added_by VARCHAR,
            position INTEGER,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (playlist_id, track_id)
        )
    """)


def load_to_duckdb(db_path='/opt/airflow/data/duckdb/spotify.duckdb', data_dir='/opt/airflow/data/raw'):
    with get_duckdb_connection(db_path) as conn:
        # Initialize tables
        init_tables(conn)
        
        # Find latest files (search recursively in organized directory structure)
        track_files = sorted(glob.glob(f"{data_dir}/spotify_tracks/**/spotify_tracks_*.csv", recursive=True))
        artist_files = sorted(glob.glob(f"{data_dir}/spotify_artists/**/spotify_artists_*.csv", recursive=True))
        playlist_files = sorted(glob.glob(f"{data_dir}/spotify_playlists/**/spotify_playlists_*.csv", recursive=True))
        playlist_track_files = sorted(glob.glob(f"{data_dir}/spotify_playlist_tracks/**/spotify_playlist_tracks_*.csv", recursive=True))
        
        loaded_count = 0
        
        # Load tracks
        if track_files:
            conn.execute(f"""
                INSERT INTO raw_spotify_tracks 
                SELECT 
                    played_at::TIMESTAMP,
                    track_id,
                    track_name,
                    artist_id,
                    artist_name,
                    album_id,
                    album_name,
                    album_release_date,
                    duration_ms,
                    popularity,
                    explicit,
                    track_uri,
                    CURRENT_TIMESTAMP as loaded_at
                FROM read_csv_auto('{track_files[-1]}')
                ON CONFLICT DO NOTHING
            """)
            loaded_count += 1
            logger.info(f"Loaded tracks from {track_files[-1]}")
        else:
            logger.warning("No track files found")
        
        # Load artists
        if artist_files:
            # Check if file has data
            row_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{artist_files[-1]}')").fetchone()[0]
            if row_count > 0:
                conn.execute(f"""
                    INSERT INTO raw_spotify_artists 
                    SELECT 
                        artist_id, 
                        artist_name, 
                        genres, 
                        popularity, 
                        followers, 
                        now() as loaded_at
                    FROM read_csv_auto('{artist_files[-1]}')
                    ON CONFLICT (artist_id) DO UPDATE SET
                        artist_name = EXCLUDED.artist_name,
                        genres = EXCLUDED.genres,
                        popularity = EXCLUDED.popularity,
                        followers = EXCLUDED.followers,
                        loaded_at = now()
                """)
                loaded_count += 1
                logger.info(f"Loaded artists from {artist_files[-1]}")
            else:
                logger.warning(f"Artists file is empty, skipping: {artist_files[-1]}")
        else:
            logger.warning("No artist files found")
        
        # Load playlists
        if playlist_files:
            if os.path.getsize(playlist_files[-1]) > 0:
                conn.execute(f"""
                    INSERT INTO raw_spotify_playlists 
                    SELECT 
                        playlist_id, 
                        playlist_name, 
                        owner_id, 
                        is_owner, 
                        is_public,
                        is_collaborative, 
                        total_tracks, 
                        description, 
                        snapshot_id,
                        extracted_at,
                        now() as loaded_at
                    FROM read_csv_auto('{playlist_files[-1]}')
                    ON CONFLICT (playlist_id) DO UPDATE SET
                        playlist_name = EXCLUDED.playlist_name,
                        owner_id = EXCLUDED.owner_id,
                        is_owner = EXCLUDED.is_owner,
                        is_public = EXCLUDED.is_public,
                        is_collaborative = EXCLUDED.is_collaborative,
                        total_tracks = EXCLUDED.total_tracks,
                        description = EXCLUDED.description,
                        snapshot_id = EXCLUDED.snapshot_id,
                        extracted_at = EXCLUDED.extracted_at,
                        loaded_at = now()
                """)
                loaded_count += 1
                logger.info(f"Loaded playlists from {playlist_files[-1]}")
            else:
                logger.warning(f"CSV file {playlist_files[-1]} is empty, skipping...")
        else:
            logger.warning("No playlist files found")
        
        # Load playlist tracks
        if playlist_track_files:
            if os.path.getsize(playlist_track_files[-1]) > 0:
                conn.execute(f"""
                    INSERT INTO raw_spotify_playlist_tracks 
                    SELECT 
                        playlist_id, 
                        track_id, 
                        added_at, 
                        added_by, 
                        position, 
                        now() as loaded_at
                    FROM read_csv_auto('{playlist_track_files[-1]}')
                    ON CONFLICT (playlist_id, track_id) DO UPDATE SET
                        added_at = EXCLUDED.added_at,
                        added_by = EXCLUDED.added_by,
                        position = EXCLUDED.position,
                        loaded_at = now()
                """)
                loaded_count += 1
                logger.info(f"Loaded playlist tracks from {playlist_track_files[-1]}")
            else:
                logger.warning(f"CSV file {playlist_track_files[-1]} is empty, skipping...")
        else:
            logger.warning("No playlist-track relationship files found")
        
        logger.info(f"Loaded {loaded_count} of 4 data types successfully")


if __name__ == "__main__":
    load_to_duckdb()
