"""
DuckDB data loading module
"""
import duckdb
import glob
import os
import logging
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuckDBLoader:
    """Load data into DuckDB warehouse"""
    
    def __init__(self, db_path='/opt/airflow/data/duckdb/spotify.duckdb'):
        #Initialize DuckDB connection with retry logic
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.conn = self._connect_with_retry()
        logger.info(f"Connected to DuckDB at {db_path}")
        self.init_tables()
    
    def _connect_with_retry(self, max_retries=5, retry_delay=2):
        for attempt in range(max_retries):
            try:
                # Try to connect
                conn = duckdb.connect(self.db_path)
                return conn
            except Exception as e:
                if "lock" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"Lock conflict on attempt {attempt + 1}: {e}")
                    cleanup_duckdb_locks(self.db_path)
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to connect to DuckDB after {attempt + 1} attempts: {e}")
                    raise
        raise Exception("Max retries exceeded for DuckDB connection")
    
    def init_tables(self):
        # Raw tracks table
        self.conn.execute("""
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
        
        # Raw audio features table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_spotify_audio_features (
                id VARCHAR PRIMARY KEY,
                danceability DOUBLE,
                energy DOUBLE,
                key INTEGER,
                loudness DOUBLE,
                mode INTEGER,
                speechiness DOUBLE,
                acousticness DOUBLE,
                instrumentalness DOUBLE,
                liveness DOUBLE,
                valence DOUBLE,
                tempo DOUBLE,
                duration_ms INTEGER,
                time_signature INTEGER,
                track_href VARCHAR,
                analysis_url VARCHAR,
                uri VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Raw artists table
        self.conn.execute("""
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
        self.conn.execute("""
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
        self.conn.execute("""
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
    
    def load_tracks(self, csv_file):
        try:
            # Get count before insert
            count_before = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_tracks").fetchone()[0]
            
            self.conn.execute(f"""
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
                FROM read_csv_auto('{csv_file}')
                ON CONFLICT DO NOTHING
            """)
            
            # Get count after insert to calculate difference
            count_after = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_tracks").fetchone()[0]
            count = count_after - count_before
            
        except Exception as e:
            logger.error(f"Error loading tracks: {e}")
            raise
    
    def load_audio_features(self, csv_file):
        try:
            # Check if file has data
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_file}')").fetchone()[0]
            if row_count == 0:
                logger.warning(f"Audio features file is empty, skipping: {csv_file}")
                return
            
            # Get count before insert
            count_before = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_audio_features").fetchone()[0]
            
            self.conn.execute(f"""
                INSERT INTO raw_spotify_audio_features 
                    (id, danceability, energy, key, loudness, mode, speechiness,
                     acousticness, instrumentalness, liveness, valence, tempo,
                     duration_ms, time_signature, uri, loaded_at)
                SELECT 
                    id, danceability, energy, key, loudness, mode, speechiness,
                    acousticness, instrumentalness, liveness, valence, tempo,
                    duration_ms, time_signature, NULL as uri, CURRENT_TIMESTAMP AS loaded_at
                FROM read_csv_auto('{csv_file}')
                ON CONFLICT (id) DO UPDATE SET
                    danceability = EXCLUDED.danceability,
                    energy = EXCLUDED.energy,
                    key = EXCLUDED.key,
                    loudness = EXCLUDED.loudness,
                    mode = EXCLUDED.mode,
                    speechiness = EXCLUDED.speechiness,
                    acousticness = EXCLUDED.acousticness,
                    instrumentalness = EXCLUDED.instrumentalness,
                    liveness = EXCLUDED.liveness,
                    valence = EXCLUDED.valence,
                    tempo = EXCLUDED.tempo,
                    loaded_at = CURRENT_TIMESTAMP
            """)
            
            # Get count after insert to calculate difference
            count_after = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_audio_features").fetchone()[0]
            count = count_after - count_before
            
        except Exception as e:
            logger.warning(f"Could not load audio features: {e}")
            logger.warning("Continuing without audio features...")
    
    def load_artists(self, csv_file):
        try:
            # Check if file has data
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_file}')").fetchone()[0]
            if row_count == 0:
                logger.warning(f"Artists file is empty, skipping: {csv_file}")
                return
            
            # Get count before insert
            count_before = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_artists").fetchone()[0]
            
            self.conn.execute(f"""
                INSERT INTO raw_spotify_artists 
                SELECT 
                    artist_id, 
                    artist_name, 
                    genres, 
                    popularity, 
                    followers, 
                    now() as loaded_at
                FROM read_csv_auto('{csv_file}')
                ON CONFLICT (artist_id) DO UPDATE SET
                    artist_name = EXCLUDED.artist_name,
                    genres = EXCLUDED.genres,
                    popularity = EXCLUDED.popularity,
                    followers = EXCLUDED.followers,
                    loaded_at = now()
            """)
            
            # Get count after insert to calculate difference
            count_after = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_artists").fetchone()[0]
            count = count_after - count_before
            
        except Exception as e:
            logger.warning(f"Could not load artists: {e}")
            logger.warning("Continuing without artist details...")
    
    def load_playlists(self, csv_file):
        try:
            # Check if file is empty
            if os.path.getsize(csv_file) == 0:
                logger.warning(f"CSV file {csv_file} is empty, skipping...")
                return
            
            # Get count before insert
            count_before = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_playlists").fetchone()[0]
            
            self.conn.execute(f"""
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
                FROM read_csv_auto('{csv_file}')
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
            
            # Get count after insert to calculate difference
            count_after = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_playlists").fetchone()[0]
            count = count_after - count_before
            
        except Exception as e:
            logger.error(f"Error loading playlists: {e}")
            raise
    
    def load_playlist_tracks(self, csv_file):
        try:
            # Check if file is empty
            if os.path.getsize(csv_file) == 0:
                logger.warning(f"CSV file {csv_file} is empty, skipping...")
                return
            
            # Get count before insert
            count_before = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_playlist_tracks").fetchone()[0]
            
            self.conn.execute(f"""
                INSERT INTO raw_spotify_playlist_tracks 
                SELECT 
                    playlist_id, 
                    track_id, 
                    added_at, 
                    added_by, 
                    position, 
                    now() as loaded_at
                FROM read_csv_auto('{csv_file}')
                ON CONFLICT (playlist_id, track_id) DO UPDATE SET
                    added_at = EXCLUDED.added_at,
                    added_by = EXCLUDED.added_by,
                    position = EXCLUDED.position,
                    loaded_at = now()
            """)
            
            # Get count after insert to calculate difference
            count_after = self.conn.execute("SELECT COUNT(*) FROM raw_spotify_playlist_tracks").fetchone()[0]
            count = count_after - count_before
            
        except Exception as e:
            logger.error(f"Error loading playlist tracks: {e}")
            raise
    
    def load_latest_csv_files(self, data_dir='/opt/airflow/data/raw'):
        # Find latest files
        track_files = sorted(glob.glob(f"{data_dir}/spotify_tracks_*.csv"))
        feature_files = sorted(glob.glob(f"{data_dir}/spotify_audio_features_*.csv"))
        artist_files = sorted(glob.glob(f"{data_dir}/spotify_artists_*.csv"))
        playlist_files = sorted(glob.glob(f"{data_dir}/spotify_playlists_*.csv"))
        playlist_track_files = sorted(glob.glob(f"{data_dir}/spotify_playlist_tracks_*.csv"))
        
        loaded_count = 0
        
        if track_files:
            self.load_tracks(track_files[-1])
            loaded_count += 1
        else:
            logger.warning("No track files found")
        
        if feature_files:
            self.load_audio_features(feature_files[-1])
            loaded_count += 1
        else:
            logger.warning("No audio features files found (this is OK if Spotify API access is limited)")
        
        if artist_files:
            self.load_artists(artist_files[-1])
            loaded_count += 1
        else:
            logger.warning("No artist files found (this is OK if Spotify API access is limited)")
        
        if playlist_files:
            self.load_playlists(playlist_files[-1])
            loaded_count += 1
        else:
            logger.warning("No playlist files found")
        
        if playlist_track_files:
            self.load_playlist_tracks(playlist_track_files[-1])
            loaded_count += 1
        else:
            logger.warning("No playlist-track relationship files found")
        
        logger.info(f"Loaded {loaded_count} of 5 data types successfully")
    
    def close(self):
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
        except Exception as e:
            logger.warning(f"Error closing DuckDB connection: {e}")
        finally:
            self.conn = None


def cleanup_duckdb_locks(db_path='/opt/airflow/data/duckdb/spotify.duckdb'):
    try:
        # Remove the main database file if it exists (will be recreated)
        if os.path.exists(db_path):
            os.remove(db_path)
        
        # Remove any .wal files that might be holding locks
        wal_files = glob.glob(f"{db_path}.wal*")
        for wal_file in wal_files:
            os.remove(wal_file)
            logger.info(f"Removed stale WAL file: {wal_file}")
        
        # Remove any .lock files
        lock_files = glob.glob(f"{db_path}.lock*")
        for lock_file in lock_files:
            os.remove(lock_file)
            logger.info(f"Removed stale lock file: {lock_file}")
        
        # Remove any .tmp files
        tmp_files = glob.glob(f"{db_path}.tmp*")
        for tmp_file in tmp_files:
            os.remove(tmp_file)
            
        # Remove any .backup files
        backup_files = glob.glob(f"{db_path}.backup*")
        for backup_file in backup_files:
            os.remove(backup_file)
            
    except Exception as e:
        logger.warning(f"Error cleaning up locks: {e}")

def load_to_duckdb():
    # Clean up any stale locks first
    cleanup_duckdb_locks()
    
    try:
        loader = DuckDBLoader()
        loader.load_latest_csv_files()
        loader.close()
    except Exception as e:
        if "lock" in str(e).lower():
            logger.error(f"Lock conflict persists after cleanup: {e}")
            
            # Try with a timestamped database file as fallback
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            fallback_path = f'/opt/airflow/data/duckdb/spotify_{timestamp}.duckdb'
            
            try:
                loader = DuckDBLoader(db_path=fallback_path)
                loader.load_latest_csv_files()
                loader.close()
            except Exception as fallback_error:
                logger.error(f"Fallback database also failed: {fallback_error}")
                raise
        else:
            raise


if __name__ == "__main__":
    load_to_duckdb()