"""
Spotify API data extraction module

Supports two authentication methods:
1. Airflow Connection (recommended) - credentials stored in Airflow's encrypted DB
2. Environment variables (fallback) - for local development
"""

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import os
import json
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spotify OAuth scopes needed
SPOTIFY_SCOPES = 'user-read-recently-played user-top-read user-library-read playlist-read-private playlist-read-collaborative'


def get_spotify_credentials():
    """
    Get Spotify credentials from Airflow Connection or environment variables.

    Airflow Connection format (conn_id='spotify_oauth'):
        - Login: client_id
        - Password: client_secret
        - Extra (JSON): {"redirect_uri": "...", "refresh_token": "..."}

    Returns tuple: (client_id, client_secret, redirect_uri, refresh_token)
    """
    # Try Airflow Connection first (when running in Airflow)
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('spotify_oauth')

        client_id = conn.login
        client_secret = conn.password
        extra = json.loads(conn.extra) if conn.extra else {}
        redirect_uri = extra.get('redirect_uri', 'http://localhost:8888/callback')
        refresh_token = extra.get('refresh_token')

        logger.info("Using credentials from Airflow Connection 'spotify_oauth'")
        return client_id, client_secret, redirect_uri, refresh_token

    except Exception as e:
        logger.debug(f"Airflow Connection not available: {e}")

    # Fallback to environment variables (local development)
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8888/callback')
    refresh_token = os.getenv('SPOTIFY_REFRESH_TOKEN')

    if not client_id or not client_secret:
        raise ValueError(
            "Spotify credentials not found. Set up Airflow Connection 'spotify_oauth' "
            "or set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables."
        )

    logger.info("Using credentials from environment variables")
    return client_id, client_secret, redirect_uri, refresh_token


class SpotifyExtractor:
    """
    Extracts data from Spotify API.

    Authentication priority:
    1. Refresh token (if provided) - no browser needed
    2. Token cache file - for backward compatibility
    3. OAuth flow - requires browser (local only)
    """

    def __init__(self, client_id=None, client_secret=None, redirect_uri=None, refresh_token=None):
        # Get credentials if not provided
        if not client_id:
            client_id, client_secret, redirect_uri, refresh_token = get_spotify_credentials()

        cache_path = os.getenv('SPOTIFY_CACHE_PATH', '/opt/airflow/data/.spotify_cache')

        # Create auth manager
        auth_manager = SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=SPOTIFY_SCOPES,
            cache_path=cache_path,
            open_browser=False
        )

        # If we have a refresh token, use it to get access token (no browser needed)
        if refresh_token:
            logger.info("Using refresh token for authentication")
            try:
                token_info = auth_manager.refresh_access_token(refresh_token)
                auth_manager.cache_handler.save_token_to_cache(token_info)
            except Exception as e:
                logger.warning(f"Failed to use refresh token: {e}, falling back to cache")
        elif not os.path.exists(cache_path):
            logger.warning(
                f"No refresh token and no cache at {cache_path}. "
                "Run authenticate_spotify.py locally first."
            )

        self.sp = spotipy.Spotify(auth_manager=auth_manager)

        # Test the connection
        user = self.sp.current_user()
        if not user:
            raise Exception("Failed to authenticate with Spotify")
        logger.info(f"Authenticated as Spotify user: {user.get('display_name', user.get('id'))}")
    
    def get_recently_played(self, limit=50):
        results = self.sp.current_user_recently_played(limit=limit)
        tracks_data = []
        
        for item in results['items']:
            track = item['track']
            if not track:
                continue

            played_at = item['played_at']

            # Safely extract artist info - some tracks may have no artists
            artists = track.get('artists', [])
            if not artists:
                logger.warning(f"Track {track.get('id')} has no artists, skipping")
                continue

            primary_artist = artists[0]

            tracks_data.append({
                'played_at': played_at,
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_id': primary_artist.get('id'),
                'artist_name': primary_artist.get('name'),
                'album_id': track.get('album', {}).get('id'),
                'album_name': track.get('album', {}).get('name'),
                'album_release_date': track.get('album', {}).get('release_date'),
                'duration_ms': track.get('duration_ms'),
                'popularity': track.get('popularity'),
                'explicit': track.get('explicit'),
                'track_uri': track.get('uri')
            })
        
        df = pd.DataFrame(tracks_data)
        logger.info(f"Extracted {len(df)} recently played tracks")
        return df
    
    def get_artist_info(self, artist_ids):
        artists_data = []
        # API allows max 50 artists per request
        for i in range(0, len(artist_ids), 50):
            batch = artist_ids[i:i+50]
            results = self.sp.artists(batch)
            
            for artist in results['artists']:
                if artist:  # Some artists might be None
                    artists_data.append({
                        'artist_id': artist['id'],
                        'artist_name': artist['name'],
                        'genres': ','.join(artist.get('genres', [])),
                        'popularity': artist['popularity'],
                        'followers': artist['followers']['total']
                    })
        
        df = pd.DataFrame(artists_data)
        logger.info(f"Extracted info for {len(df)} artists")
        return df
    
    def get_user_playlists(self):
        playlists_data = []
        playlist_tracks_data = []
        
        # Get current user ID
        user = self.sp.current_user()
        user_id = user['id']
        
        # Get all playlists (handle pagination)
        offset = 0
        limit = 50
        
        while True:
            playlists = self.sp.current_user_playlists(limit=limit, offset=offset)
            
            if not playlists['items']:
                break
            
            for playlist in playlists['items']:
                if not playlist:  # Skip None items
                    continue
                
                playlist_id = playlist['id']
                playlist_name = playlist['name']
                playlist_owner = playlist['owner']['id']
                is_public = playlist['public']
                is_collaborative = playlist['collaborative']
                total_tracks = playlist['tracks']['total']
                
                # Store playlist metadata
                playlists_data.append({
                    'playlist_id': playlist_id,
                    'playlist_name': playlist_name,
                    'owner_id': playlist_owner,
                    'is_owner': (playlist_owner == user_id),
                    'is_public': is_public,
                    'is_collaborative': is_collaborative,
                    'total_tracks': total_tracks,
                    'description': playlist.get('description', ''),
                    'snapshot_id': playlist.get('snapshot_id', ''),
                    'extracted_at': datetime.now()
                })
                
                # Get tracks for this playlist
                track_offset = 0
                track_limit = 100
                
                while True:
                    playlist_tracks = self.sp.playlist_tracks(
                        playlist_id,
                        limit=track_limit,
                        offset=track_offset
                    )
                    
                    if not playlist_tracks['items']:
                        break
                    
                    for item in playlist_tracks['items']:
                        if not item or not item.get('track'):
                            continue
                        
                        track = item['track']
                        if not track or not track.get('id'):
                            continue
                        
                        playlist_tracks_data.append({
                            'playlist_id': playlist_id,
                            'track_id': track['id'],
                            'added_at': item.get('added_at'),
                            'added_by': item.get('added_by', {}).get('id') if item.get('added_by') else None,
                            'position': track_offset + playlist_tracks['items'].index(item)
                        })
                    
                    if not playlist_tracks['next']:
                        break
                    track_offset += track_limit
            
            if not playlists['next']:
                break
            offset += limit
        
        playlists_df = pd.DataFrame(playlists_data)
        playlist_tracks_df = pd.DataFrame(playlist_tracks_data)
        
        logger.info(f"Extracted {len(playlists_df)} playlists with {len(playlist_tracks_df)} track relationships")
        return playlists_df, playlist_tracks_df
    
    def save_to_csv(self, df, filename, output_dir='/opt/airflow/data/raw'):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Extract year, month, day from timestamp (format: YYYYMMDD_HHMMSS)
        year = timestamp[:4]
        month = timestamp[4:6]
        day = timestamp[6:8]
        
        # Create directory structure: {category}/{year}/{month}/{day}/
        category_dir = os.path.join(output_dir, filename, year, month, day)
        os.makedirs(category_dir, exist_ok=True)
        
        filepath = os.path.join(category_dir, f"{filename}_{timestamp}.csv")
        
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(df)} records to {filepath}")
        return filepath


def extract_spotify_data():
    """
    Extract data from Spotify API and save to CSV files.
    Returns dict with file paths on success, None on failure.
    """
    try:
        extractor = SpotifyExtractor()
    except Exception as e:
        logger.error(f"Failed to initialize Spotify client: {e}")
        raise

    result = {}

    try:
        # Extract recently played tracks
        tracks_df = extractor.get_recently_played(limit=50)
    except Exception as e:
        logger.error(f"Failed to get recently played tracks: {e}")
        raise

    if tracks_df.empty:
        logger.warning("No tracks found")
        return None

    try:
        # Save tracks
        tracks_file = extractor.save_to_csv(tracks_df, 'spotify_tracks')
        result['tracks_file'] = tracks_file
    except Exception as e:
        logger.error(f"Failed to save tracks CSV: {e}")
        raise

    # Extract artist information (non-critical, continue on failure)
    try:
        artist_ids = tracks_df['artist_id'].dropna().unique().tolist()
        if artist_ids:
            artists_df = extractor.get_artist_info(artist_ids)
            if not artists_df.empty:
                artists_file = extractor.save_to_csv(artists_df, 'spotify_artists')
                result['artists_file'] = artists_file
            else:
                logger.warning("Artist info not available (skipped)")
                result['artists_file'] = None
        else:
            logger.warning("No artist IDs to fetch")
            result['artists_file'] = None
    except Exception as e:
        logger.warning(f"Failed to get artist info (non-critical): {e}")
        result['artists_file'] = None

    # Extract playlist information (non-critical, continue on failure)
    try:
        playlists_df, playlist_tracks_df = extractor.get_user_playlists()
        if not playlists_df.empty:
            playlists_file = extractor.save_to_csv(playlists_df, 'spotify_playlists')
            result['playlists_file'] = playlists_file
        else:
            logger.warning("Playlists not available (skipped)")
            result['playlists_file'] = None

        if not playlist_tracks_df.empty:
            playlist_tracks_file = extractor.save_to_csv(playlist_tracks_df, 'spotify_playlist_tracks')
            result['playlist_tracks_file'] = playlist_tracks_file
        else:
            logger.warning("Playlist tracks not available (skipped)")
            result['playlist_tracks_file'] = None
    except Exception as e:
        logger.warning(f"Failed to get playlist info (non-critical): {e}")
        result['playlists_file'] = None
        result['playlist_tracks_file'] = None

    logger.info(f"Extraction complete: {sum(1 for v in result.values() if v)} of 4 data sources saved")
    return result


if __name__ == "__main__":
    extract_spotify_data()
