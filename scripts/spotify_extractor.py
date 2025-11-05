"""
Spotify API data extraction module
"""

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import os
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SpotifyExtractor:

    def __init__(self):
        client_id = os.getenv('SPOTIFY_CLIENT_ID')
        client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI')
        cache_path = '/opt/airflow/data/.spotify_cache'
        
        # Check if cache exists
        if not os.path.exists(cache_path):
            logger.warning(
                f"Spotify token cache not found at {cache_path}."
            )
        
        self.sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope='user-read-recently-played user-top-read user-library-read playlist-read-private playlist-read-collaborative',
                cache_path=cache_path,
                open_browser=False  # Disable browser in Airflow environment
            )
        )
        # Test the connection
        is_authenticated = self.sp.current_user()
        if not is_authenticated:
            raise Exception("Failed to authenticate with Spotify")
        else:
            logger.info("Spotify client initialized and authenticated successfully")
    
    def get_recently_played(self, limit=50):
        results = self.sp.current_user_recently_played(limit=limit)
        tracks_data = []
        
        for item in results['items']:
            track = item['track']
            played_at = item['played_at']
            
            tracks_data.append({
                'played_at': played_at,
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_id': track['artists'][0]['id'],
                'artist_name': track['artists'][0]['name'],
                'album_id': track['album']['id'],
                'album_name': track['album']['name'],
                'album_release_date': track['album'].get('release_date'),
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'explicit': track['explicit'],
                'track_uri': track['uri']
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
    extractor = SpotifyExtractor()
    result = {}
    
    # Extract recently played tracks
    tracks_df = extractor.get_recently_played(limit=50)
    
    if not tracks_df.empty:
        # Save tracks
        tracks_file = extractor.save_to_csv(tracks_df, 'spotify_tracks')
        result['tracks_file'] = tracks_file
        logger.info(f"Saved {len(tracks_df)} tracks")
        
        # Extract artist information (may return empty DataFrame if unavailable)
        artist_ids = tracks_df['artist_id'].unique().tolist()
        artists_df = extractor.get_artist_info(artist_ids)
        if not artists_df.empty:
            artists_file = extractor.save_to_csv(artists_df, 'spotify_artists')
            result['artists_file'] = artists_file
            logger.info(f"Saved {len(artists_df)} artists")
        else:
            logger.warning("Artist info not available (skipped)")
            result['artists_file'] = None
        
        # Extract playlist information
        playlists_df, playlist_tracks_df = extractor.get_user_playlists()
        if not playlists_df.empty:
            playlists_file = extractor.save_to_csv(playlists_df, 'spotify_playlists')
            result['playlists_file'] = playlists_file
            logger.info(f"Saved {len(playlists_df)} playlists")
        else:
            logger.warning("Playlists not available (skipped)")
            result['playlists_file'] = None
        
        if not playlist_tracks_df.empty:
            playlist_tracks_file = extractor.save_to_csv(playlist_tracks_df, 'spotify_playlist_tracks')
            result['playlist_tracks_file'] = playlist_tracks_file
            logger.info(f"Saved {len(playlist_tracks_df)} playlist-track relationships")
        else:
            logger.warning("Playlist tracks not available (skipped)")
            result['playlist_tracks_file'] = None
        
        logger.info(f"Extraction complete: {sum(1 for v in result.values() if v)} of 4 data sources saved")
        # Wait 30 seconds before proceeding
        time.sleep(30)
        return result
    else:
        logger.warning("No tracks found")
        # Wait 30 seconds before proceeding
        time.sleep(30)
        return None


if __name__ == "__main__":
    extract_spotify_data()