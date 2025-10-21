"""
Spotify API data extraction module
"""

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SpotifyExtractor:

    def __init__(self):
        client_id = os.getenv('SPOTIFY_CLIENT_ID')
        client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI')
        
        if not client_id or not client_secret:
            raise ValueError(
                "Missing Spotify credentials."
            )
        
        cache_path = '/opt/airflow/data/.spotify_cache'
        
        # Check if cache exists
        if not os.path.exists(cache_path):
            logger.warning(
                f"Spotify token cache not found at {cache_path}."
            )
        
        try:
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
            self.sp.current_user()
            logger.info("Spotify client initialized and authenticated successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Spotify client: {e}")
            raise
    
    def get_recently_played(self, limit=50):
        try:
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
            
        except Exception as e:
            logger.error(f"Error fetching recently played: {e}")
            raise
    
    def get_audio_features(self, track_ids):
        try:
            features = []
            # API allows max 100 tracks per request
            for i in range(0, len(track_ids), 100):
                batch = track_ids[i:i+100]
                results = self.sp.audio_features(batch)
                features.extend([f for f in results if f is not None])
            
            df = pd.DataFrame(features)
            logger.info(f"Extracted audio features for {len(df)} tracks")
            return df
            
        except Exception as e:
            logger.warning(f"Error fetching audio features: {e}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'id', 'danceability', 'energy', 'key', 'loudness', 'mode',
                'speechiness', 'acousticness', 'instrumentalness', 'liveness',
                'valence', 'tempo', 'duration_ms', 'time_signature'
            ])
    
    def get_artist_info(self, artist_ids):
        try:
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
            
        except Exception as e:
            logger.warning(f"Error fetching artist info: {e}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'artist_id', 'artist_name', 'genres', 'popularity', 'followers'
            ])
    
    def get_user_playlists(self):
        try:
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
                    try:
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
                            
                    except Exception as e:
                        logger.warning(f"Error fetching tracks for playlist {playlist_name}: {e}")
                        continue
                
                if not playlists['next']:
                    break
                offset += limit
            
            playlists_df = pd.DataFrame(playlists_data)
            playlist_tracks_df = pd.DataFrame(playlist_tracks_data)
            
            logger.info(f"Extracted {len(playlists_df)} playlists with {len(playlist_tracks_df)} track relationships")
            return playlists_df, playlist_tracks_df
            
        except Exception as e:
            logger.error(f"Error fetching playlists: {e}")
            return (
                pd.DataFrame(columns=['playlist_id', 'playlist_name', 'owner_id', 'is_owner', 'is_public', 'is_collaborative', 'total_tracks', 'description', 'snapshot_id', 'extracted_at']),
                pd.DataFrame(columns=['playlist_id', 'track_id', 'added_at', 'added_by', 'position'])
            )
    
    def save_to_csv(self, df, filename, output_dir='/opt/airflow/data/raw'):
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = os.path.join(output_dir, f"{filename}_{timestamp}.csv")
        
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
        
        # Extract audio features (may return empty DataFrame if unavailable)
        track_ids = tracks_df['track_id'].unique().tolist()
        features_df = extractor.get_audio_features(track_ids)
        if not features_df.empty:
            features_file = extractor.save_to_csv(features_df, 'spotify_audio_features')
            result['features_file'] = features_file
            logger.info(f"Saved {len(features_df)} audio features")
        else:
            logger.warning("Audio features not available (skipped)")
            result['features_file'] = None
        
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
        
        logger.info(f"Extraction complete: {sum(1 for v in result.values() if v)} of 5 data sources saved")
        return result
    else:
        logger.warning("No tracks found")
        return None


if __name__ == "__main__":
    extract_spotify_data()