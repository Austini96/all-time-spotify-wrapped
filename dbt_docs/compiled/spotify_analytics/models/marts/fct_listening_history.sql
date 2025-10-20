

SELECT
    t.play_id,
    t.played_at,
    t.played_date,
    t.played_year,
    t.played_month,
    t.played_day,
    t.played_hour,
    t.played_day_of_week,
    t.played_day_name,
    
    -- Track information
    t.track_id,
    t.track_name,
    t.duration_ms,
    t.popularity as track_popularity,
    t.explicit,
    t.track_uri,
    
    -- Artist information
    t.artist_id,
    t.artist_name,
    a.genres as artist_genres,
    a.popularity as artist_popularity,
    a.followers as artist_followers,
    
    -- Album information
    t.album_id,
    t.album_name,
    t.album_release_date,
    t.album_release_year,
    
    -- Playlist information (if track was played from a playlist context)
    -- Note: This requires context tracking which isn't available in recently_played API
    -- For now, we'll add a reference to show which playlists contain this track
    STRING_AGG(DISTINCT p.playlist_name, ', ') AS playlists_containing_track,
    COUNT(DISTINCT pt.playlist_id) AS playlist_count,
    
    -- Audio features
    af.danceability,
    af.energy,
    af.key,
    af.loudness,
    af.mode,
    af.mode_name,
    af.speechiness,
    af.acousticness,
    af.instrumentalness,
    af.liveness,
    af.valence,
    af.tempo,
    af.time_signature,
    af.mood_category,
    
    -- Metadata
    t.loaded_at

FROM "spotify"."staging"."stg_spotify_tracks" t
LEFT JOIN "spotify"."staging"."stg_spotify_audio_features" af
    ON t.track_id = af.track_id
LEFT JOIN "spotify"."staging"."stg_spotify_artists" a
    ON t.artist_id = a.artist_id
LEFT JOIN "spotify"."staging"."stg_spotify_playlist_tracks" pt
    ON t.track_id = pt.track_id
LEFT JOIN "spotify"."staging"."stg_spotify_playlists" p
    ON pt.playlist_id = p.playlist_id
GROUP BY
    t.play_id,
    t.played_at,
    t.played_date,
    t.played_year,
    t.played_month,
    t.played_day,
    t.played_hour,
    t.played_day_of_week,
    t.played_day_name,
    t.track_id,
    t.track_name,
    t.duration_ms,
    t.popularity,
    t.explicit,
    t.track_uri,
    t.artist_id,
    t.artist_name,
    a.genres,
    a.popularity,
    a.followers,
    t.album_id,
    t.album_name,
    t.album_release_date,
    t.album_release_year,
    af.danceability,
    af.energy,
    af.key,
    af.loudness,
    af.mode,
    af.mode_name,
    af.speechiness,
    af.acousticness,
    af.instrumentalness,
    af.liveness,
    af.valence,
    af.tempo,
    af.time_signature,
    af.mood_category,
    t.loaded_at