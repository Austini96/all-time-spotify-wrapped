{{ config(
    materialized='table',
    tags=['marts', 'fact']
) }}

WITH api_plays AS (
    SELECT
        play_id,
        played_at,
        played_date,
        played_year,
        played_month,
        played_day,
        played_hour,
        played_day_of_week,
        played_day_name,
        track_id,
        track_name,
        duration_ms,
        popularity as track_popularity,
        explicit,
        track_uri,
        artist_id,
        artist_name,
        album_id,
        album_name,
        album_release_date,
        album_release_year,
        'api' as data_source,
        NULL as platform,
        NULL as conn_country,
        NULL as shuffle,
        NULL as skipped,
        NULL as reason_start,
        NULL as reason_end,
        loaded_at
    FROM {{ ref('stg_spotify_tracks') }}
),

extended_plays AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY played_at) + 1000000 as play_id,  -- Add offset to avoid conflicts
        played_at,
        played_date,
        EXTRACT(YEAR FROM played_at) as played_year,
        EXTRACT(MONTH FROM played_at) as played_month,
        EXTRACT(DAY FROM played_at) as played_day,
        EXTRACT(HOUR FROM played_at) as played_hour,
        EXTRACT(DOW FROM played_at) as played_day_of_week,
        DAYNAME(played_at) as played_day_name,
        track_id,
        track_name,
        ms_played as duration_ms,
        NULL as track_popularity,
        NULL as explicit,
        spotify_track_uri as track_uri,
        artist_id,
        artist_name,
        album_id,
        album_name,
        NULL as album_release_date,
        NULL as album_release_year,
        'extended_history' as data_source,
        platform,
        conn_country,
        shuffle,
        skipped,
        reason_start,
        reason_end,
        loaded_at
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE track_id IS NOT NULL
),

all_plays AS (
    SELECT * FROM api_plays
    UNION ALL
    SELECT * FROM extended_plays
),

playlist_rankings AS (
    SELECT
        ap.play_id,
        p.playlist_name,
        ROW_NUMBER() OVER (PARTITION BY ap.play_id ORDER BY pt.added_at DESC) as playlist_rank
    FROM all_plays ap
    LEFT JOIN {{ ref('stg_spotify_playlist_tracks') }} pt
        ON ap.track_id = pt.track_id
    LEFT JOIN {{ ref('stg_spotify_playlists') }} p
        ON pt.playlist_id = p.playlist_id
    WHERE p.playlist_name IS NOT NULL
),

plays_with_playlists AS (
    SELECT
        ap.*,
        pr1.playlist_name AS playlist_1,
        pr2.playlist_name AS playlist_2,
        pr3.playlist_name AS playlist_3,
        pr4.playlist_name AS playlist_4,
        pr5.playlist_name AS playlist_5,
        (SELECT COUNT(DISTINCT playlist_name) FROM playlist_rankings pr WHERE pr.play_id = ap.play_id) AS playlist_count
    FROM all_plays ap
    LEFT JOIN playlist_rankings pr1 ON ap.play_id = pr1.play_id AND pr1.playlist_rank = 1
    LEFT JOIN playlist_rankings pr2 ON ap.play_id = pr2.play_id AND pr2.playlist_rank = 2
    LEFT JOIN playlist_rankings pr3 ON ap.play_id = pr3.play_id AND pr3.playlist_rank = 3
    LEFT JOIN playlist_rankings pr4 ON ap.play_id = pr4.play_id AND pr4.playlist_rank = 4
    LEFT JOIN playlist_rankings pr5 ON ap.play_id = pr5.play_id AND pr5.playlist_rank = 5
)

SELECT
    pwp.play_id,
    pwp.played_at,
    pwp.played_at AT TIME ZONE 'America/Chicago' AS played_at_cst,
    pwp.played_date,
    pwp.played_year,
    pwp.played_month,
    pwp.played_day,
    pwp.played_hour AS played_hour_utc,
    EXTRACT(HOUR FROM (pwp.played_at AT TIME ZONE 'America/Chicago')) AS played_hour_cst,
    pwp.played_day_of_week,
    pwp.played_day_name,
    
    -- Track information
    pwp.track_id,
    pwp.track_name,
    pwp.duration_ms,
    pwp.track_popularity,
    pwp.explicit,
    pwp.track_uri,
    
    -- Artist information
    pwp.artist_id,
    pwp.artist_name,
    a.genres as artist_genres,
    a.popularity as artist_popularity,
    a.followers as artist_followers,
    
    -- Album information
    pwp.album_id,
    pwp.album_name,
    pwp.album_release_date,
    pwp.album_release_year,
    
    -- Playlist information (separate columns for up to 5 playlists)
    pwp.playlist_1,
    pwp.playlist_2,
    pwp.playlist_3,
    pwp.playlist_4,
    pwp.playlist_5,
    pwp.playlist_count,
    
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
    
    -- Extended history context
    pwp.data_source,
    pwp.platform,
    pwp.conn_country,
    pwp.shuffle,
    pwp.skipped,
    pwp.reason_start,
    pwp.reason_end,
    
    -- Metadata
    pwp.loaded_at

FROM plays_with_playlists pwp
LEFT JOIN {{ ref('stg_spotify_audio_features') }} af
    ON pwp.track_id = af.track_id
LEFT JOIN {{ ref('stg_spotify_artists') }} a
    ON pwp.artist_id = a.artist_id