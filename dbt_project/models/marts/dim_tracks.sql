{{ config(
    materialized='table',
    tags=['marts', 'dimension']
) }}

WITH api_plays AS (
    SELECT
        track_id,
        played_at,
        played_date
    FROM {{ ref('stg_spotify_tracks') }}
),

extended_plays AS (
    SELECT
        track_id,
        played_at,
        played_date
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE track_id IS NOT NULL
),

all_plays AS (
    SELECT * FROM api_plays
    UNION ALL
    SELECT * FROM extended_plays
),

track_stats AS (
    SELECT
        track_id,
        COUNT(*) as total_plays,
        MIN(played_at) as first_played_at,
        MAX(played_at) as last_played_at,
        COUNT(DISTINCT played_date) as days_played
    FROM all_plays
    GROUP BY track_id
),

track_base AS (
    SELECT
        track_id,
        track_name,
        artist_id,
        artist_name,
        album_id,
        album_name,
        album_release_date,
        album_release_year,
        duration_ms,
        popularity,
        explicit,
        loaded_at
    FROM {{ ref('stg_spotify_tracks') }}
    
    UNION
    
    SELECT
        track_id,
        track_name,
        artist_id,
        artist_name,
        album_id,
        album_name,
        NULL as album_release_date,
        NULL as album_release_year,
        ms_played as duration_ms,
        NULL as popularity,
        NULL as explicit,
        loaded_at
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE track_id IS NOT NULL
)

SELECT
    t.track_id,
    t.track_name,
    t.artist_id,
    t.artist_name,
    t.album_id,
    t.album_name,
    t.album_release_date,
    t.album_release_year,
    t.duration_ms,
    t.popularity,
    t.explicit,
    -- Audio features
    af.danceability,
    af.energy,
    af.valence,
    af.tempo,
    af.acousticness,
    af.instrumentalness,
    af.speechiness,
    af.liveness,
    af.loudness,
    af.mode_name,
    af.mood_category,
    -- Play statistics (from both API and extended history)
    COALESCE(ts.total_plays, 0) as total_plays,
    ts.first_played_at,
    ts.last_played_at,
    COALESCE(ts.days_played, 0) as days_played
FROM track_base t
LEFT JOIN {{ ref('stg_spotify_audio_features') }} af
    ON t.track_id = af.track_id
LEFT JOIN track_stats ts
    ON t.track_id = ts.track_id

-- Get most recent version of each track
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.track_id ORDER BY t.loaded_at DESC) = 1