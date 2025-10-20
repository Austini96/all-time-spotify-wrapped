{{ config(
    materialized='table',
    tags=['marts', 'dimension']
) }}

WITH api_plays AS (
    SELECT
        album_id,
        track_id,
        played_at,
        played_date
    FROM {{ ref('stg_spotify_tracks') }}
),

extended_plays AS (
    SELECT
        album_id,
        track_id,
        played_at,
        played_date
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE album_id IS NOT NULL
),

all_plays AS (
    SELECT * FROM api_plays
    UNION ALL
    SELECT * FROM extended_plays
),

album_stats AS (
    SELECT
        album_id,
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks_played,
        COUNT(DISTINCT played_date) as days_played,
        MIN(played_at) as first_played_at,
        MAX(played_at) as last_played_at
    FROM all_plays
    GROUP BY album_id
),

album_base AS (
    SELECT
        album_id,
        album_name,
        artist_id,
        artist_name,
        loaded_at
    FROM {{ ref('stg_spotify_tracks') }}
    
    UNION
    
    SELECT
        album_id,
        album_name,
        artist_id,
        artist_name,
        loaded_at
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE album_id IS NOT NULL
)

SELECT
    t.album_id,
    t.album_name,
    t.artist_id,
    t.artist_name,
    -- Album statistics (from both API and extended history)
    COALESCE(s.total_plays, 0) as total_plays,
    COALESCE(s.unique_tracks_played, 0) as unique_tracks_played,
    COALESCE(s.days_played, 0) as days_played,
    s.first_played_at,
    s.last_played_at

FROM album_base t
LEFT JOIN album_stats s ON t.album_id = s.album_id

-- Get most recent version of each album
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.album_id ORDER BY t.loaded_at DESC) = 1