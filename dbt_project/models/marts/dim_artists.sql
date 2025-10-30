{{ config(
    materialized='table',
    tags=['marts', 'dimension']
) }}

WITH api_plays AS (
    SELECT
        artist_id,
        track_id,
        played_at,
        played_date,
        duration_ms
    FROM {{ ref('stg_spotify_tracks') }}
),

extended_plays AS (
    SELECT
        artist_id,
        track_id,
        played_at,
        played_date,
        ms_played as duration_ms
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE artist_id IS NOT NULL
),

all_plays AS (
    SELECT * FROM api_plays
    UNION ALL
    SELECT * FROM extended_plays
),

artist_stats AS (
    SELECT
        artist_id,
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks_played,
        COUNT(DISTINCT played_date) as days_played,
        MIN(played_at) as first_played_at,
        MAX(played_at) as last_played_at,
        SUM(duration_ms) as total_listen_time_ms
    FROM all_plays
    GROUP BY artist_id
),

all_artists AS (
    -- API artists (with full metadata)
    SELECT
        artist_id,
        artist_name,
        genres,
        popularity,
        followers
    FROM {{ ref('stg_spotify_artists') }}
    
    UNION
    
    -- Extended history artists (name only, from play history)
    SELECT DISTINCT
        artist_id,
        artist_name,
        NULL as genres,
        NULL as popularity,
        NULL as followers
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE artist_id IS NOT NULL
),

final_artists AS (
SELECT
    a.artist_id,
    a.artist_name,
    a.genres,
    a.popularity,
    a.followers,
    -- Play statistics (from both API and extended history)
    COALESCE(s.total_plays, 0) as total_plays,
    COALESCE(s.unique_tracks_played, 0) as unique_tracks_played,
    COALESCE(s.days_played, 0) as days_played,
    s.first_played_at,
    s.last_played_at,
    COALESCE(s.total_listen_time_ms, 0) as total_listen_time_ms,
    ROUND(COALESCE(s.total_listen_time_ms, 0) / 3600000.0, 2) as total_listen_time_hours

FROM all_artists a
LEFT JOIN artist_stats s ON a.artist_id = s.artist_id
-- Get the best version of each artist (prefer API data over extended history)
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.artist_id ORDER BY CASE WHEN a.genres IS NOT NULL THEN 0 ELSE 1 END) = 1
)

SELECT
    ROW_NUMBER() OVER (ORDER BY artist_id) AS artist_key,
    artist_id,
    artist_name,
    genres,
    popularity,
    followers,
    total_plays,
    unique_tracks_played,
    days_played,
    first_played_at,
    last_played_at,
    total_listen_time_hours
FROM final_artists