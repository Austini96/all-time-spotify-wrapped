{{ config(
    materialized='table',
    tags=['analytics']
) }}

SELECT
    f.played_date,
    f.artist_key,
    da.artist_id,
    da.artist_name,
    da.genres as artist_genres,
    COUNT(*) as play_count,
    COUNT(DISTINCT f.track_key) as unique_tracks_played,
    SUM(f.duration_ms) as total_listen_time_ms,
    ROUND(SUM(f.duration_ms) / 60000.0, 2) as total_listen_time_minutes,
    AVG(f.track_popularity) as avg_track_popularity,
    MAX(da.followers) as artist_followers

FROM {{ ref('fct_listening_history') }} f
LEFT JOIN {{ ref('dim_artists') }} da
    ON f.artist_key = da.artist_key
GROUP BY 
    f.played_date,
    f.artist_key,
    da.artist_id,
    da.artist_name,
    da.genres
ORDER BY 
    f.played_date DESC,
    play_count DESC