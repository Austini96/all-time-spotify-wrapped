{{ config(
    materialized='table',
    tags=['analytics']
) }}

SELECT
    played_date,
    played_hour_cst,
    played_hour_utc,
    played_day_of_week,
    played_day_name,
    
    -- Volume metrics
    COUNT(*) as play_count,
    COUNT(DISTINCT track_key) as unique_tracks,
    COUNT(DISTINCT artist_key) as unique_artists,
    SUM(duration_ms) as total_listen_time_ms,
    ROUND(SUM(duration_ms) / 60000.0, 2) as total_listen_time_minutes

FROM {{ ref('fct_listening_history') }}
GROUP BY 
    played_date,
    played_hour_cst,
    played_hour_utc,
    played_day_of_week,
    played_day_name
ORDER BY 
    played_date DESC,
    played_hour_cst