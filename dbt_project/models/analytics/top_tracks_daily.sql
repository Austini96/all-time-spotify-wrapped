{{ config(
    materialized='table',
    tags=['analytics']
) }}

SELECT
    f.played_date,
    f.track_key,
    dt.track_id,
    dt.track_name,
    dt.artist_name,
    dt.album_name,
    COUNT(*) as play_count,
    MAX(f.track_popularity) as track_popularity,
    MAX(f.duration_ms) as duration_ms,
    MAX(f.explicit) as explicit

FROM {{ ref('fct_listening_history') }} f
LEFT JOIN {{ ref('dim_tracks') }} dt
    ON f.track_key = dt.track_key
GROUP BY 
    f.played_date,
    f.track_key,
    dt.track_id,
    dt.track_name,
    dt.artist_name,
    dt.album_name
ORDER BY 
    f.played_date DESC,
    play_count DESC