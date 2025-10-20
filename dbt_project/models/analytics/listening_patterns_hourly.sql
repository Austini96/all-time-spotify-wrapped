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
    COUNT(DISTINCT track_id) as unique_tracks,
    COUNT(DISTINCT artist_id) as unique_artists,
    SUM(duration_ms) as total_listen_time_ms,
    ROUND(SUM(duration_ms) / 60000.0, 2) as total_listen_time_minutes,
    
    -- Audio feature averages
    AVG(energy) as avg_energy,
    AVG(valence) as avg_valence,
    AVG(danceability) as avg_danceability,
    AVG(tempo) as avg_tempo,
    AVG(acousticness) as avg_acousticness,
    AVG(instrumentalness) as avg_instrumentalness,
    AVG(speechiness) as avg_speechiness,
    AVG(loudness) as avg_loudness,
    
    -- Most common mood
    MODE() WITHIN GROUP (ORDER BY mood_category) as most_common_mood

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