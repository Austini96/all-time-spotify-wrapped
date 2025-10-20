{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source AS (
    SELECT * FROM raw_spotify_audio_features
),

ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) as rn
    FROM source
)

SELECT
    id as track_id,
    danceability,
    energy,
    key,
    loudness,
    mode,
    CASE mode
        WHEN 0 THEN 'Minor'
        WHEN 1 THEN 'Major'
    END as mode_name,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    duration_ms,
    time_signature,
    
    -- Derive additional features
    CASE
        WHEN valence >= 0.7 AND energy >= 0.7 THEN 'Happy & Energetic'
        WHEN valence >= 0.7 AND energy < 0.4 THEN 'Happy & Calm'
        WHEN valence < 0.4 AND energy >= 0.7 THEN 'Sad & Energetic'
        WHEN valence < 0.4 AND energy < 0.4 THEN 'Sad & Calm'
        ELSE 'Neutral'
    END as mood_category,
    
    loaded_at
FROM ranked
WHERE rn = 1