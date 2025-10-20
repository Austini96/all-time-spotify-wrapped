

SELECT
    played_date,
    
    -- Mood distribution
    COUNT(CASE WHEN mood_category = 'Happy & Energetic' THEN 1 END) as happy_energetic_count,
    COUNT(CASE WHEN mood_category = 'Happy & Calm' THEN 1 END) as happy_calm_count,
    COUNT(CASE WHEN mood_category = 'Sad & Energetic' THEN 1 END) as sad_energetic_count,
    COUNT(CASE WHEN mood_category = 'Sad & Calm' THEN 1 END) as sad_calm_count,
    COUNT(CASE WHEN mood_category = 'Neutral' THEN 1 END) as neutral_count,
    
    -- Audio feature statistics
    AVG(danceability) as avg_danceability,
    AVG(energy) as avg_energy,
    AVG(valence) as avg_valence,
    AVG(tempo) as avg_tempo,
    AVG(acousticness) as avg_acousticness,
    AVG(instrumentalness) as avg_instrumentalness,
    AVG(speechiness) as avg_speechiness,
    AVG(liveness) as avg_liveness,
    AVG(loudness) as avg_loudness,
    
    -- Feature ranges
    MIN(energy) as min_energy,
    MAX(energy) as max_energy,
    MIN(valence) as min_valence,
    MAX(valence) as max_valence,
    MIN(tempo) as min_tempo,
    MAX(tempo) as max_tempo,
    
    -- Mode distribution
    COUNT(CASE WHEN mode_name = 'Major' THEN 1 END) as major_key_count,
    COUNT(CASE WHEN mode_name = 'Minor' THEN 1 END) as minor_key_count,
    
    -- Explicit content
    COUNT(CASE WHEN explicit THEN 1 END) as explicit_tracks_count,
    ROUND(100.0 * COUNT(CASE WHEN explicit THEN 1 END) / COUNT(*), 2) as explicit_percentage

FROM "spotify"."marts"."fct_listening_history"
GROUP BY played_date
ORDER BY played_date DESC