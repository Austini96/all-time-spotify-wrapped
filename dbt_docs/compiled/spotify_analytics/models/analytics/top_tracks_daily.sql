

SELECT
    played_date,
    track_id,
    track_name,
    artist_name,
    album_name,
    COUNT(*) as play_count,
    MAX(track_popularity) as track_popularity,
    MAX(duration_ms) as duration_ms,
    MAX(explicit) as explicit,
    AVG(energy) as avg_energy,
    AVG(valence) as avg_valence,
    AVG(danceability) as avg_danceability,
    MAX(mood_category) as mood_category

FROM "spotify"."marts"."fct_listening_history"
GROUP BY 
    played_date,
    track_id,
    track_name,
    artist_name,
    album_name
ORDER BY 
    played_date DESC,
    play_count DESC