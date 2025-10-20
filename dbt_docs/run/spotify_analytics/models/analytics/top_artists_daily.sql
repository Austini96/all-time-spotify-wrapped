
  
    
    

    create  table
      "spotify"."analytics"."top_artists_daily__dbt_tmp"
  
    as (
      

SELECT
    played_date,
    artist_id,
    artist_name,
    artist_genres,
    COUNT(*) as play_count,
    COUNT(DISTINCT track_id) as unique_tracks_played,
    SUM(duration_ms) as total_listen_time_ms,
    ROUND(SUM(duration_ms) / 60000.0, 2) as total_listen_time_minutes,
    AVG(track_popularity) as avg_track_popularity,
    AVG(energy) as avg_energy,
    AVG(valence) as avg_valence,
    AVG(danceability) as avg_danceability,
    MAX(artist_followers) as artist_followers

FROM "spotify"."marts"."fct_listening_history"
GROUP BY 
    played_date,
    artist_id,
    artist_name,
    artist_genres
ORDER BY 
    played_date DESC,
    play_count DESC
    );
  
  