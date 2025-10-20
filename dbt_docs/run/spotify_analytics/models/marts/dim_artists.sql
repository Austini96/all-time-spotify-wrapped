
  
    
    

    create  table
      "spotify"."marts"."dim_artists__dbt_tmp"
  
    as (
      

WITH artist_stats AS (
    SELECT
        artist_id,
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks_played,
        COUNT(DISTINCT played_date) as days_played,
        MIN(played_at) as first_played_at,
        MAX(played_at) as last_played_at,
        SUM(duration_ms) as total_listen_time_ms
    FROM "spotify"."staging"."stg_spotify_tracks"
    GROUP BY artist_id
)

SELECT
    a.artist_id,
    a.artist_name,
    a.genres,
    a.popularity,
    a.followers,
    
    -- Play statistics
    s.total_plays,
    s.unique_tracks_played,
    s.days_played,
    s.first_played_at,
    s.last_played_at,
    s.total_listen_time_ms,
    ROUND(s.total_listen_time_ms / 3600000.0, 2) as total_listen_time_hours

FROM "spotify"."staging"."stg_spotify_artists" a
LEFT JOIN artist_stats s
    ON a.artist_id = s.artist_id
    );
  
  