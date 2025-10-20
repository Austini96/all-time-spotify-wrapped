
  
    
    

    create  table
      "spotify"."marts"."dim_albums__dbt_tmp"
  
    as (
      

WITH album_stats AS (
    SELECT
        album_id,
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks_played,
        COUNT(DISTINCT played_date) as days_played,
        MIN(played_at) as first_played_at,
        MAX(played_at) as last_played_at
    FROM "spotify"."staging"."stg_spotify_tracks"
    GROUP BY album_id
)

SELECT
    t.album_id,
    t.album_name,
    t.artist_id,
    t.artist_name,
    t.album_release_date,
    t.album_release_year,
    
    -- Album statistics
    s.total_plays,
    s.unique_tracks_played,
    s.days_played,
    s.first_played_at,
    s.last_played_at

FROM "spotify"."staging"."stg_spotify_tracks" t
LEFT JOIN album_stats s
    ON t.album_id = s.album_id

-- Get most recent version of each album
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.album_id ORDER BY t.loaded_at DESC) = 1
    );
  
  