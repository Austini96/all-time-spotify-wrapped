
  
    
    

    create  table
      "spotify"."marts"."dim_tracks__dbt_tmp"
  
    as (
      

WITH track_stats AS (
    SELECT
        track_id,
        COUNT(*) as total_plays,
        MIN(played_at) as first_played_at,
        MAX(played_at) as last_played_at,
        COUNT(DISTINCT played_date) as days_played
    FROM "spotify"."staging"."stg_spotify_tracks"
    GROUP BY track_id
)

SELECT
    t.track_id,
    t.track_name,
    t.artist_id,
    t.artist_name,
    t.album_id,
    t.album_name,
    t.album_release_date,
    t.album_release_year,
    t.duration_ms,
    t.popularity,
    t.explicit,
    
    -- Audio features
    af.danceability,
    af.energy,
    af.valence,
    af.tempo,
    af.acousticness,
    af.instrumentalness,
    af.speechiness,
    af.liveness,
    af.loudness,
    af.mode_name,
    af.mood_category,
    
    -- Play statistics
    ts.total_plays,
    ts.first_played_at,
    ts.last_played_at,
    ts.days_played

FROM "spotify"."staging"."stg_spotify_tracks" t
LEFT JOIN "spotify"."staging"."stg_spotify_audio_features" af
    ON t.track_id = af.track_id
LEFT JOIN track_stats ts
    ON t.track_id = ts.track_id

-- Get most recent version of each track
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.track_id ORDER BY t.loaded_at DESC) = 1
    );
  
  