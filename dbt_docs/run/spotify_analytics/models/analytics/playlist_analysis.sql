
  
    
    

    create  table
      "spotify"."analytics"."playlist_analysis__dbt_tmp"
  
    as (
      

-- Playlist analysis with track listening statistics
WITH playlist_tracks AS (
    SELECT
        p.playlist_id,
        p.playlist_name,
        p.is_owner,
        p.is_public,
        p.is_collaborative,
        pt.track_id,
        pt.added_at,
        pt.position
    FROM "spotify"."marts"."dim_playlists" p
    INNER JOIN "spotify"."staging"."stg_spotify_playlist_tracks" pt
        ON p.playlist_id = pt.playlist_id
),

track_plays AS (
    SELECT
        track_id,
        COUNT(*) AS play_count,
        MIN(played_at) AS first_played,
        MAX(played_at) AS last_played
    FROM "spotify"."staging"."stg_spotify_tracks"
    GROUP BY track_id
),

track_details AS (
    SELECT
        t.track_id,
        t.track_name,
        t.artist_name,
        t.album_name,
        t.duration_ms,
        af.valence,
        af.energy,
        af.danceability,
        af.mood_category
    FROM "spotify"."staging"."stg_spotify_tracks" t
    LEFT JOIN "spotify"."staging"."stg_spotify_audio_features" af
        ON t.track_id = af.track_id
),

playlist_stats AS (
    SELECT
        pt.playlist_id,
        pt.playlist_name,
        pt.is_owner,
        pt.is_public,
        pt.is_collaborative,
        
        -- Track counts
        COUNT(DISTINCT pt.track_id) AS total_tracks,
        COUNT(DISTINCT CASE WHEN tp.play_count > 0 THEN pt.track_id END) AS played_tracks,
        COUNT(DISTINCT CASE WHEN tp.play_count IS NULL THEN pt.track_id END) AS unplayed_tracks,
        
        -- Play statistics
        SUM(COALESCE(tp.play_count, 0)) AS total_plays,
        ROUND(AVG(COALESCE(tp.play_count, 0)), 2) AS avg_plays_per_track,
        MAX(tp.play_count) AS max_plays,
        
        -- Audio features averages
        ROUND(AVG(td.valence), 3) AS avg_valence,
        ROUND(AVG(td.energy), 3) AS avg_energy,
        ROUND(AVG(td.danceability), 3) AS avg_danceability,
        
        -- Timing
        MIN(pt.added_at) AS first_track_added,
        MAX(pt.added_at) AS last_track_added,
        MIN(tp.first_played) AS first_play_date,
        MAX(tp.last_played) AS last_play_date,
        
        -- Duration
        ROUND(SUM(td.duration_ms) / 1000.0 / 60.0, 2) AS total_duration_minutes,
        ROUND(AVG(td.duration_ms) / 1000.0 / 60.0, 2) AS avg_track_duration_minutes
        
    FROM playlist_tracks pt
    LEFT JOIN track_plays tp ON pt.track_id = tp.track_id
    LEFT JOIN track_details td ON pt.track_id = td.track_id
    GROUP BY
        pt.playlist_id,
        pt.playlist_name,
        pt.is_owner,
        pt.is_public,
        pt.is_collaborative
)

SELECT
    playlist_id,
    playlist_name,
    is_owner,
    is_public,
    is_collaborative,
    total_tracks,
    played_tracks,
    unplayed_tracks,
    ROUND(100.0 * played_tracks / NULLIF(total_tracks, 0), 1) AS played_percentage,
    total_plays,
    avg_plays_per_track,
    max_plays,
    avg_valence,
    avg_energy,
    avg_danceability,
    CASE
        WHEN avg_valence >= 0.6 AND avg_energy >= 0.6 THEN 'Happy & Energetic'
        WHEN avg_valence >= 0.6 AND avg_energy < 0.6 THEN 'Happy & Calm'
        WHEN avg_valence < 0.4 AND avg_energy >= 0.6 THEN 'Sad & Energetic'
        WHEN avg_valence < 0.4 AND avg_energy < 0.6 THEN 'Sad & Calm'
        ELSE 'Neutral'
    END AS playlist_mood,
    total_duration_minutes,
    avg_track_duration_minutes,
    first_track_added,
    last_track_added,
    first_play_date,
    last_play_date,
    CURRENT_TIMESTAMP AS analyzed_at
FROM playlist_stats
ORDER BY total_plays DESC, total_tracks DESC
    );
  
  