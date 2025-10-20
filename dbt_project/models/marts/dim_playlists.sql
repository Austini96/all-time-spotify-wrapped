-- Playlist dimension table with slowly changing dimensions (Type 1)
WITH playlists AS (
    SELECT *
    FROM {{ ref('stg_spotify_playlists') }}
),

playlist_stats AS (
    -- Calculate actual track counts from playlist_tracks
    SELECT
        playlist_id,
        COUNT(DISTINCT track_id) AS actual_track_count,
        MIN(added_at) AS first_track_added,
        MAX(added_at) AS last_track_added
    FROM {{ ref('stg_spotify_playlist_tracks') }}
    GROUP BY playlist_id
),

final AS (
    SELECT
        -- Surrogate key
        MD5(p.playlist_id) AS playlist_sk,
        
        -- Natural key
        p.playlist_id,
        
        -- Attributes
        p.playlist_name,
        p.owner_id,
        p.is_owner,
        p.is_public,
        p.is_collaborative,
        p.total_tracks AS reported_track_count,
        COALESCE(ps.actual_track_count, 0) AS actual_track_count,
        p.description,
        ps.first_track_added,
        ps.last_track_added,
        
        -- Metadata
        p.snapshot_id,
        p.extracted_at,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM playlists p
    LEFT JOIN playlist_stats ps ON p.playlist_id = ps.playlist_id
)

SELECT * FROM final

