{{ config(
    materialized='table',
    tags=['marts', 'fact']
) }}

WITH api_plays AS (
    SELECT
        play_id,
        played_at,
        played_at - INTERVAL 6 HOURS AS played_at_cst,
        played_date,
        played_year,
        played_month,
        played_day,
        played_hour,
        played_day_of_week,
        played_day_name,
        track_id,
        track_name,
        duration_ms,
        popularity as track_popularity,
        explicit,
        track_uri,
        artist_id,
        artist_name,
        album_id,
        album_name,
        album_release_date,
        album_release_year,
        'api' as data_source,
        NULL as platform,
        NULL as conn_country,
        NULL as shuffle,
        NULL as skipped,
        NULL as reason_start,
        NULL as reason_end,
        loaded_at
    FROM {{ ref('stg_spotify_tracks') }}
),

extended_plays AS (
    SELECT
        -- Generate surrogate key from track_id + played_at to avoid collision with API play_ids
        {{ dbt_utils.generate_surrogate_key(['track_id', 'played_at']) }} as play_id,
        played_at,
        played_at - INTERVAL 6 HOURS AS played_at_cst,
        played_date,
        EXTRACT(YEAR FROM played_at) as played_year,
        EXTRACT(MONTH FROM played_at) as played_month,
        EXTRACT(DAY FROM played_at) as played_day,
        EXTRACT(HOUR FROM played_at) as played_hour,
        EXTRACT(DOW FROM played_at) as played_day_of_week,
        DAYNAME(played_at) as played_day_name,
        track_id,
        track_name,
        ms_played as duration_ms,
        NULL as track_popularity,
        NULL as explicit,
        spotify_track_uri as track_uri,
        artist_id,
        artist_name,
        album_id,
        album_name,
        NULL as album_release_date,
        NULL as album_release_year,
        'extended_history' as data_source,
        platform,
        conn_country,
        shuffle,
        skipped,
        reason_start,
        reason_end,
        loaded_at
    FROM {{ ref('stg_spotify_extended_history') }}
    WHERE track_id IS NOT NULL
),

all_plays AS (
    SELECT * FROM api_plays
    UNION ALL
    SELECT * FROM extended_plays
),

-- Rank playlists for each play and pivot to columns in a single pass
playlist_rankings AS (
    SELECT
        ap.play_id,
        dp.playlist_key,
        ROW_NUMBER() OVER (PARTITION BY ap.play_id ORDER BY pt.added_at DESC) as playlist_rank
    FROM all_plays ap
    INNER JOIN {{ ref('stg_spotify_playlist_tracks') }} pt
        ON ap.track_id = pt.track_id
    INNER JOIN {{ ref('dim_playlists') }} dp
        ON pt.playlist_id = dp.playlist_id
),

-- Aggregate playlist keys into a single row per play using conditional aggregation
-- This replaces 5 separate JOINs + correlated subquery with 1 GROUP BY
playlist_pivot AS (
    SELECT
        play_id,
        MAX(CASE WHEN playlist_rank = 1 THEN playlist_key END) AS playlist_key_1,
        MAX(CASE WHEN playlist_rank = 2 THEN playlist_key END) AS playlist_key_2,
        MAX(CASE WHEN playlist_rank = 3 THEN playlist_key END) AS playlist_key_3,
        MAX(CASE WHEN playlist_rank = 4 THEN playlist_key END) AS playlist_key_4,
        MAX(CASE WHEN playlist_rank = 5 THEN playlist_key END) AS playlist_key_5,
        COUNT(DISTINCT playlist_key) AS playlist_count
    FROM playlist_rankings
    GROUP BY play_id
),

plays_with_playlists AS (
    SELECT
        ap.*,
        pp.playlist_key_1,
        pp.playlist_key_2,
        pp.playlist_key_3,
        pp.playlist_key_4,
        pp.playlist_key_5,
        COALESCE(pp.playlist_count, 0) AS playlist_count
    FROM all_plays ap
    LEFT JOIN playlist_pivot pp ON ap.play_id = pp.play_id
),

plays_with_dimension_keys AS (
    SELECT
        pwp.*,
        dt.track_key,
        da.artist_key,
        dal.album_key
    FROM plays_with_playlists pwp
    LEFT JOIN {{ ref('dim_tracks') }} dt
        ON pwp.track_id = dt.track_id
    LEFT JOIN {{ ref('dim_artists') }} da
        ON pwp.artist_id = da.artist_id
    LEFT JOIN {{ ref('dim_albums') }} dal
        ON pwp.album_id = dal.album_id
),

plays_ordered AS (
    SELECT
        *,
        -- Previous play information (for consecutive tracking) - using keys instead of IDs
        LAG(played_at) OVER (ORDER BY played_at) AS prev_played_at,
        LAG(played_date) OVER (ORDER BY played_at) AS prev_played_date,
        LAG(track_key) OVER (ORDER BY played_at) AS prev_track_key,
        LAG(artist_key) OVER (ORDER BY played_at) AS prev_artist_key,
        -- Time gap between consecutive plays (in minutes)
        EXTRACT(EPOCH FROM (played_at - LAG(played_at) OVER (ORDER BY played_at))) / 60.0 AS minutes_since_prev_play,
        -- Session logic: new session if gap > 30 minutes or new day
        CASE 
            WHEN LAG(played_at) OVER (ORDER BY played_at) IS NULL THEN 1
            WHEN EXTRACT(EPOCH FROM (played_at - LAG(played_at) OVER (ORDER BY played_at))) / 60.0 > 30 THEN 1
            WHEN played_date != LAG(played_date) OVER (ORDER BY played_at) THEN 1
            ELSE 0
        END AS is_new_session,
        -- Repeat tracking: is this the same track as the previous play?
        CASE 
            WHEN LAG(track_id) OVER (ORDER BY played_at) IS NULL THEN FALSE
            WHEN track_id = LAG(track_id) OVER (ORDER BY played_at) THEN TRUE
            ELSE FALSE
        END AS is_repeat,
        -- Flag when a new track group starts (used for repeat sequence calculation)
        CASE 
            WHEN LAG(track_id) OVER (ORDER BY played_at) IS NULL THEN 1
            WHEN track_id != LAG(track_id) OVER (ORDER BY played_at) THEN 1
            ELSE 0
        END AS is_new_track_group
    FROM plays_with_dimension_keys
),

plays_with_sessions AS (
    SELECT
        *,
        -- Session number (increments when is_new_session = 1)
        SUM(is_new_session) OVER (ORDER BY played_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_number,
        -- Track group number (increments when track changes)
        SUM(is_new_track_group) OVER (ORDER BY played_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS track_group_number
    FROM plays_ordered
),

plays_with_repeat_sequence AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_group_number 
            ORDER BY played_at
        ) - 1 AS repeat_sequence
    FROM plays_with_sessions
)

SELECT
    pws.play_id,
    pws.played_at,
    pws.played_at_cst,
    pws.played_date,
    pws.played_year,
    pws.played_month,
    pws.played_day,
    pws.played_hour AS played_hour_utc,
    EXTRACT(HOUR FROM pws.played_at_cst) AS played_hour_cst,
    pws.played_day_of_week,
    pws.played_day_name,
    DAYNAME(pws.played_at_cst) AS played_day_name_cst,
    
    -- Dimension foreign keys (star schema design)
    pws.track_key,
    pws.artist_key,
    pws.album_key,
    pws.playlist_key_1,
    pws.playlist_key_2,
    pws.playlist_key_3,
    pws.playlist_key_4,
    pws.playlist_key_5,
    pws.playlist_count,
    
    -- Track-level metrics (not stored in dimension)
    pws.duration_ms,
    pws.track_popularity,
    pws.explicit,
    pws.track_uri,
    
    -- Consecutive listening information
    pws.prev_track_key,
    pws.prev_artist_key,
    pws.minutes_since_prev_play,
    pws.is_new_session,
    pws.session_number,
    
    -- Repeat tracking
    pws.is_repeat,
    pws.repeat_sequence,
    
    -- Extended history context
    pws.data_source,
    pws.platform,
    pws.conn_country,
    pws.shuffle,
    pws.skipped,
    pws.reason_start,
    pws.reason_end,
    
    -- Metadata
    pws.loaded_at

FROM plays_with_repeat_sequence pws