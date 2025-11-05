{{ config(
    materialized='table',
    tags=['analytics']
) }}

WITH session_stats AS (
    SELECT
        played_date,
        session_number,
        COUNT(*) as songs_in_session,
        SUM(duration_ms) as session_duration_ms,
        ROUND(SUM(duration_ms) / 60000.0, 2) as session_duration_minutes,
        MIN(played_at) as session_start,
        MAX(played_at) as session_end,
        EXTRACT(EPOCH FROM (MAX(played_at) - MIN(played_at))) / 60.0 as session_length_minutes
    FROM {{ ref('fct_listening_history') }}
    GROUP BY
        played_date,
        session_number
),

daily_consecutive_stats AS (
    SELECT
        played_date,
        -- Total consecutive sessions
        COUNT(DISTINCT session_number) as total_sessions,
        
        -- Songs consecutively
        SUM(songs_in_session) as total_consecutive_songs,
        MAX(songs_in_session) as max_consecutive_songs,
        ROUND(AVG(songs_in_session), 2) as avg_consecutive_songs_per_session,
        
        -- Time consecutively
        SUM(session_duration_ms) as total_consecutive_time_ms,
        ROUND(SUM(session_duration_minutes), 2) as total_consecutive_time_minutes,
        ROUND(MAX(session_duration_minutes), 2) as max_consecutive_time_minutes,
        ROUND(AVG(session_duration_minutes), 2) as avg_consecutive_time_minutes,
        ROUND(MAX(session_length_minutes), 2) as longest_session_length_minutes,
        
        -- Session timing
        MIN(session_start) as first_session_start,
        MAX(session_end) as last_session_end
    FROM session_stats
    GROUP BY played_date
)

SELECT
    played_date,
    total_sessions,
    
    -- Songs metrics
    total_consecutive_songs,
    max_consecutive_songs,
    avg_consecutive_songs_per_session,
    
    -- Time metrics (all in minutes for readability)
    total_consecutive_time_minutes,
    max_consecutive_time_minutes,
    avg_consecutive_time_minutes,
    longest_session_length_minutes,
    
    -- Additional metrics
    ROUND(total_consecutive_time_minutes / NULLIF(total_sessions, 0), 2) as avg_time_per_session_minutes,
    
    -- Timing
    first_session_start,
    last_session_end,
    
    CURRENT_TIMESTAMP AS analyzed_at
FROM daily_consecutive_stats
ORDER BY played_date DESC

