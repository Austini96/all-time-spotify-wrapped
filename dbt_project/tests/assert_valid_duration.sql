-- Test to ensure all tracks have valid duration (non-negative, reasonable values)
-- duration_ms = 0 is valid for skipped tracks in extended history
-- Flags tracks with negative duration or more than 1 hour
SELECT
    play_id,
    track_key,
    duration_ms
FROM {{ ref('fct_listening_history') }}
WHERE duration_ms < 0
   OR duration_ms > 3600000  -- 1 hour in milliseconds
