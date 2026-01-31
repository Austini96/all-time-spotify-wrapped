-- Test to ensure no duplicate plays exist (same track at same timestamp)
-- This validates the deduplication logic in staging models
SELECT
    track_key,
    played_at,
    COUNT(*) AS play_count
FROM {{ ref('fct_listening_history') }}
GROUP BY track_key, played_at
HAVING COUNT(*) > 1
