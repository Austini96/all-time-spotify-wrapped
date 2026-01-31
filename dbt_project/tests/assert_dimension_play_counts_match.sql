-- Test to ensure fact table play counts match dimension table aggregated totals
-- This validates referential integrity and aggregation logic consistency
WITH fact_artist_counts AS (
    SELECT
        artist_key,
        COUNT(*) AS fact_play_count
    FROM {{ ref('fct_listening_history') }}
    GROUP BY artist_key
),
dim_artist_counts AS (
    SELECT
        artist_key,
        total_plays AS dim_play_count
    FROM {{ ref('dim_artists') }}
)
SELECT
    f.artist_key,
    f.fact_play_count,
    d.dim_play_count,
    ABS(f.fact_play_count - d.dim_play_count) AS difference
FROM fact_artist_counts f
JOIN dim_artist_counts d ON f.artist_key = d.artist_key
WHERE f.fact_play_count != d.dim_play_count
