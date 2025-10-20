-- Test to ensure all daily aggregations have positive play counts
SELECT
    played_date,
    play_count
FROM {{ ref('top_artists_daily') }}
WHERE play_count <= 0