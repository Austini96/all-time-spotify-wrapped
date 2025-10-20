{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source AS (
    SELECT * FROM raw_spotify_tracks
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_id, played_at 
            ORDER BY loaded_at DESC
        ) as rn
    FROM source
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['track_id', 'played_at']) }} as play_id,
    played_at,
    track_id,
    track_name,
    artist_id,
    artist_name,
    album_id,
    album_name,
    album_release_date,
    CAST(SUBSTRING(album_release_date, 1, 4) AS INTEGER) as album_release_year,
    duration_ms,
    popularity,
    explicit,
    track_uri,
    CAST(played_at AS DATE) as played_date,
    EXTRACT(YEAR FROM played_at) as played_year,
    EXTRACT(MONTH FROM played_at) as played_month,
    EXTRACT(DAY FROM played_at) as played_day,
    EXTRACT(HOUR FROM played_at) as played_hour,
    EXTRACT(DOW FROM played_at) as played_day_of_week,
    CASE EXTRACT(DOW FROM played_at)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as played_day_name,
    loaded_at
FROM deduplicated
WHERE rn = 1
    AND duration_ms >= {{ var('min_play_duration_ms') }}