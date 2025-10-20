{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source AS (
    SELECT * FROM raw_spotify_artists
),

ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY loaded_at DESC) as rn
    FROM source
)

SELECT
    artist_id,
    artist_name,
    genres,
    popularity,
    followers,
    loaded_at
FROM ranked
WHERE rn = 1