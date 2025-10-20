{{ config(
    materialized='view',
    tags=['staging', 'extended_history']
) }}

WITH source AS (
    SELECT *
    FROM {{ source('spotify', 'raw_spotify_extended_history') }}
),

parsed AS (
    SELECT
        ts AS played_at,
        CAST(ts AS DATE) AS played_date,
        platform,
        ms_played,
        conn_country,
        
        -- Extract track ID from URI (spotify:track:TRACK_ID)
        CASE 
            WHEN spotify_track_uri LIKE 'spotify:track:%' 
            THEN SUBSTRING(spotify_track_uri, 15)  -- Skip "spotify:track:"
            ELSE NULL
        END AS track_id,
        
        master_metadata_track_name AS track_name,
        master_metadata_album_artist_name AS artist_name,
        
        -- Generate artist ID from artist name (placeholder, will be enriched with API data)
        -- This is a hash to create a deterministic ID
        'artist_' || SUBSTR(MD5(COALESCE(master_metadata_album_artist_name, 'unknown')), 1, 22) AS artist_id,
        
        master_metadata_album_album_name AS album_name,
        
        -- Generate album ID from album name (placeholder, will be enriched with API data)
        'album_' || SUBSTR(MD5(COALESCE(master_metadata_album_album_name, 'unknown')), 1, 22) AS album_id,
        
        spotify_track_uri,
        reason_start,
        reason_end,
        shuffle,
        skipped,
        offline,
        incognito_mode,
        loaded_at
    FROM source
    WHERE spotify_track_uri IS NOT NULL
        AND spotify_track_uri LIKE 'spotify:track:%'
)

SELECT
    played_at,
    played_date,
    platform,
    ms_played,
    conn_country,
    track_id,
    track_name,
    artist_id,
    artist_name,
    album_id,
    album_name,
    spotify_track_uri,
    reason_start,
    reason_end,
    shuffle,
    skipped,
    offline,
    incognito_mode,
    loaded_at
FROM parsed
