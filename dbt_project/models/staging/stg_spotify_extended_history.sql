{{ config(
    materialized='view',
    tags=['staging', 'extended_history']
) }}

WITH source AS (
    SELECT *
    FROM {{ source('spotify', 'raw_spotify_extended_history') }}
),

-- Lookup table: track_id -> real artist_id, album_id from API data
track_metadata_lookup AS (
    SELECT DISTINCT
        track_id,
        FIRST_VALUE(artist_id) OVER (PARTITION BY track_id ORDER BY loaded_at DESC) AS api_artist_id,
        FIRST_VALUE(album_id) OVER (PARTITION BY track_id ORDER BY loaded_at DESC) AS api_album_id
    FROM raw_spotify_tracks
    QUALIFY ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY loaded_at DESC) = 1
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
        master_metadata_album_album_name AS album_name,

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
),

enriched AS (
    SELECT
        p.played_at,
        p.played_date,
        p.platform,
        p.ms_played,
        p.conn_country,
        p.track_id,
        p.track_name,
        p.artist_name,
        p.album_name,
        p.spotify_track_uri,
        p.reason_start,
        p.reason_end,
        p.shuffle,
        p.skipped,
        p.offline,
        p.incognito_mode,
        p.loaded_at,
        -- Use real Spotify IDs from API if available, otherwise generate hash-based fallback
        COALESCE(
            tm.api_artist_id,
            'ext_artist_' || SUBSTR(MD5(COALESCE(p.artist_name, 'unknown')), 1, 18)
        ) AS artist_id,
        COALESCE(
            tm.api_album_id,
            'ext_album_' || SUBSTR(MD5(COALESCE(p.album_name, 'unknown')), 1, 19)
        ) AS album_id,
        -- Deduplicate: keep most recently loaded record per track+timestamp
        ROW_NUMBER() OVER (
            PARTITION BY p.track_id, p.played_at
            ORDER BY p.loaded_at DESC
        ) AS row_num
    FROM parsed p
    LEFT JOIN track_metadata_lookup tm ON p.track_id = tm.track_id
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
FROM enriched
WHERE row_num = 1
