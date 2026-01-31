-- Test to ensure extended history artist/album IDs follow expected patterns
-- IDs should either be real Spotify IDs (22 chars alphanumeric) or ext_ prefixed hashes
SELECT
    artist_id,
    album_id
FROM {{ ref('stg_spotify_extended_history') }}
WHERE
    -- Artist ID should be either real Spotify ID or ext_artist_ prefixed
    (artist_id NOT LIKE 'ext_artist_%' AND LENGTH(artist_id) != 22)
    OR
    -- Album ID should be either real Spotify ID or ext_album_ prefixed
    (album_id NOT LIKE 'ext_album_%' AND LENGTH(album_id) != 22)
LIMIT 10
