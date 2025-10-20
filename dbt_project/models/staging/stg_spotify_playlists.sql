-- Staging model for Spotify playlists
WITH source AS (
    SELECT *
    FROM raw_spotify_playlists
),

renamed AS (
    SELECT
        playlist_id,
        playlist_name,
        owner_id,
        is_owner,
        is_public,
        is_collaborative,
        total_tracks,
        description,
        snapshot_id,
        extracted_at,
        loaded_at
    FROM source
)

SELECT * FROM renamed

