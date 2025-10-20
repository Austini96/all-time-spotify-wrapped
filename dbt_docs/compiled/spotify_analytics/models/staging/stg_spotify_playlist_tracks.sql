-- Staging model for Spotify playlist-track relationships
WITH source AS (
    SELECT *
    FROM raw_spotify_playlist_tracks
),

renamed AS (
    SELECT
        playlist_id,
        track_id,
        added_at,
        added_by,
        position,
        loaded_at
    FROM source
)

SELECT * FROM renamed