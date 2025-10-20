
    
    

select
    playlist_id as unique_field,
    count(*) as n_records

from "spotify"."staging"."stg_spotify_playlists"
where playlist_id is not null
group by playlist_id
having count(*) > 1


