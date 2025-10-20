
    
    

select
    playlist_id as unique_field,
    count(*) as n_records

from "spotify"."marts"."dim_playlists"
where playlist_id is not null
group by playlist_id
having count(*) > 1


