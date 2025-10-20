
    
    

select
    album_id as unique_field,
    count(*) as n_records

from "spotify"."marts"."dim_albums"
where album_id is not null
group by album_id
having count(*) > 1


