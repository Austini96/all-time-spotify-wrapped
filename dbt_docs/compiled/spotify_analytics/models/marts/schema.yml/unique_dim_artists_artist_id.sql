
    
    

select
    artist_id as unique_field,
    count(*) as n_records

from "spotify"."marts"."dim_artists"
where artist_id is not null
group by artist_id
having count(*) > 1


