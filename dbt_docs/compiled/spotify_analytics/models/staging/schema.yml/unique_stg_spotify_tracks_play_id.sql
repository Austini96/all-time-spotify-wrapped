
    
    

select
    play_id as unique_field,
    count(*) as n_records

from "spotify"."staging"."stg_spotify_tracks"
where play_id is not null
group by play_id
having count(*) > 1


