
    
    

select
    play_id as unique_field,
    count(*) as n_records

from "spotify"."marts"."fct_listening_history"
where play_id is not null
group by play_id
having count(*) > 1


