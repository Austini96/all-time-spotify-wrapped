select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select play_id
from "spotify"."staging"."stg_spotify_tracks"
where play_id is null



      
    ) dbt_internal_test