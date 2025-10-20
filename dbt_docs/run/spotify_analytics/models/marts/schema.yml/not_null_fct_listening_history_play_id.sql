select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select play_id
from "spotify"."marts"."fct_listening_history"
where play_id is null



      
    ) dbt_internal_test