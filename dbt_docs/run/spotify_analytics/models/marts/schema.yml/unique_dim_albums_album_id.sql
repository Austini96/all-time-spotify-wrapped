select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    album_id as unique_field,
    count(*) as n_records

from "spotify"."marts"."dim_albums"
where album_id is not null
group by album_id
having count(*) > 1



      
    ) dbt_internal_test