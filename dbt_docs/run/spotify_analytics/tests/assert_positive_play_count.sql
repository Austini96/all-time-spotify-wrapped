select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test to ensure all daily aggregations have positive play counts
SELECT
    played_date,
    play_count
FROM "spotify"."analytics"."top_artists_daily"
WHERE play_count <= 0
      
    ) dbt_internal_test