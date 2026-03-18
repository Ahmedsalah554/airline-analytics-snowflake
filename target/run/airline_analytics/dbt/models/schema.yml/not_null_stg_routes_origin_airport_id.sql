select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select origin_airport_id
from ANALYTICS_DB.STAGING.stg_routes
where origin_airport_id is null



      
    ) dbt_internal_test