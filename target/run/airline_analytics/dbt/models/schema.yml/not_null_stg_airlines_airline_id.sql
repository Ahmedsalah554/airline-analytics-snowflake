select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select airline_id
from ANALYTICS_DB.STAGING.stg_airlines
where airline_id is null



      
    ) dbt_internal_test