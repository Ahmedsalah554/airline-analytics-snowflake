select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select offer_id
from ANALYTICS_DB.SILVER.fct_daily_flight_offers
where offer_id is null



      
    ) dbt_internal_test