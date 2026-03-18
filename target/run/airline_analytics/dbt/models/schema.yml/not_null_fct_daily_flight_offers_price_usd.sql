select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price_usd
from ANALYTICS_DB.SILVER.fct_daily_flight_offers
where price_usd is null



      
    ) dbt_internal_test