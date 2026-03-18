select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    airline_id as unique_field,
    count(*) as n_records

from ANALYTICS_DB.STAGING.stg_airlines
where airline_id is not null
group by airline_id
having count(*) > 1



      
    ) dbt_internal_test