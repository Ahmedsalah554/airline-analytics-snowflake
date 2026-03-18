
    
    

select
    offer_id as unique_field,
    count(*) as n_records

from ANALYTICS_DB.SILVER.fct_daily_flight_offers
where offer_id is not null
group by offer_id
having count(*) > 1


