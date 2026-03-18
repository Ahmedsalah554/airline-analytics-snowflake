
  
    

        create or replace transient table ANALYTICS_DB.GOLD.alternative_airports
         as
        (select * from (
              

WITH airport_prices AS (
    SELECT 
        a.AIRPORT_IATA_CODE,
        a.CITY_NAME,
        ROUND(AVG(f.price_usd), 2) as AVG_PRICE
    FROM ANALYTICS_DB.SILVER.fct_daily_flight_offers f
    INNER JOIN ANALYTICS_DB.STAGING.stg_airports a 
        ON f.route_id LIKE CONCAT('%', a.AIRPORT_IATA_CODE, '%')
    GROUP BY a.AIRPORT_IATA_CODE, a.CITY_NAME
),

ranked_airports AS (
    SELECT 
        CITY_NAME,
        AIRPORT_IATA_CODE,
        AVG_PRICE,
        ROW_NUMBER() OVER (PARTITION BY CITY_NAME ORDER BY AVG_PRICE DESC NULLS LAST) as price_rank
    FROM airport_prices
    WHERE CITY_NAME IN ('London', 'Los Angeles', 'Dubai', 'New York', 'Paris', 'Chicago')
)

SELECT 
    CITY_NAME,
    'XX' as COUNTRY_CODE,
    MAX(CASE WHEN price_rank = 1 THEN AIRPORT_IATA_CODE END) as PRIMARY_AIRPORT,
    MAX(CASE WHEN price_rank = 2 THEN AIRPORT_IATA_CODE END) as ALTERNATIVE_AIRPORT,
    50 as DISTANCE_KM,
    MAX(CASE WHEN price_rank = 1 THEN AVG_PRICE END) as PRIMARY_AVG_PRICE,
    MAX(CASE WHEN price_rank = 2 THEN AVG_PRICE END) as ALTERNATIVE_AVG_PRICE,
    MAX(CASE WHEN price_rank = 2 THEN AVG_PRICE END) - 
    MAX(CASE WHEN price_rank = 1 THEN AVG_PRICE END) as PRICE_DIFF,
    CURRENT_TIMESTAMP as DBT_UPDATED_AT
FROM ranked_airports
GROUP BY CITY_NAME
ORDER BY CITY_NAME
              ) order by (CITY_NAME)
        );
      alter table ANALYTICS_DB.GOLD.alternative_airports cluster by (CITY_NAME);
  