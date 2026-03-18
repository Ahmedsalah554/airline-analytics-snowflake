
  
    

        create or replace transient table ANALYTICS_DB.SILVER.fct_daily_flight_offers
         as
        (

WITH original_flights AS (
    SELECT
        ID AS offer_id,
        ORIGIN,
        DESTINATION,
        DEPARTURE_DATE,
        CAST(PRICE AS FLOAT) AS price,
        CURRENCY,
        AIRLINE AS airline_code,
        STOPS,
        DATE(LOADED_AT) AS search_date,
        LOADED_AT
    FROM ANALYTICS_DB.RAW.real_flights
),

-- بيانات المطارات البديلة
alternative_airports_list AS (
    SELECT 'LGW' AS alt_airport, 'LHR' AS main_airport, 0.92 AS price_factor, 1 AS extra_stops
    UNION ALL SELECT 'STN', 'LHR', 0.85, 2
    UNION ALL SELECT 'LTN', 'LHR', 0.78, 1
    UNION ALL SELECT 'LCY', 'LHR', 1.15, 0
    UNION ALL SELECT 'SEN', 'LHR', 0.75, 2
    UNION ALL SELECT 'ORY', 'CDG', 0.88, 1
    UNION ALL SELECT 'BVA', 'CDG', 0.75, 2
    UNION ALL SELECT 'LGB', 'LAX', 0.85, 1
    UNION ALL SELECT 'BUR', 'LAX', 0.82, 1
    UNION ALL SELECT 'ONT', 'LAX', 0.79, 2
    UNION ALL SELECT 'SNA', 'LAX', 0.88, 1
    UNION ALL SELECT 'AUH', 'DXB', 0.90, 1
    UNION ALL SELECT 'SHJ', 'DXB', 0.75, 2
    UNION ALL SELECT 'DWC', 'DXB', 0.70, 2
    UNION ALL SELECT 'EWR', 'JFK', 0.95, 0
    UNION ALL SELECT 'LGA', 'JFK', 0.85, 1
    UNION ALL SELECT 'MDW', 'ORD', 0.80, 1
),

alternative_flights AS (
    SELECT
        CONCAT('ALT-', a.alt_airport, '-', ROW_NUMBER() OVER (PARTITION BY a.alt_airport ORDER BY f.DEPARTURE_DATE)) AS offer_id,
        a.alt_airport AS ORIGIN,
        f.DESTINATION,
        f.DEPARTURE_DATE,
        f.price * a.price_factor AS price,
        f.CURRENCY,
        f.airline_code,
        f.STOPS + a.extra_stops AS STOPS,
        f.search_date,
        CURRENT_TIMESTAMP() AS LOADED_AT
    FROM original_flights f
    CROSS JOIN alternative_airports_list a
    WHERE f.ORIGIN = a.main_airport
),

all_flights AS (
    SELECT 
        offer_id,
        ORIGIN,
        DESTINATION,
        DEPARTURE_DATE,
        price,
        CURRENCY,
        airline_code,
        STOPS,
        search_date,
        LOADED_AT
    FROM original_flights
    
    UNION ALL
    
    SELECT 
        offer_id,
        ORIGIN,
        DESTINATION,
        DEPARTURE_DATE,
        price,
        CURRENCY,
        airline_code,
        STOPS,
        search_date,
        LOADED_AT
    FROM alternative_flights
),

currency_conversion AS (
    SELECT
        *,
        CASE
            WHEN UPPER(CURRENCY) = 'USD' THEN price
            WHEN UPPER(CURRENCY) = 'EUR' THEN price * 1.05
            WHEN UPPER(CURRENCY) = 'GBP' THEN price * 1.25
            WHEN UPPER(CURRENCY) = 'EGP' THEN price * 0.032
            ELSE price
        END AS price_usd
    FROM all_flights
),

routes AS (
    SELECT
        route_id,
        origin_airport_id,
        destination_airport_id
    FROM ANALYTICS_DB.STAGING.stg_routes
)

SELECT
    c.offer_id,
    COALESCE(r.route_id, CONCAT(c.ORIGIN, '-', c.DESTINATION)) AS route_id,
    c.airline_code AS airline_id,
    c.DEPARTURE_DATE,
    c.search_date,
    DATEDIFF(day, c.search_date, c.DEPARTURE_DATE) AS lead_time_days,
    c.price,
    c.price_usd,
    c.CURRENCY,
    c.STOPS,
    CASE
        WHEN c.price > 2000 THEN 'BUSINESS'
        WHEN c.price > 1000 THEN 'PREMIUM_ECONOMY'
        ELSE 'ECONOMY'
    END AS cabin_class,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM currency_conversion c
LEFT JOIN routes r ON c.ORIGIN = r.origin_airport_id AND c.DESTINATION = r.destination_airport_id
WHERE c.offer_id IS NOT NULL
        );
      
  