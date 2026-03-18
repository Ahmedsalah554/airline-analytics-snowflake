

WITH route_stats AS (
    SELECT
        f.route_id,
        r.origin_airport_id,
        r.destination_airport_id,
        f.DEPARTURE_DATE,
        COUNT(*) AS offer_count,
        AVG(f.price_usd) AS avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price_usd) AS median_price,
        MIN(f.price_usd) AS min_price,
        MAX(f.price_usd) AS max_price,
        STDDEV(f.price_usd) AS price_stddev,
        AVG(f.lead_time_days) AS avg_lead_time,
        AVG(f.STOPS) AS avg_stops,
        COUNT(DISTINCT f.airline_id) AS airline_count
    FROM ANALYTICS_DB.SILVER.fct_daily_flight_offers f
    LEFT JOIN ANALYTICS_DB.STAGING.stg_routes r ON f.route_id = r.route_id
    WHERE f.route_id IS NOT NULL
    GROUP BY f.route_id, r.origin_airport_id, r.destination_airport_id, f.DEPARTURE_DATE
),

volatility_calculation AS (
    SELECT
        route_id,
        origin_airport_id,
        destination_airport_id,
        DEPARTURE_DATE,
        offer_count,
        avg_price,
        median_price,
        min_price,
        max_price,
        price_stddev,
        CASE 
            WHEN avg_price > 0 THEN ROUND(100.0 * price_stddev / avg_price, 2)
            ELSE 0
        END AS price_volatility_index,
        avg_lead_time,
        avg_stops,
        airline_count
    FROM route_stats
)

SELECT
    v.route_id,
    v.origin_airport_id,
    v.destination_airport_id,
    CONCAT(o.CITY_NAME, ' - ', d.CITY_NAME) AS route_name,
    v.DEPARTURE_DATE,
    v.offer_count,
    v.avg_price,
    v.median_price,
    v.min_price,
    v.max_price,
    v.price_stddev,
    v.price_volatility_index,
    CASE
        WHEN v.price_volatility_index < 10 THEN 'Low'
        WHEN v.price_volatility_index < 20 THEN 'Medium'
        ELSE 'High'
    END AS volatility_category,
    v.avg_lead_time,
    GREATEST(1, ROUND(v.avg_lead_time - 7)) AS cheapest_window_start,
    LEAST(365, ROUND(v.avg_lead_time + 7)) AS cheapest_window_end,
    v.avg_stops,
    v.airline_count,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM volatility_calculation v
LEFT JOIN ANALYTICS_DB.STAGING.stg_airports o ON v.origin_airport_id = o.airport_id
LEFT JOIN ANALYTICS_DB.STAGING.stg_airports d ON v.destination_airport_id = d.airport_id
ORDER BY v.DEPARTURE_DATE DESC, v.price_volatility_index DESC