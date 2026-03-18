
  
    

        create or replace transient table ANALYTICS_DB.GOLD.airline_performance
         as
        (

WITH daily_airline_stats AS (
    SELECT
        f.airline_id,
        f.DEPARTURE_DATE,
        COUNT(*) AS flight_count,
        AVG(f.price_usd) AS avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price_usd) AS median_price,
        MIN(f.price_usd) AS min_price,
        MAX(f.price_usd) AS max_price,
        STDDEV(f.price_usd) AS price_stddev,
        AVG(f.lead_time_days) AS avg_lead_time
    FROM ANALYTICS_DB.SILVER.fct_daily_flight_offers f
    WHERE f.airline_id IS NOT NULL
    GROUP BY f.airline_id, f.DEPARTURE_DATE
),

total_market AS (
    SELECT
        DEPARTURE_DATE,
        SUM(flight_count) AS total_flights,
        AVG(avg_price) AS market_avg_price
    FROM daily_airline_stats
    GROUP BY DEPARTURE_DATE
)

SELECT
    s.airline_id,
    a.airline_name,
    s.DEPARTURE_DATE,
    s.flight_count,
    t.total_flights,
    ROUND(100.0 * s.flight_count / NULLIF(t.total_flights, 0), 2) AS market_share_percentage,
    s.avg_price,
    s.median_price,
    s.min_price,
    s.max_price,
    s.price_stddev,
    s.avg_lead_time,
    t.market_avg_price,
    ROUND(s.avg_price - t.market_avg_price, 2) AS price_vs_market,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM daily_airline_stats s
LEFT JOIN ANALYTICS_DB.STAGING.stg_airlines a ON s.airline_id = a.airline_id
LEFT JOIN total_market t ON s.DEPARTURE_DATE = t.DEPARTURE_DATE
ORDER BY s.DEPARTURE_DATE DESC, market_share_percentage DESC
        );
      
  