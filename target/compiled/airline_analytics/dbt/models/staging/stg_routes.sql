

WITH routes AS (
    SELECT DISTINCT
        ORIGIN,
        DESTINATION
    FROM ANALYTICS_DB.RAW.real_flights
    WHERE ORIGIN IS NOT NULL AND DESTINATION IS NOT NULL
)

SELECT
    CONCAT(ORIGIN, '-', DESTINATION) AS route_id,
    ORIGIN AS origin_airport_id,
    DESTINATION AS destination_airport_id,
    NULL AS route_distance_km,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM routes