

WITH source AS (
    SELECT DISTINCT
        AIRLINE AS airline_code
    FROM ANALYTICS_DB.RAW.real_flights
    WHERE AIRLINE IS NOT NULL
)

SELECT
    airline_code AS airline_id,
    airline_code AS airline_name,
    airline_code AS airline_iata_code,
    'Unknown' AS country_name,
    'XX' AS country_iso,
    NULL AS alliance,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM source