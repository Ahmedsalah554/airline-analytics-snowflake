
  
    

        create or replace transient table ANALYTICS_DB.STAGING.dim_airports
         as
        (

WITH source AS (
    SELECT 
        AIRPORT_ID,
        AIRPORT_IATA_CODE,
        -- مش في AIRPORT_NAME، هنستخدم IATA CODE كاسم
        AIRPORT_IATA_CODE AS AIRPORT_NAME,
        CITY_NAME,
        COUNTRY_CODE,
        TIMEZONE,
        DBT_UPDATED_AT
    FROM ANALYTICS_DB.STAGING.stg_airports
)

SELECT
    AIRPORT_ID,
    AIRPORT_IATA_CODE,
    AIRPORT_NAME,
    CITY_NAME,
    COUNTRY_CODE,
    TIMEZONE,
    CURRENT_TIMESTAMP AS DBT_UPDATED_AT
FROM source
WHERE AIRPORT_ID IS NOT NULL
ORDER BY CITY_NAME, AIRPORT_IATA_CODE
        );
      
  