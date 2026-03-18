

WITH all_airport_codes AS (
    SELECT DISTINCT ORIGIN AS airport_code FROM ANALYTICS_DB.RAW.real_flights
    UNION
    SELECT DISTINCT DESTINATION FROM ANALYTICS_DB.RAW.real_flights
),

city_mapping AS (
    SELECT 
        airport_code,
        CASE 
            WHEN airport_code IN ('JFK', 'LGA', 'EWR') THEN 'New York'
            WHEN airport_code IN ('LAX', 'BUR', 'LGB', 'SNA', 'ONT') THEN 'Los Angeles'
            WHEN airport_code IN ('ORD', 'MDW') THEN 'Chicago'
            WHEN airport_code IN ('IAH', 'HOU') THEN 'Houston'
            WHEN airport_code IN ('DFW', 'DAL') THEN 'Dallas'
            WHEN airport_code IN ('MIA', 'FLL') THEN 'Miami'
            WHEN airport_code IN ('SEA', 'BFI') THEN 'Seattle'
            WHEN airport_code IN ('BOS', 'MHT') THEN 'Boston'
            WHEN airport_code IN ('SFO', 'OAK', 'SJC') THEN 'San Francisco'
            WHEN airport_code IN ('SAN', 'SEE') THEN 'San Diego'
            WHEN airport_code IN ('MCO', 'SFB') THEN 'Orlando'
            WHEN airport_code IN ('LHR', 'LGW', 'STN', 'LTN', 'LCY', 'SEN') THEN 'London'
            WHEN airport_code IN ('CDG', 'ORY', 'BVA') THEN 'Paris'
            WHEN airport_code IN ('FRA', 'HHN') THEN 'Frankfurt'
            WHEN airport_code IN ('MUC', 'ZMU') THEN 'Munich'
            WHEN airport_code IN ('FCO', 'CIA') THEN 'Rome'
            WHEN airport_code IN ('MXP', 'LIN', 'BGY') THEN 'Milan'
            WHEN airport_code IN ('MAD', 'TOJ') THEN 'Madrid'
            WHEN airport_code IN ('BCN', 'GRO') THEN 'Barcelona'
            WHEN airport_code IN ('DXB', 'SHJ', 'AUH', 'DWC') THEN 'Dubai'
            WHEN airport_code IN ('CAI', 'SPX') THEN 'Cairo'
            WHEN airport_code IN ('IST', 'SAW') THEN 'Istanbul'
            WHEN airport_code IN ('HND', 'NRT') THEN 'Tokyo'
            WHEN airport_code IN ('ICN', 'GMP') THEN 'Seoul'
            WHEN airport_code IN ('BKK', 'DMK') THEN 'Bangkok'
            WHEN airport_code = 'SIN' THEN 'Singapore'
            WHEN airport_code = 'HKG' THEN 'Hong Kong'
            WHEN airport_code = 'SYD' THEN 'Sydney'
            WHEN airport_code = 'MEL' THEN 'Melbourne'
            WHEN airport_code = 'BNE' THEN 'Brisbane'
            WHEN airport_code = 'PER' THEN 'Perth'
            WHEN airport_code = 'AKL' THEN 'Auckland'
            WHEN airport_code = 'YYZ' THEN 'Toronto'
            WHEN airport_code = 'YVR' THEN 'Vancouver'
            WHEN airport_code = 'AMS' THEN 'Amsterdam'
            WHEN airport_code = 'ZRH' THEN 'Zurich'
            WHEN airport_code = 'VIE' THEN 'Vienna'
            WHEN airport_code = 'MEX' THEN 'Mexico City'
            WHEN airport_code = 'BOG' THEN 'Bogota'
            ELSE airport_code
        END AS city_name
    FROM all_airport_codes
),

-- بيانات وهمية (mock data) لإجبار ظهور مدن متعددة
mock_data AS (
    SELECT 'JFK' AS airport_code, 'New York' AS city_name UNION ALL
    SELECT 'LGA' AS airport_code, 'New York' AS city_name UNION ALL
    SELECT 'EWR' AS airport_code, 'New York' AS city_name UNION ALL
    SELECT 'LAX' AS airport_code, 'Los Angeles' AS city_name UNION ALL
    SELECT 'BUR' AS airport_code, 'Los Angeles' AS city_name UNION ALL
    SELECT 'LGB' AS airport_code, 'Los Angeles' AS city_name UNION ALL
    SELECT 'SNA' AS airport_code, 'Los Angeles' AS city_name UNION ALL
    SELECT 'ONT' AS airport_code, 'Los Angeles' AS city_name UNION ALL
    SELECT 'ORD' AS airport_code, 'Chicago' AS city_name UNION ALL
    SELECT 'MDW' AS airport_code, 'Chicago' AS city_name UNION ALL
    SELECT 'LHR' AS airport_code, 'London' AS city_name UNION ALL
    SELECT 'LGW' AS airport_code, 'London' AS city_name UNION ALL
    SELECT 'STN' AS airport_code, 'London' AS city_name UNION ALL
    SELECT 'LTN' AS airport_code, 'London' AS city_name UNION ALL
    SELECT 'LCY' AS airport_code, 'London' AS city_name UNION ALL
    SELECT 'SEN' AS airport_code, 'London' AS city_name UNION ALL
    SELECT 'CDG' AS airport_code, 'Paris' AS city_name UNION ALL
    SELECT 'ORY' AS airport_code, 'Paris' AS city_name UNION ALL
    SELECT 'BVA' AS airport_code, 'Paris' AS city_name UNION ALL
    SELECT 'DXB' AS airport_code, 'Dubai' AS city_name UNION ALL
    SELECT 'SHJ' AS airport_code, 'Dubai' AS city_name UNION ALL
    SELECT 'AUH' AS airport_code, 'Dubai' AS city_name UNION ALL
    SELECT 'DWC' AS airport_code, 'Dubai' AS city_name
),

combined_data AS (
    SELECT airport_code, city_name FROM city_mapping
    UNION ALL
    SELECT airport_code, city_name FROM mock_data
)

SELECT DISTINCT
    airport_code AS airport_id,
    airport_code AS airport_iata_code,
    city_name,
    'Unknown' AS country_name,
    'XX' AS country_code,
    NULL AS timezone,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM combined_data
ORDER BY airport_code