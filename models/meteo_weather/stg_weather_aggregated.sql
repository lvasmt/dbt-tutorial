WITH aggregated_data AS (
    SELECT
        EXTRACT(DATE FROM time) AS date,
        MAX(temperature)    AS max_temp,
        MIN(temperature)    AS min_temp,
        AVG(temperature)    AS avg_temp,
        MAX(humidity)       AS max_humidity,
        MIN(humidity)       AS min_humidity,
        AVG(humidity)       AS avg_humidity,
        SUM(precipitation)  AS total_precipitation,
        SUM(rain)           AS total_rain,
        SUM(snow)           AS total_snow
    FROM
        {{ref('stg_weather_deduped')}}
    GROUP BY 
        EXTRACT(DATE FROM time)  
)

SELECT
    *
FROM
    aggregated_data