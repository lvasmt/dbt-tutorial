{{
  config(
    materialized = "table",
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH stg_data AS (
    SELECT 
        date,
        max_temp,
        min_temp,
        avg_temp,
        max_humidity,
        min_humidity,
        avg_humidity,
        total_precipitation,
        total_rain,
        total_snow
    FROM    
        {{ref('stg_weather_aggregated')}}
)

SELECT 
    *
FROM 
    stg_data