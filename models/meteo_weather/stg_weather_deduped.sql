WITH raw_meteo_data AS (
    SELECT  
        time,
        temperature_2m      AS temperature,
        relativehumidity_2m AS humidity,
        precipitation       AS precipitation,
        rain                AS rain,
        snowfall            AS snow,
        time_of_load        AS time_of_load
    FROM    
        {{source('meteo_data','raw_open_meteo_weather_data')}}
), 

dedupe_key AS (
  SELECT
    *, 
    ROW_NUMBER() OVER (PARTITION BY time ORDER BY time_of_load DESC) as row_num
  FROM
    raw_meteo_data
),

deduped_data AS (
  SELECT
    *
    EXCEPT (row_num)
  FROM
    dedupe_key
  WHERE
    row_num = 1
)

SELECT
  *
FROM
  deduped_data