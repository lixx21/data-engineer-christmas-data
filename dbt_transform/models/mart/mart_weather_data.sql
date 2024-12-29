{{ config(
   materialized="table"
) }}

SELECT 
    weather,
    weather_description,
    temp - 273.15 AS temp_celcius,
    wind_speed,
    country_name,
    TO_TIMESTAMP(DATE_DATA) timestamp
FROM {{ source('staging','sg_weather_data') }} 
