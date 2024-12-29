{{ config(
   materialized="table"
) }}

WITH crhistmas_data AS (
SELECT 
    *
FROM {{ source('staging', 'sg_christmas_sales') }}
WHERE deliverytime IS NOT NULL
    AND Event = 'Christmas Market'
)
SELECT 
    * EXCLUDE(Date, Time),
    TO_TIMESTAMP(CONCAT(Date, ' ', Time)) Timestamp
FROM crhistmas_data

