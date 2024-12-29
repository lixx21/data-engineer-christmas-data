{{ config(
   materialized="table"
) }}

SELECT 
    * EXCLUDE(Votes),
    CAST(REPLACE(votes, ',', '') AS NUMBER) Votes
FROM {{ source('staging', 'sg_christmas_movies') }}