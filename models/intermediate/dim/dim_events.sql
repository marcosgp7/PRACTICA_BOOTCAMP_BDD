{{ config(
    materialized='incremental'
) }}
WITH dim_events AS (

    SELECT * FROM
    {{ source('mias', 'events') }}
)
    SELECT * FROM dim_events