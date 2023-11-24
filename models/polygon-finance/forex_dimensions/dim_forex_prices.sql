{{ config(
    materialized='incremental',
    unique_key='forex_id'
) }}
SELECT
    GENERATE_UUID() AS price_id,
    forex_id,
    open_price,
    close_price,
    lowest_price,
    highest_price,
    CAST(number_of_transaction AS INTEGER) AS number_of_transaction,
    adjusted
FROM {{ ref("stg_forex") }}