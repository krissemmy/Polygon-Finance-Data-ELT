{{ config(
    materialized='incremental',
    unique_key='stock_id'
) }}
WITH cte AS (
SELECT
    ss.id as stock_id,
    dsp.id as price_id,
    ss.request_id,
    dsd.date,
    ss.symbol,
    ss.trading_volume,
    ss.volume_weighted
FROM {{ ref('stg_stock') }} ss
INNER JOIN {{ ref('dim_stock_dates') }} dsd
ON ss.date = dsd.date
INNER JOIN {{ ref("dim_stock_prices") }} dsp
ON ss.id = dsp.stock_id
)

SELECT DISTINCT *
FROM cte