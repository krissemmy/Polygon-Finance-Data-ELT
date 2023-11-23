{{ config(
    materialized='incremental',
    unique_key='stock_id'
) }}
SELECT
    ss.stock_id,
    dsd.date_id,
    dsp.price_id,
    ss.symbol,
    ss.trading_volume
FROM {{ ref('stg_stock') }} ss
LEFT JOIN {{ ref('dim_stock_dates') }} dsd
ON ss.request_id = dsd.request_id
LEFT JOIN {{ ref("dim_stock_prices") }} dsp
ON ss.stock_id = dsp.stock_id