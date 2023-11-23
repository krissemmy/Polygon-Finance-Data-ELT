{{ config(
    materialized='incremental',
    unique_key='crypto_id'
) }}
SELECT
    sc.crypto_id,
    dcd.date_id,
    dcp.price_id,
    sc.symbol,
    sc.trading_volume
FROM {{ ref('stg_crypto') }} sc
LEFT JOIN {{ ref('dim_crypto_dates') }} dcd
ON sc.request_id = dcd.request_id
LEFT JOIN {{ ref("dim_crypto_prices") }} dcp
ON sc.crypto_id = dcp.crypto_id