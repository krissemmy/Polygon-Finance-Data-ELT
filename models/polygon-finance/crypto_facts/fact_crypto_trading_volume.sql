{{ config(
    materialized='incremental',
    unique_key='crypto_id'
) }}
SELECT
    sc.id AS crypto_id,
    dcd.date,
    dcp.id AS price_id,
    sc.request_id,
    sc.symbol,
    sc.trading_volume,
    sc.volume_weighted
FROM {{ ref('stg_crypto') }} sc
INNER JOIN {{ ref('dim_crypto_dates') }} dcd
ON sc.date = dcd.date
INNER JOIN {{ ref("dim_crypto_prices") }} dcp
ON sc.id = dcp.crypto_id