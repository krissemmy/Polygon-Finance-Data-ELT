{{ config(
    materialized='incremental',
    unique_key='forex_id'
) }}
SELECT
    sf.id AS forex_id,
    dfd.date,
    dfp.id AS price_id,
    sf.request_id,
    sf.symbol,
    sf.trading_volume,
    sf.volume_weighted
FROM {{ ref('stg_forex') }} sf
LEFT JOIN {{ ref('dim_forex_dates') }} dfd
ON sf.date = dfd.date
LEFT JOIN {{ ref("dim_forex_prices") }} dfp
ON sf.forex_id = dfp.forex_id