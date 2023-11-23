{{ config(
    materialized='incremental',
    unique_key='forex_id'
) }}
SELECT
    sf.forex_id,
    dfd.date_id,
    dfp.price_id,
    sf.symbol,
    sf.trading_volume
FROM {{ ref('stg_forex') }} sf
LEFT JOIN {{ ref('dim_forex_dates') }} dfd
ON sf.request_id = dfd.request_id
LEFT JOIN {{ ref("dim_forex_prices") }} dfp
ON sf.forex_id = dfp.forex_id