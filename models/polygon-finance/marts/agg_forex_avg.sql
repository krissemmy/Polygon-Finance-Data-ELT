{{ config(
    materialized='incremental',
    unique_key='symbol'
) }}
SELECT
    sf.symbol,
    dfd.date,
    AVG(dfp.open_price) AS avg_open_price,
    AVG(dfp.close_price) AS avg_close_price,
    AVG(dfp.lowest_price) AS avg_lowest_price,
    AVG(dfp.highest_price) AS avg_highest_price,
    AVG(CAST(sf.number_of_transaction AS INTEGER)) AS avg_number_of_transaction
FROM {{ ref('stg_forex') }} sf
LEFT JOIN {{ ref('dim_forex_dates') }} dfd
ON sf.date = dfd.date
LEFT JOIN {{ ref("dim_forex_prices") }} dfp
ON sf.id = dfp.forex_id
GROUP BY sf.symbol, dfd.date