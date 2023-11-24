SELECT
    ss.id as stock_id,
    dsd.date,
    dsp.id as price_id,
    ss.request_id,
    ss.symbol,
    ss.trading_volume,
    ss.volume_weighted
FROM {{ ref('stg_stock') }} ss
LEFT JOIN {{ ref('dim_stock_dates') }} dsd
ON ss.date = dsd.date
LEFT JOIN {{ ref("dim_stock_prices") }} dsp
ON ss.request_id = dsp.request_id