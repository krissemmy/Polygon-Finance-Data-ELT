SELECT
    ss.symbol,
    AVG(dsp.open_price) AS avg_open_price,
    AVG(dsp.close_price) AS avg_close_price,
    AVG(dsp.lowest_price) AS avg_lowest_price,
    AVG(dsp.highest_price) AS avg_highest_price,
    AVG(CAST(ss.number_of_transaction AS INTEGER)) AS avg_number_of_transaction
FROM {{ ref('stg_stock') }} ss
LEFT JOIN {{ ref('dim_stock_dates') }} dsd
ON ss.date = dsd.date
LEFT JOIN {{ ref("dim_stock_prices") }} dsp
ON ss.id = dsp.stock_id
GROUP BY ss.symbol