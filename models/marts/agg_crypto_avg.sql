SELECT
    sc.symbol,
    AVG(dcp.open_price) AS avg_open_price,
    AVG(dcp.close_price) AS avg_close_price,
    AVG(dcp.lowest_price) AS avg_lowest_price,
    AVG(dcp.highest_price) AS avg_highest_price,
    AVG(CAST(sc.number_of_transaction AS INTEGER)) AS avg_number_of_transaction
FROM {{ ref('stg_crypto') }} sc
LEFT JOIN {{ ref('dim_crypto_dates') }} dcd
ON sc.date = dcd.date
LEFT JOIN {{ ref("dim_crypto_prices") }} dcp
ON sc.id = dcp.crypto_id
GROUP BY sc.symbol