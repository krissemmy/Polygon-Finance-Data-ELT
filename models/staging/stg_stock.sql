WITH source AS (

    SELECT * FROM {{ source('raw', 'polygon_stock') }}

),

renamed AS (

    SELECT
        GENERATE_UUID() as id,
        symbol,
        request_id,
        date,
        timestamp_unix,
        open_price,
        close_price,
        lowest_price,
        highest_price,
        adjusted,
        CAST(number_of_transaction AS INTEGER) AS number_of_transaction,
        trading_volume,
        volume_weighted

    FROM source

)


SELECT
    CASE 
        WHEN f.id IS NULL THEN t.id
        ELSE GENERATE_UUID()  
    END AS id,
    t.request_id,
    t.symbol,
    t.date,
    t.timestamp_unix,
    t.open_price,
    t.close_price,
    t.lowest_price,
    t.highest_price,
    t.adjusted,
    CAST(t.number_of_transaction AS INTEGER) AS number_of_transaction,
    t.trading_volume,
    t.volume_weighted
FROM renamed t
LEFT JOIN {{ ref('stg_stock_1') }} f
ON t.id = f.id