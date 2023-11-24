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

SELECT * 
FROM renamed