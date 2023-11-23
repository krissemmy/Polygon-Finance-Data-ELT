WITH source AS (

    SELECT * FROM {{ source('raw', 'polygon_forex') }}

),

renamed AS (

    SELECT
        GENERATE_UUID() as unique_key,
        symbol,
        request_id,
        date,
        timestamp_unix,
        open_price,
        close_price,
        lowest_price,
        highest_price,
        adjusted,
        number_of_transaction,
        trading_volume,
        volume_weighted

    FROM source

)

SELECT * 
FROM renamed