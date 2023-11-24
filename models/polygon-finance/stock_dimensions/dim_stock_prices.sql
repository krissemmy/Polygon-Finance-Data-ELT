SELECT
    GENERATE_UUID() AS id,
    stock_id,
    open_price,
    close_price,
    lowest_price,
    highest_price,
    CAST(number_of_transaction AS INTEGER) AS number_of_transaction,
    adjusted
FROM {{ ref("stg_stock") }}
