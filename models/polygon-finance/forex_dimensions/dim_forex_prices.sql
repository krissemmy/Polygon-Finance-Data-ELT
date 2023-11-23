SELECT
    GENERATE_UUID() AS price_id,
    forex_id,
    open_price,
    close_price,
    lowest_price,
    highest_price,
    adjusted
FROM {{ ref("stg_forex") }}