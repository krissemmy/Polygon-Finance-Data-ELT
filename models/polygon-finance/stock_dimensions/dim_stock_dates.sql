{{ config(
    materialized='incremental',
    unique_key='date'
) }}
WITH dim_date AS (
    SELECT
        date,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        FORMAT_DATE('%A', date) AS weekday,
        EXTRACT(QUARTER FROM date) AS quarter,
        EXTRACT(WEEK FROM date) AS week_number,
        CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekday_weekend,
        CASE WHEN EXTRACT(DAY FROM date) IN (25, 26, 31) AND EXTRACT(MONTH FROM date) = 12 THEN 'Holiday' ELSE 'Non-Holiday' END AS holiday,
        IF(EXTRACT(MONTH FROM date) = 2 AND MOD(EXTRACT(YEAR FROM date), 4) = 0, 'Leap Year', 'Non-Leap Year') AS leap_year,
        CASE
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Unknown'
        END AS season
    FROM {{ ref("stg_stock") }}
)
SELECT DISTINCT *
FROM dim_date