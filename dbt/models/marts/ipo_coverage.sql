{{ config(materialized='table', schema='analytics') }}

WITH ipos AS (
    SELECT
        event_id,
        ipo_date,
        ipo_symbol,
        company_name,
        exchange,
        industry,
        sector,
        price_vendor,
        price_symbol,
        is_priceable
    FROM {{ ref('stg_ipos_recent') }}
),

prices AS (
    SELECT
        event_id,
        COUNT(*) AS price_rows,
        MIN(price_date) AS first_price_date,
        MAX(price_date) AS last_price_date,
        COUNT(DISTINCT price_date) AS distinct_price_days
    FROM {{ ref('stg_prices_ipo_window_100d') }}
    GROUP BY 1
)

SELECT
    i.event_id,
    i.ipo_date,
    i.ipo_symbol,
    i.company_name,
    i.exchange,
    i.industry,
    i.sector,
    i.price_vendor,
    i.price_symbol,
    i.is_priceable,
    COALESCE(p.price_rows, 0) AS price_rows,
    COALESCE(p.distinct_price_days, 0) AS distinct_price_days,
    p.first_price_date,
    p.last_price_date,
    CASE
        WHEN i.is_priceable = false THEN 'not_priceable'
        WHEN i.price_symbol IS NULL THEN 'unmapped'
        WHEN COALESCE(p.distinct_price_days, 0) = 0 THEN 'mapped_but_no_prices'
        WHEN COALESCE(p.distinct_price_days, 0) < 20 THEN 'very_low_coverage'
        WHEN COALESCE(p.distinct_price_days, 0) < 60 THEN 'partial_coverage'
        ELSE 'good_coverage'
    END AS coverage_status,
    CASE
        WHEN COALESCE(p.distinct_price_days, 0) >= 60 THEN true
        ELSE false
    END AS is_analysis_ready
FROM ipos i
LEFT JOIN prices p
    ON i.event_id = p.event_id