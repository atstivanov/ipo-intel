{{ config(materialized='view', schema='staging') }}

WITH ipos AS (
    SELECT
        event_id,
        ipo_date,
        ipo_symbol,
        price_symbol,
        price_vendor
    FROM {{ ref('stg_ipos_recent') }}
    WHERE is_priceable = true
      AND price_symbol IS NOT NULL
),

preferred_daily_prices AS (
    SELECT
        source,
        symbol,
        price_date::date AS price_date,
        open,
        high,
        low,
        close,
        volume,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, price_date
            ORDER BY
                CASE
                    WHEN source = 'alphavantage' THEN 1
                    WHEN source = 'yahoo' THEN 2
                    ELSE 99
                END
        ) AS rn
    FROM {{ source('raw', 'daily_prices') }}
)

SELECT
    i.event_id,
    i.ipo_symbol,
    i.ipo_date,
    i.price_symbol,
    p.source AS price_source,
    p.price_date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    (p.price_date - i.ipo_date) AS day_num
FROM ipos i
JOIN preferred_daily_prices p
  ON i.price_symbol = p.symbol
 AND p.rn = 1
WHERE p.price_date >= i.ipo_date
  AND p.price_date <= i.ipo_date + interval '100 days'