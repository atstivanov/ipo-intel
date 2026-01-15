{{ config(materialized='view', schema='staging') }}

WITH ipos AS (
  SELECT
    event_id,
    ipo_date,
    price_symbol
  FROM {{ ref('stg_ipos_recent') }}
  WHERE is_priceable = true
    AND price_symbol IS NOT NULL
),

prices AS (
  SELECT
    symbol,
    price_date,
    open,
    high,
    low,
    close,
    volume
  FROM {{ source('raw', 'daily_prices') }}
  WHERE source = 'alphavantage'
)

SELECT
  i.event_id,
  i.ipo_date,
  p.symbol AS price_symbol,
  p.price_date,
  p.open, p.high, p.low, p.close, p.volume
FROM ipos i
JOIN prices p
  ON p.symbol = i.price_symbol
WHERE p.price_date >= i.ipo_date
  AND p.price_date <= i.ipo_date + INTERVAL '{{ var("ipo_price_window_days", 100) }} days'
