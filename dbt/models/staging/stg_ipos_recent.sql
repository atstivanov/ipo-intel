{{ config(materialized='view', schema='staging') }}

WITH base AS (
  SELECT
    event_id,
    ipo_date::date AS ipo_date,
    upper(symbol) AS ipo_symbol,
    company_name,
    exchange
  FROM {{ source('raw', 'ipo_events') }}
  WHERE ipo_date >= (current_date - interval '180 days')
    AND symbol IS NOT NULL
    AND symbol <> ''
),

mapped AS (
  SELECT
    b.event_id,
    b.ipo_date,
    b.ipo_symbol,
    b.company_name,
    b.exchange,
    m.vendor_symbol AS price_symbol,
    COALESCE(m.is_priceable, false) AS is_priceable
  FROM base b
  LEFT JOIN {{ source('raw', 'symbol_map') }} m
    ON m.vendor = 'alphavantage'
   AND m.ipo_symbol = b.ipo_symbol
),

profile AS (
  SELECT
    symbol AS ipo_symbol,
    industry,
    country,
    currency,
    market_cap,
    weburl
  FROM {{ source('raw', 'company_profiles') }}
  WHERE source = 'finnhub'
)

SELECT
  m.event_id,
  m.ipo_date,
  m.ipo_symbol,
  m.price_symbol,
  m.is_priceable,
  m.company_name,
  m.exchange,
  p.industry,
  p.country,
  p.currency,
  p.market_cap,
  p.weburl
FROM mapped m
LEFT JOIN profile p
  ON p.ipo_symbol = m.ipo_symbol
