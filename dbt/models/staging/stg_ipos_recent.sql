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
      AND upper(symbol) ~ '^[A-Z]{1,5}$'
      AND upper(symbol) !~ '.*(U|W|R)$'
      AND (
            upper(exchange) LIKE '%NASDAQ%'
         OR upper(exchange) LIKE '%NYSE%'
      )
),

preferred_symbol_map AS (
    SELECT
        ipo_symbol,
        vendor,
        vendor_symbol,
        is_priceable,
        notes,
        ROW_NUMBER() OVER (
            PARTITION BY ipo_symbol
            ORDER BY
                CASE
                    WHEN vendor = 'alphavantage' AND is_priceable = true THEN 1
                    WHEN vendor = 'yahoo'        AND is_priceable = true THEN 2
                    WHEN vendor = 'alphavantage'                         THEN 3
                    WHEN vendor = 'yahoo'                                THEN 4
                    ELSE 99
                END,
                last_checked_at DESC NULLS LAST
        ) AS rn
    FROM {{ source('raw', 'symbol_map') }}
),

preferred_profiles AS (
    SELECT
        symbol,
        source,
        industry,
        sector,
        country,
        currency,
        market_cap,
        weburl,
        ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY
                CASE
                    WHEN source = 'finnhub' AND industry IS NOT NULL THEN 1
                    WHEN source = 'yahoo'   AND industry IS NOT NULL THEN 2
                    WHEN source = 'finnhub' AND sector   IS NOT NULL THEN 3
                    WHEN source = 'yahoo'   AND sector   IS NOT NULL THEN 4
                    WHEN source = 'finnhub' THEN 5
                    WHEN source = 'yahoo'   THEN 6
                    ELSE 99
                END
        ) AS rn
    FROM {{ source('raw', 'company_profiles') }}
)

SELECT
    b.event_id,
    b.ipo_date,
    b.ipo_symbol,
    b.company_name,
    b.exchange,
    sm.vendor AS price_vendor,
    sm.vendor_symbol AS price_symbol,
    coalesce(sm.is_priceable, false) AS is_priceable,
    pp.source AS profile_source,
    pp.industry,
    pp.sector,
    pp.country,
    pp.currency,
    pp.market_cap,
    pp.weburl
FROM base b
LEFT JOIN preferred_symbol_map sm
    ON b.ipo_symbol = sm.ipo_symbol
   AND sm.rn = 1
LEFT JOIN preferred_profiles pp
    ON b.ipo_symbol = pp.symbol
   AND pp.rn = 1