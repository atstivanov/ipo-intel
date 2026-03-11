{{ config(materialized='table', schema='analytics') }}

WITH prices AS (
    SELECT
        event_id,
        ipo_symbol,
        ipo_date,
        price_symbol,
        price_source,
        price_date,
        close,
        day_num,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY price_date) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY price_date DESC) AS rn_desc
    FROM {{ ref('stg_prices_ipo_window_100d') }}
    WHERE close IS NOT NULL
),

first_last AS (
    SELECT
        event_id,
        MAX(CASE WHEN rn_asc = 1 THEN close END) AS first_close,
        MAX(CASE WHEN rn_desc = 1 THEN close END) AS last_close
    FROM prices
    GROUP BY 1
),

agg AS (
    SELECT
        event_id,
        COUNT(*) AS n_days,
        MIN(close) AS min_close,
        MAX(close) AS max_close
    FROM prices
    GROUP BY 1
),

joined AS (
    SELECT
        c.event_id,
        c.ipo_date,
        c.ipo_symbol,
        c.company_name,
        c.exchange,
        c.industry,
        c.sector,
        c.price_vendor,
        c.price_symbol,
        c.coverage_status,
        c.is_analysis_ready,
        a.n_days,
        fl.first_close,
        fl.last_close,
        a.min_close,
        a.max_close
    FROM {{ ref('ipo_coverage') }} c
    LEFT JOIN agg a
        ON c.event_id = a.event_id
    LEFT JOIN first_last fl
        ON c.event_id = fl.event_id
)

SELECT
    event_id,
    ipo_date,
    ipo_symbol,
    company_name,
    exchange,
    COALESCE(NULLIF(TRIM(industry), ''), 'Not available') AS industry,
    COALESCE(NULLIF(TRIM(sector), ''), 'Not available') AS sector,
    price_vendor,
    price_symbol,
    coverage_status,
    is_analysis_ready,
    n_days,
    first_close,
    last_close,
    min_close,
    max_close,
    CASE
        WHEN first_close IS NULL OR last_close IS NULL OR first_close = 0 THEN NULL
        ELSE (last_close - first_close) / first_close
    END AS return_100d,
    CASE
        WHEN first_close IS NULL OR max_close IS NULL OR first_close = 0 THEN NULL
        ELSE (max_close - first_close) / first_close
    END AS max_gain_100d,
    CASE
        WHEN first_close IS NULL OR min_close IS NULL OR first_close = 0 THEN NULL
        ELSE (min_close - first_close) / first_close
    END AS max_drawdown_100d
FROM joined