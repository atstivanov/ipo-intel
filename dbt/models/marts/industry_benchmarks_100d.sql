{{ config(materialized='table', schema='analytics') }}

WITH base AS (
    SELECT
        COALESCE(NULLIF(TRIM(industry), ''), 'Not available') AS industry,
        COALESCE(NULLIF(TRIM(sector), ''), 'Not available') AS sector,
        exchange,
        return_100d,
        max_gain_100d,
        max_drawdown_100d,
        n_days,
        coverage_status
    FROM {{ ref('ipo_metrics_100d') }}
    WHERE return_100d IS NOT NULL
      AND coverage_status IN ('partial_coverage', 'good_coverage')
)

SELECT
    industry,
    sector,
    exchange,
    COUNT(*) AS cohort_size,
    ROUND(AVG(return_100d)::numeric, 4) AS avg_return_100d,
    ROUND(AVG(max_gain_100d)::numeric, 4) AS avg_max_gain_100d,
    ROUND(AVG(max_drawdown_100d)::numeric, 4) AS avg_max_drawdown_100d,
    ROUND(AVG(n_days)::numeric, 2) AS avg_price_days
FROM base
GROUP BY
    industry,
    sector,
    exchange
HAVING COUNT(*) >= 1
ORDER BY
    cohort_size DESC,
    industry