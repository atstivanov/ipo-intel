{{ config(materialized='table', schema='analytics') }}

WITH base AS (
    SELECT
        industry,
        sector,
        exchange,
        return_100d,
        max_gain_100d,
        max_drawdown_100d,
        n_days
    FROM {{ ref('ipo_metrics_100d') }}
    WHERE is_analysis_ready = true
      AND return_100d IS NOT NULL
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
GROUP BY 1,2,3
HAVING COUNT(*) >= 2
ORDER BY cohort_size DESC, industry