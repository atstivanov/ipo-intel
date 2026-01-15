{{ config(materialized='table', schema='analytics') }}

WITH m AS (
  SELECT *
  FROM {{ ref('ipo_metrics_100d') }}
  WHERE n_days >= 5
),

bench AS (
  SELECT
    COALESCE(industry, 'Unknown') AS industry,
    COUNT(*) AS cohort_size,
    AVG(return_100d) AS avg_return_100d,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_100d) AS median_return_100d,
    AVG(max_drawdown_100d) AS avg_max_drawdown_100d,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY max_drawdown_100d) AS median_max_drawdown_100d
  FROM m
  GROUP BY 1
)

SELECT *
FROM bench
ORDER BY cohort_size DESC, industry

