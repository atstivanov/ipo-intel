{{ config(materialized='table', schema='analytics') }}

SELECT
    coverage_status,
    COUNT(*) AS ipo_count,
    COUNT(*) FILTER (WHERE industry IS NOT NULL) AS with_industry,
    COUNT(*) FILTER (WHERE sector IS NOT NULL) AS with_sector,
    ROUND(AVG(distinct_price_days)::numeric, 2) AS avg_price_days
FROM {{ ref('ipo_coverage') }}
GROUP BY 1
ORDER BY ipo_count DESC