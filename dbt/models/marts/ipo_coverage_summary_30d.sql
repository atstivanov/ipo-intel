{{ config(materialized='table') }}

select
    coverage_status_30d as coverage_status,
    count(*) as ipo_count,
    count(industry) as with_industry,
    count(sector) as with_sector,
    round(avg(price_days)::numeric, 2) as avg_price_days
from {{ ref('ipo_metrics_30d') }}
group by 1
order by
    case coverage_status_30d
        when 'good_coverage' then 1
        when 'partial_coverage' then 2
        when 'very_low_coverage' then 3
        when 'no_prices' then 4
        else 5
    end