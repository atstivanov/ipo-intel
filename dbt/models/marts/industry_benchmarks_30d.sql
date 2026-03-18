{{ config(materialized='table') }}

select
    coalesce(industry, 'Unknown') as industry,
    count(*) as ipo_count,
    round(avg(return_30d_pct)::numeric, 2) as avg_return_30d_pct,
    round(avg(price_days)::numeric, 2) as avg_price_days
from {{ ref('ipo_metrics_30d') }}
where price_days >= 15
  and return_30d_pct is not null
group by 1
order by ipo_count desc, industry