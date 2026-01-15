{{ config(materialized='table') }}

select
  coverage_status,
  instrument_type,
  count(*) as ipo_count
from {{ ref('ipo_coverage') }}
group by 1,2
order by 1,2
