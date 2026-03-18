{{ config(materialized='view') }}

select
    *
from {{ ref('stg_prices_ipo_window_100d') }}
where price_date::date >= ipo_date::date
  and price_date::date <= (ipo_date::date + interval '30 day')