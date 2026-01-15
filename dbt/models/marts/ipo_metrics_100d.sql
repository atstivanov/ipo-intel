{{ config(materialized='table', schema='analytics') }}

with ipos as (
  select * from {{ ref('stg_ipos_recent') }}
),

px as (
  select
    event_id,
    price_date,
    close,
    lag(close) over (partition by event_id order by price_date) as prev_close,
    row_number() over (partition by event_id order by price_date) as rn,
    count(*) over (partition by event_id) as n_days,
    first_value(close) over (partition by event_id order by price_date) as first_close,
    last_value(close) over (
      partition by event_id order by price_date
      rows between unbounded preceding and unbounded following
    ) as last_close,
    min(close) over (partition by event_id) as min_close,
    max(close) over (partition by event_id) as max_close
  from {{ ref('stg_prices_ipo_window_100d') }}
  where close is not null
),

agg as (
  select
    event_id,

    max(n_days) as n_days,
    max(first_close) as first_close,
    max(last_close) as last_close,
    min(min_close) as min_close,
    max(max_close) as max_close,

    -- first trading day return (day2 vs day1), only if we have >=2 rows
    max(case when rn = 1 then close end) as day1_close,
    max(case when rn = 2 then close end) as day2_close,

    -- positive days ratio over the window: count of days where close > prev_close (ignore first day)
    sum(case when prev_close is not null and close > prev_close then 1 else 0 end) as positive_days,
    sum(case when prev_close is not null then 1 else 0 end) as comparable_days

  from px
  group by event_id
),

metrics as (
  select
    a.event_id,
    a.n_days,
    a.first_close,
    a.last_close,

    -- return over the whole available window up to 100d (not necessarily full 100 if not enough data)
    case
      when a.first_close is null or a.first_close = 0 or a.last_close is null then null
      else (a.last_close / a.first_close) - 1
    end as return_100d,

    -- first day return pct (day2 vs day1)
    case
      when a.day1_close is null or a.day1_close = 0 or a.day2_close is null then null
      else (a.day2_close / a.day1_close) - 1
    end as first_day_return_pct,

    -- max drawdown approximation using min/max in window
    case
      when a.max_close is null or a.max_close = 0 or a.min_close is null then null
      else 1 - (a.min_close / a.max_close)
    end as max_drawdown_100d,

    -- positive days ratio
    case
      when a.comparable_days is null or a.comparable_days = 0 then null
      else (a.positive_days::numeric / a.comparable_days::numeric)
    end as positive_days_ratio_100d

  from agg a
)

select
  i.event_id,
  i.ipo_date,
  i.ipo_symbol,
  i.price_symbol,
  i.company_name,
  i.exchange,
  i.industry,
  i.country,
  i.market_cap,

  m.n_days,
  m.first_close,
  m.last_close,
  m.return_100d,
  m.first_day_return_pct,
  m.max_drawdown_100d,
  m.positive_days_ratio_100d,

  current_date as as_of_date
from ipos i
left join metrics m
  on m.event_id = i.event_id
