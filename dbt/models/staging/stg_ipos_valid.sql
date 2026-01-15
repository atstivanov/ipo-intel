{{ config(materialized='view') }}

select
  event_id,
  ipo_date,
  symbol,
  company_name,
  exchange,
  price_range_low,
  price_range_high,
  shares,
  ingested_at
from raw.ipo_events
where symbol is not null
  and symbol <> ''
  and symbol ~ '^[A-Z]{1,5}$'
  and ipo_date is not null
