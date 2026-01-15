{{ config(materialized='table') }}

with ipos as (
    select
        event_id,
        ipo_date,
        symbol,
        case
            when symbol ~ '.*(U|W|R)$' then 'spac_like'
            else 'equity_candidate'
        end as instrument_type
    from {{ ref('stg_ipos_valid') }}
),

mapped as (
    select
        vendor,
        ipo_symbol,
        vendor_symbol,
        is_priceable
    from raw.symbol_map
    where vendor = 'alphavantage'
),

price_days as (
    select
        symbol,
        count(*) as price_rows,
        min(price_date) as min_price_date,
        max(price_date) as max_price_date
    from raw.daily_prices
    where source = 'alphavantage'
    group by 1
),

joined as (
    select
        i.event_id,
        i.ipo_date,
        i.symbol as ipo_symbol,
        i.instrument_type,
        m.vendor_symbol,
        m.is_priceable,
        coalesce(p.price_rows, 0) as price_rows,
        p.min_price_date,
        p.max_price_date,
        case
            when i.instrument_type = 'spac_like' then 'excluded_spac_like'
            when m.ipo_symbol is null then 'unmapped'
            when m.is_priceable = false then 'mapped_not_priceable'
            when coalesce(p.price_rows,0) = 0 then 'mapped_priceable_no_data'
            else 'has_prices'
        end as coverage_status
    from ipos i
    left join mapped m
      on m.ipo_symbol = i.symbol
    left join price_days p
      on p.symbol = m.vendor_symbol
)

select * from joined
