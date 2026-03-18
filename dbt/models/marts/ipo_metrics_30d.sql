{{ config(materialized='table') }}

with ipos as (

    select
        event_id,
        ipo_date,
        ipo_symbol,
        price_symbol,
        industry,
        sector
    from {{ ref('stg_ipos_recent') }}

),

prices as (

    select
        event_id,
        ipo_date,
        price_date,
        close,
        price_source
    from {{ ref('stg_prices_ipo_window_30d') }}

),

price_stats as (

    select
        event_id,
        count(distinct price_date) as price_days,
        min(price_date) as first_price_date,
        max(price_date) as last_price_date,
        avg(close) as avg_close_30d,
        min(close) as min_close_30d,
        max(close) as max_close_30d
    from prices
    group by 1

),

first_prices as (

    select
        event_id,
        close as first_close_30d
    from (
        select
            event_id,
            close,
            row_number() over (partition by event_id order by price_date asc) as rn
        from prices
    ) x
    where rn = 1

),

last_prices as (

    select
        event_id,
        close as last_close_30d
    from (
        select
            event_id,
            close,
            row_number() over (partition by event_id order by price_date desc) as rn
        from prices
    ) x
    where rn = 1

),

final as (

    select
        i.event_id,
        i.ipo_date,
        i.ipo_symbol,
        i.price_symbol,
        i.industry,
        i.sector,

        coalesce(ps.price_days, 0) as price_days,
        ps.first_price_date,
        ps.last_price_date,
        fp.first_close_30d,
        lp.last_close_30d,
        ps.avg_close_30d,
        ps.min_close_30d,
        ps.max_close_30d,

        case
            when fp.first_close_30d is not null
             and lp.last_close_30d is not null
             and fp.first_close_30d <> 0
            then round(((lp.last_close_30d / fp.first_close_30d) - 1) * 100.0, 2)
            else null
        end as return_30d_pct,

        case
            when coalesce(ps.price_days, 0) >= 15 then 'good_coverage'
            when coalesce(ps.price_days, 0) >= 7 then 'partial_coverage'
            when coalesce(ps.price_days, 0) >= 1 then 'very_low_coverage'
            else 'no_prices'
        end as coverage_status_30d

    from ipos i
    left join price_stats ps using (event_id)
    left join first_prices fp using (event_id)
    left join last_prices lp using (event_id)

)

select *
from final