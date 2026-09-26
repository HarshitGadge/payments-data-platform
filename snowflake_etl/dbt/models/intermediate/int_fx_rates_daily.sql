-- A rate for every (currency, date) a payment actually occurred on.
--
-- FX markets close at the weekend and on holidays; payments do not. Joining
-- payments straight to published rates therefore loses every payment taken on a
-- Saturday. This model forward-fills: each date carries the most recent rate
-- published on or before it, which is what "Friday's rate applies until Monday"
-- means in practice.
--
-- The date spine is built from dates that exist in the data (payment dates plus
-- rate dates) rather than from a generated calendar. That keeps the SQL plain
-- ANSI -- Snowflake's GENERATOR and DuckDB's generate_series have no common
-- spelling -- and generates no rows for dates nothing happened on.

with payment_dates as (

    select distinct payment_date as calendar_date
    from {{ ref('stg_payments') }}

),

rate_dates as (

    select distinct rate_date as calendar_date
    from {{ ref('stg_fx_rates') }}

),

spine as (

    select calendar_date from payment_dates
    union
    select calendar_date from rate_dates

),

traded_currencies as (

    select distinct currency
    from {{ ref('stg_payments') }}
    where currency <> '{{ var("reporting_currency") }}'

),

grid as (

    select
        spine.calendar_date,
        traded_currencies.currency
    from spine
    cross join traded_currencies

),

with_published as (

    select
        grid.calendar_date,
        grid.currency,
        fx.rate as published_rate
    from grid
    left join {{ ref('stg_fx_rates') }} as fx
        on  fx.rate_date = grid.calendar_date
        and fx.currency  = grid.currency

),

filled as (

    select
        calendar_date,
        currency,
        published_rate,
        -- Carry the last published rate forward across closed-market days.
        last_value(published_rate ignore nulls) over (
            partition by currency
            order by calendar_date
            rows between unbounded preceding and current row
        ) as effective_rate
    from with_published

)

select
    calendar_date,
    currency,
    published_rate,
    effective_rate,
    -- "Carried forward" means an older published rate was reused, which is
    -- only true when there was one to reuse. A date before the feed began, or
    -- a currency the feed does not cover, has no rate at all -- flagging those
    -- as carried forward would overstate the carried_forward_count metric and
    -- hide the fact that the payment could not be converted.
    published_rate is null and effective_rate is not null as is_carried_forward
from filled
