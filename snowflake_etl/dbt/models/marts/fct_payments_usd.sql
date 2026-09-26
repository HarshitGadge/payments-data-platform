-- Every payment, normalised to the reporting currency.
--
-- Conversion uses the rate effective on the *payment date*, not today's rate.
-- Revaluing history at the current rate makes last quarter's reported revenue
-- move every morning, which is the fastest way to lose finance's trust in a
-- warehouse.
--
-- A payment whose currency has no applicable rate keeps a NULL amount_usd and
-- is flagged rather than dropped or defaulted to 1.0. Defaulting would report
-- 100 GBP as 100 USD; dropping would make the row vanish from revenue without
-- anything surfacing that it had.

{{ config(
    materialized = 'incremental',
    unique_key   = 'payment_id',
    incremental_strategy = 'merge'
) }}

with payments as (

    select * from {{ ref('stg_payments') }}

    {% if is_incremental() %}
    -- Re-process a trailing window rather than only new rows: a rate published
    -- late changes the USD value of payments already loaded.
    where payment_date >= (
        select coalesce(max(payment_date), cast('1900-01-01' as date))
               - interval {{ var('fx_restatement_days') }} day
        from {{ this }}
    )
    {% endif %}

),

rates as (

    select * from {{ ref('int_fx_rates_daily') }}

),

converted as (

    select
        payments.payment_id,
        payments.merchant_id,
        payments.shopper_id,
        payments.payment_date,
        payments.created_at,
        payments.currency,
        payments.amount,
        payments.payment_method,
        payments.payment_status,
        payments.country_code,

        case
            when payments.currency = '{{ var("reporting_currency") }}' then 1.0
            else rates.effective_rate
        end as fx_rate,

        case
            when payments.currency = '{{ var("reporting_currency") }}'
                then payments.amount
            when rates.effective_rate is not null and rates.effective_rate > 0
                then cast(payments.amount / rates.effective_rate as decimal(18, 2))
            else null
        end as amount_usd,

        coalesce(rates.is_carried_forward, false) as fx_rate_carried_forward

    from payments
    left join rates
        on  rates.currency      = payments.currency
        and rates.calendar_date = payments.payment_date

)

select
    *,
    -- Settled money only. Counting failed, pending, cancelled, refunded or
    -- charged-back payments as revenue is the most common way a payments
    -- dashboard overstates income.
    payment_status in ('authorized', 'captured') as is_revenue,
    amount_usd is null                            as is_unconverted
from converted
