-- Daily settled revenue per merchant, in the reporting currency.
--
-- Only rows that both settled and converted are summed. An unconverted payment
-- is excluded from the total and counted separately, so the gap is visible
-- instead of silently understating revenue.

with payments as (

    select * from {{ ref('fct_payments_usd') }}

)

select
    payment_date                                          as revenue_date,
    merchant_id,
    count(*)                                              as payment_count,
    sum(case when is_revenue then 1 else 0 end)           as settled_count,
    sum(case when is_revenue and not is_unconverted
             then amount_usd else 0 end)                  as gross_amount_usd,
    sum(case when is_revenue and is_unconverted
             then 1 else 0 end)                           as unconverted_count,
    count(distinct shopper_id)                            as distinct_shoppers,
    sum(case when fx_rate_carried_forward then 1 else 0 end) as carried_forward_count
from payments
group by payment_date, merchant_id
