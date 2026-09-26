-- A payment already in the reporting currency must pass through untouched.
-- Any rate other than 1.0 applied to it is a bug in the join.
select payment_id, amount, amount_usd, fx_rate
from {{ ref('fct_payments_usd') }}
where currency = '{{ var("reporting_currency") }}'
  and (amount_usd <> amount or fx_rate <> 1.0)
