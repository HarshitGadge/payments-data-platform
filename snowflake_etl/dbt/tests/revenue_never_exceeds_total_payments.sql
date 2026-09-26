-- Settled payments are a subset of all payments; a count that exceeds the
-- total means the status filter or the grain is wrong.
select revenue_date, merchant_id, payment_count, settled_count
from {{ ref('fct_daily_revenue_usd') }}
where settled_count > payment_count
