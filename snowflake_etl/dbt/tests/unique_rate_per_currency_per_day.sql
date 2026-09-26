-- Two rates for one currency on one day make the payment join non-deterministic
-- and silently fan out the fact table.
select rate_date, currency, count(*) as rate_count
from {{ ref('stg_fx_rates') }}
group by rate_date, currency
having count(*) > 1
