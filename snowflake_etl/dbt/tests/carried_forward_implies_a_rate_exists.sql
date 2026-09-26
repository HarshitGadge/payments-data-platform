-- A row flagged as carried forward must actually have a rate. A date before
-- the feed began has nothing to carry forward from, and marking it as carried
-- would overstate the carried_forward_count metric while hiding that the
-- payment never converted.
select calendar_date, currency, published_rate, effective_rate
from {{ ref('int_fx_rates_daily') }}
where is_carried_forward and effective_rate is null
