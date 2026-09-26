-- Published daily FX rates: units of `currency` per one unit of `base_currency`.
--
-- Non-positive rates are dropped rather than loaded. A rate of zero would make
-- the conversion divide by zero; a negative rate is bad data, not a cheap
-- currency.

with source as (

    select * from {{ ref('raw_fx_rates') }}

)

select
    cast(rate_date as date)          as rate_date,
    upper(trim(base_currency))       as base_currency,
    upper(trim(currency))            as currency,
    cast(rate as decimal(18, 8))     as rate
from source
where rate is not null
  and cast(rate as decimal(18, 8)) > 0
