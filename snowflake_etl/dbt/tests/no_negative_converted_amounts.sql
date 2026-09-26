-- A positive amount must not convert to a negative one. This catches a
-- sign-flipped or inverted rate, which otherwise looks plausible in aggregate.
select payment_id, amount, amount_usd
from {{ ref('fct_payments_usd') }}
where amount > 0 and amount_usd < 0
