-- Typed, lightly cleaned view over the landed payments extract.
--
-- amount is cast to DECIMAL, never FLOAT: binary floating point cannot
-- represent 0.10, and the error compounds across millions of rows into a
-- reconciliation break against the source ledger.

with source as (

    select * from {{ ref('raw_payments') }}

),

typed as (

    select
        cast(payment_id      as {{ dbt.type_bigint() }})  as payment_id,
        cast(merchant_id     as {{ dbt.type_bigint() }})  as merchant_id,
        cast(shopper_id      as {{ dbt.type_bigint() }})  as shopper_id,
        cast(amount          as decimal(12, 2))           as amount,
        upper(trim(currency))                             as currency,
        lower(trim(payment_method))                       as payment_method,
        lower(trim(payment_status))                       as payment_status,
        upper(trim(country_code))                         as country_code,
        cast(created_at      as {{ dbt.type_timestamp() }}) as created_at,
        cast(created_at      as date)                     as payment_date
    from source

)

select * from typed
