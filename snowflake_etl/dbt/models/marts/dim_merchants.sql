with merchants as (

    select * from {{ ref('stg_merchants') }}

)

select
    merchant_id,
    merchant_name,
    country_code,
    category
from merchants
