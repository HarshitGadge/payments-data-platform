with source as (

    select * from {{ ref('raw_merchants') }}

)

select
    cast(merchant_id as {{ dbt.type_bigint() }}) as merchant_id,
    trim(merchant_name)                          as merchant_name,
    upper(trim(country_code))                    as country_code,
    lower(trim(category))                        as category
from source
