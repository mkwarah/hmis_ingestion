with source as (

    select * from {{ source('landing', 'data_sets') }}

)

select
    id                                  as data_set_id,
    nullif(trim(code), '')              as data_set_code,
    trim(name)                          as data_set_name,
    period_type,
    cast(last_updated as timestamp)     as last_updated_at,
    ingested_at

from source
