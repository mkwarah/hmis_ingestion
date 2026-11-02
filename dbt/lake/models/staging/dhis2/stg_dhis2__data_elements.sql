with source as (

    select * from {{ source('landing', 'data_elements') }}

)

select
    id                                  as data_element_id,
    nullif(trim(code), '')              as data_element_code,
    trim(name)                          as data_element_name,
    trim(short_name)                    as data_element_short_name,
    value_type,
    aggregation_type,
    domain_type,
    cast(last_updated as timestamp)     as last_updated_at,
    ingested_at

from source
