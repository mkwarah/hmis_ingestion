-- Latest data element snapshot, renamed and typed.

with source as (

    select * from {{ source('landing', 'data_elements') }}
    {{ latest_snapshot() }}

)

select
    id                                  as data_element_id,
    nullif(trim(code), '')              as data_element_code,
    trim(name)                          as data_element_name,
    trim(shortName)                     as data_element_short_name,
    valueType                           as value_type,
    aggregationType                     as aggregation_type,
    domainType                          as domain_type,
    try_cast(lastUpdated as timestamp)  as last_updated_at,
    run_id

from source
