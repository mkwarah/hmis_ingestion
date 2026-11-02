-- Latest data set (reporting form) snapshot. The org units assigned to each
-- form are unnested in stg_dhis2__data_set_org_units.

with source as (

    select * from {{ source('landing', 'data_sets') }}
    {{ latest_snapshot() }}

)

select
    id                                  as data_set_id,
    nullif(trim(code), '')              as data_set_code,
    trim(name)                          as data_set_name,
    periodType                          as period_type,
    len(coalesce(organisationUnits, [])) as assigned_org_units,
    try_cast(lastUpdated as timestamp)  as last_updated_at,
    run_id

from source
