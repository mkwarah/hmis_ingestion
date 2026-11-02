-- Latest org unit snapshot. Names in the DHIS2 demo carry stray leading
-- spaces (' Panderu MCHP'), so every text field is trimmed here, once.
-- The intermediate layer builds the hierarchy from `path`.

with source as (

    select * from {{ source('landing', 'organisation_units') }}
    {{ latest_snapshot() }}

)

select
    id                                  as org_unit_id,
    nullif(trim(code), '')              as org_unit_code,
    trim(name)                          as org_unit_name,
    trim(shortName)                     as org_unit_short_name,
    level                               as org_unit_level,
    parent.id                           as parent_id,
    path,
    try_cast(openingDate as date)       as opening_date,
    try_cast(closedDate as date)        as closed_date,
    try_cast(lastUpdated as timestamp)  as last_updated_at,
    run_id

from source
