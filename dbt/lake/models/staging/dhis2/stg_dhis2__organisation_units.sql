-- Names in the DHIS2 demo carry stray leading spaces (' Panderu MCHP'),
-- so every text field is trimmed here, once, for everything downstream.
-- The pipeline's ou_level_* columns are dropped: the intermediate layer
-- rebuilds the hierarchy from `path` (see int_dhis2__org_unit_hierarchy).

with source as (

    select * from {{ source('landing', 'organisation_units') }}

)

select
    id                                  as org_unit_id,
    nullif(trim(code), '')              as org_unit_code,
    trim(name)                          as org_unit_name,
    trim(short_name)                    as org_unit_short_name,
    level                               as org_unit_level,
    parent_id,
    path,
    cast(opening_date as date)          as opening_date,
    cast(closed_date as date)           as closed_date,
    cast(last_updated as timestamp)     as last_updated_at,
    ingested_at

from source
