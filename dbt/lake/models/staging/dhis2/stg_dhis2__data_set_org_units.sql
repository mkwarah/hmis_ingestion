-- Data set assignments: one row per (data set, org unit) the form is
-- assigned to, i.e. the facilities EXPECTED to report it. This is the
-- denominator of the reporting rate.

with source as (

    select * from {{ source('landing', 'data_sets') }}
    {{ latest_snapshot() }}

),

assignments as (

    select
        id                              as data_set_id,
        unnest(organisationUnits).id    as org_unit_id,
        run_id
    from source

)

select
    md5(data_set_id || '|' || org_unit_id) as data_set_org_unit_key,
    data_set_id,
    org_unit_id,
    run_id
from assignments
