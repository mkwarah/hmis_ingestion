-- Flattens the DHIS2 org unit tree into one row per org unit with a column
-- per level. DHIS2 stores each unit's ancestry in `path`:
--   /ImspTQPwCqd/kJq2mPyFEHo/KIUCimTXf8Q/ueuQlqb8ccl
--    country     district    chiefdom    facility
-- Level names follow the Sierra Leone demo; rename them for another country.

with org_units as (

    select * from {{ ref('stg_dhis2__organisation_units') }}

),

split as (

    select
        org_unit_id,
        org_unit_name,
        org_unit_level,
        -- leading '/' gives an empty first element, so ancestors start at [2]
        string_split(path, '/')[2] as level_1_id,
        string_split(path, '/')[3] as level_2_id,
        string_split(path, '/')[4] as level_3_id,
        string_split(path, '/')[5] as level_4_id
    from org_units

)

select
    s.org_unit_id,
    s.org_unit_name,
    s.org_unit_level,

    s.level_1_id    as country_id,
    l1.org_unit_name as country_name,
    s.level_2_id    as district_id,
    l2.org_unit_name as district_name,
    s.level_3_id    as chiefdom_id,
    l3.org_unit_name as chiefdom_name,
    s.level_4_id    as facility_id,
    l4.org_unit_name as facility_name

from split as s
left join org_units as l1 on l1.org_unit_id = nullif(s.level_1_id, '')
left join org_units as l2 on l2.org_unit_id = nullif(s.level_2_id, '')
left join org_units as l3 on l3.org_unit_id = nullif(s.level_3_id, '')
left join org_units as l4 on l4.org_unit_id = nullif(s.level_4_id, '')
