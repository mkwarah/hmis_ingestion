-- Which facilities are expected to report which form, with their place in
-- the hierarchy. Denominator of the reporting rate in gold:
--   facilities reporting a form in a period / facilities assigned the form.
-- Only org units that still exist and are open are counted.

with assignments as (

    select * from {{ ref('stg_dhis2__data_set_org_units') }}

),

data_sets as (

    select * from {{ ref('stg_dhis2__data_sets') }}

),

org_units as (

    select * from {{ ref('stg_dhis2__organisation_units') }}

),

hierarchy as (

    select * from {{ ref('int_dhis2__org_unit_hierarchy') }}

)

select
    a.data_set_org_unit_key,
    a.data_set_id,
    ds.data_set_name,
    ds.period_type                  as data_set_period_type,
    a.org_unit_id,
    h.org_unit_name,
    h.org_unit_level,
    h.district_id,
    h.district_name,
    h.chiefdom_id,
    h.chiefdom_name,
    ou.opening_date,
    ou.closed_date
from assignments as a
join data_sets as ds  on ds.data_set_id = a.data_set_id
join org_units as ou  on ou.org_unit_id = a.org_unit_id
join hierarchy as h   on h.org_unit_id = a.org_unit_id
where ou.closed_date is null
