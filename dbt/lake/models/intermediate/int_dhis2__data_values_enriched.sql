-- Data values joined to every dimension: what was counted, how it is
-- disaggregated, where (full hierarchy), and through which data set.
-- Same grain as stg_dhis2__data_values: one row per data value.
--
-- PII BOUNDARY: this is the first layer with no personal data. The hashed
-- data-entry user (stored_by_hash) and the free-text comment from staging
-- are NOT selected; only the fact that a comment exists is kept.
-- assert_no_pii_in_intermediate enforces this.
--
-- Left joins keep values whose metadata is missing; the relationship tests
-- in staging flag those cases.

with data_values as (

    select * from {{ ref('stg_dhis2__data_values') }}

),

data_elements as (

    select * from {{ ref('stg_dhis2__data_elements') }}

),

category_option_combos as (

    select * from {{ ref('stg_dhis2__category_option_combos') }}

),

org_units as (

    select * from {{ ref('int_dhis2__org_unit_hierarchy') }}

),

data_sets as (

    select * from {{ ref('stg_dhis2__data_sets') }}

)

select
    dv.data_value_key,

    -- data set
    dv.data_set_id,
    ds.data_set_name,
    ds.period_type                              as data_set_period_type,

    -- period
    dv.period,
    dv.period_type,
    dv.period_type = ds.period_type             as is_expected_period_type,
    dv.period_start_date,
    dv.period_end_date,

    -- what
    dv.data_element_id,
    de.data_element_name,
    de.value_type,
    de.aggregation_type,
    dv.category_option_combo_id,
    coc.category_option_combo_name,

    -- where
    dv.org_unit_id,
    ou.org_unit_name,
    ou.org_unit_level,
    ou.country_name,
    ou.district_id,
    ou.district_name,
    ou.chiefdom_id,
    ou.chiefdom_name,
    ou.facility_id,
    ou.facility_name,

    -- measure
    dv.value_raw,
    dv.value_numeric,

    -- audit (no personal data)
    dv.created_at,
    dv.last_updated_at,
    dv.is_followup,
    dv.comment is not null                      as has_comment,
    dv.ingested_at,
    dv.run_id

from data_values as dv
left join data_elements          as de  on de.data_element_id = dv.data_element_id
left join category_option_combos as coc on coc.category_option_combo_id = dv.category_option_combo_id
left join org_units              as ou  on ou.org_unit_id = dv.org_unit_id
left join data_sets              as ds  on ds.data_set_id = dv.data_set_id
