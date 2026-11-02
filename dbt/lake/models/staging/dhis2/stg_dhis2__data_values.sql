-- First transformation of DHIS2 data values: landing JSON -> one typed row
-- per current data value. Steps:
--   1. unnest   each payload's dataValues into rows
--   2. restore  fields DHIS2 hoisted to the payload level (period, orgUnit,
--               attributeOptionCombo) onto every value
--   3. snapshot keep, per data set and period, only rows from the latest
--               FULL run onward (a full run is a complete snapshot of its
--               window, so values deleted in DHIS2 drop out), plus later
--               incremental changes
--   4. dedupe   one row per data value: latest run, then latest lastUpdated
--   5. type     timestamps, numeric value, period dates
--   6. hash     surrogate key (md5 of the natural key) and the data-entry
--               user (salted SHA-256): storedBy is personal data and does
--               not leave landing in clear text
-- The free-text comment is kept here and removed in intermediate (PII).

with payloads as (

    select * from {{ source('landing', 'data_values') }}

),

values_ as (

    select
        dataset                                 as dataset_key,
        data_set_id,
        run_id,
        mode                                    as load_mode,
        filename                                as source_file,
        period                                  as payload_period,
        orgUnit                                 as payload_org_unit,
        attributeOptionCombo                    as payload_attribute_option_combo,
        unnest(dataValues)                      as dv
    from payloads

),

restored as (

    select
        dataset_key,
        data_set_id,
        run_id,
        load_mode,
        source_file,
        dv.dataElement                                           as data_element_id,
        coalesce(dv.period, payload_period)                      as period,
        coalesce(dv.orgUnit, payload_org_unit)                   as org_unit_id,
        dv.categoryOptionCombo                                   as category_option_combo_id,
        coalesce(dv.attributeOptionCombo, payload_attribute_option_combo)
                                                                 as attribute_option_combo_id,
        dv.value                                                 as value_raw,
        dv.storedBy                                              as stored_by,
        dv.created                                               as created_raw,
        dv.lastUpdated                                           as last_updated_raw,
        dv.comment                                               as comment,
        dv.followup                                              as followup
    from values_

),

snapshot as (

    select *
    from (
        select
            *,
            max(case when load_mode = 'full' then run_id end)
                over (partition by data_set_id, period)          as latest_full_run_id
        from restored
    )
    where latest_full_run_id is null
       or run_id >= latest_full_run_id

),

deduped as (

    select *
    from snapshot
    qualify row_number() over (
        partition by data_set_id, data_element_id, period, org_unit_id,
                     category_option_combo_id, attribute_option_combo_id
        order by run_id desc, last_updated_raw desc
    ) = 1

),

typed as (

    select
        *,
        {{ dhis2_period_type('period') }}                        as period_type
    from deduped

),

dated as (

    select
        *,
        {{ dhis2_period_start_date('period', 'period_type') }}   as period_start_date
    from typed

)

select
    -- surrogate key over the natural key (+ data set: the same value can be
    -- reported through more than one data set)
    md5(concat_ws('|',
        data_set_id, data_element_id, period, org_unit_id,
        category_option_combo_id, attribute_option_combo_id
    ))                                                           as data_value_key,

    -- foreign keys
    dataset_key,
    data_set_id,
    data_element_id,
    org_unit_id,
    category_option_combo_id,
    attribute_option_combo_id,

    -- period
    period,
    period_type,
    period_start_date,
    {{ dhis2_period_end_date('period_start_date', 'period_type') }} as period_end_date,

    -- measure
    value_raw,
    try_cast(value_raw as double)                                as value_numeric,

    -- audit
    try_cast(created_raw as timestamptz)                         as created_at,
    try_cast(last_updated_raw as timestamptz)                    as last_updated_at,
    coalesce(followup, false)                                    as is_followup,

    -- personal data: hashed here, removed in intermediate
    {{ hash_pii('stored_by') }}                                  as stored_by_hash,
    nullif(trim(comment), '')                                    as comment,

    -- lineage
    run_id,
    load_mode,
    strptime(run_id, '%Y%m%dT%H%M%SZ')                           as ingested_at,
    source_file

from dated
