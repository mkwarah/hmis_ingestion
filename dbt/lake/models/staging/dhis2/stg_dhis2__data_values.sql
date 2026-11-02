-- One row per DHIS2 data value. Keeps IDs only: names are joined from
-- current metadata in the intermediate layer, so a renamed data element or
-- facility shows its new name everywhere, not the name it had at load time.

with source as (

    select * from {{ source('landing', 'data_values') }}

),

typed as (

    select
        *,
        {{ dhis2_period_type('period') }} as period_type
    from source

),

dated as (

    select
        *,
        {{ dhis2_period_start_date('period', 'period_type') }} as period_start_date
    from typed

)

select
    -- surrogate key over DHIS2's natural key (+ data set: the same value can
    -- be reported through more than one data set)
    md5(concat_ws('|',
        data_set_id, data_element_id, period, org_unit_id,
        category_option_combo_id, attribute_option_combo_id
    ))                                          as data_value_key,

    -- foreign keys
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
    value                                       as value_raw,
    value_numeric,

    -- audit
    stored_by,
    created                                     as created_at,
    last_updated                                as last_updated_at,
    nullif(trim(comment), '')                   as comment,
    coalesce(followup, false)                   as is_followup,
    ingested_at,
    filename                                    as source_file

from dated
