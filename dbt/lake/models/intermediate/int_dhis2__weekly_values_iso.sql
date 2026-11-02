-- Weekly data values put on one calendar: ISO weeks (Monday to Sunday).
--
-- Why: the IDSR Weekly form is ISO-weekly, but the DHIS2 demo stores some
-- weeks as Wednesday weeks (2026WedW45 = Wed 4 Nov .. Tue 10 Nov) instead.
-- Rules:
--   1. Every weekly value is assigned to the ISO week holding most of its
--      days. A Wednesday week has 5 of its 7 days (Wed..Sun) in the ISO week
--      its Wednesday falls in, so that week is the Monday on or before the
--      period start.
--   2. One report per week: if the data set has ANY values in its own period
--      type (ISO) for an ISO week, only those are used; Wednesday weeks only
--      fill ISO weeks that have no ISO data at all.
--      Why week level, not per facility: in the five weeks the demo holds in
--      both forms, each form is a complete report (~206 facilities, only 45
--      in common). Matching facility by facility kept most of both and
--      doubled those weeks' totals.
-- Grain: data set x ISO week x org unit x data element x disaggregation.

with weekly as (

    select
        *,
        period_start_date - cast(isodow(period_start_date) - 1 as integer) as iso_week_start
    from {{ ref('int_dhis2__data_values_enriched') }}
    where data_set_period_type like 'Weekly%'
      and period_type like 'Weekly%'

),

ranked as (

    select
        *,
        bool_or(is_expected_period_type) over (
            partition by data_set_id, iso_week_start
        ) as week_has_native_value
    from weekly

)

select
    md5(concat_ws('|',
        data_set_id, iso_week_start, org_unit_id,
        data_element_id, category_option_combo_id
    ))                                          as weekly_value_key,

    data_set_id,
    data_set_name,

    -- ISO week
    iso_week_start,
    iso_week_start + 6                          as iso_week_end,
    isoyear(iso_week_start) || 'W' || week(iso_week_start) as iso_week,

    -- where the value came from
    period                                      as source_period,
    period_type                                 as source_period_type,
    not is_expected_period_type                 as is_mapped_from_other_week_type,

    -- where
    org_unit_id,
    org_unit_name,
    district_id,
    district_name,
    chiefdom_id,
    chiefdom_name,
    facility_id,
    facility_name,

    -- what
    data_element_id,
    data_element_name,
    category_option_combo_id,
    category_option_combo_name,

    value_numeric

from ranked
where is_expected_period_type
   or not week_has_native_value
