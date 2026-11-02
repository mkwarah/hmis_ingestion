{{ config(order_by = '(data_element_name, district_name, week_start)') }}

-- Weekly disease surveillance (IDSR) totals per district on ISO weeks.
-- Grain: ISO week x district x data element x disaggregation.
-- Week harmonising (Wednesday weeks moved to ISO weeks, ISO values winning
-- where both exist) happens in the lake: int_dhis2__weekly_values_iso.

select
    toDate(ifNull(iso_week_start, toDate32('1970-01-01'))) as week_start,
    toDate(any(iso_week_end))               as week_end,
    any(iso_week)                           as iso_week,
    ifNull(district_name, '')               as district_name,
    ifNull(data_element_name, '')           as data_element_name,
    ifNull(category_option_combo_name, '')  as category_option_combo_name,
    sum(value_numeric)                      as value,
    uniqExact(org_unit_id)                  as facilities_reporting,
    countIf(is_mapped_from_other_week_type) as values_from_wednesday_weeks
from {{ ref('lake_dhis2__weekly_values_iso') }}
where data_set_name = 'IDSR Weekly'
group by
    week_start, district_name, data_element_name, category_option_combo_name
