{{ config(order_by = '(data_set_name, data_element_name, district_name, month)') }}

-- Monthly totals per district for every monthly data set (Child Health,
-- Reproductive Health). Grain: data set x month x district x data element
-- x disaggregation. Values whose period does not match their data set
-- (the few daily values in Reproductive Health) are left out.

select
    ifNull(data_set_name, '')               as data_set_name,
    toDate(ifNull(period_start_date, toDate32('1970-01-01'))) as month,
    any(period)                             as period,
    ifNull(district_name, '')               as district_name,
    ifNull(data_element_name, '')           as data_element_name,
    ifNull(category_option_combo_name, '')  as category_option_combo_name,
    sum(value_numeric)                      as value,
    uniqExact(org_unit_id)                  as facilities_reporting
from {{ ref('lake_dhis2__data_values') }}
where data_set_period_type = 'Monthly'
  and is_expected_period_type
group by
    data_set_name, month, district_name, data_element_name, category_option_combo_name
