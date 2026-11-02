{{ config(order_by = '(data_set_name, district_name, month)') }}

-- How many facilities in each district reported each month, per monthly
-- data set. A reporting-rate *proxy*: the denominator is all facilities in
-- the district, not the facilities the data set is assigned to (assignments
-- are not ingested yet), so rates can stay below 100% even when every
-- expected facility reported.

with reported as (

    select
        ifNull(data_set_name, '')           as data_set_name,
        toDate(ifNull(period_start_date, toDate32('1970-01-01'))) as month,
        ifNull(district_name, '')           as district_name,
        uniqExact(org_unit_id)              as facilities_reporting
    from {{ ref('lake_dhis2__data_values') }}
    where data_set_period_type = 'Monthly'
      and is_expected_period_type
    group by data_set_name, month, district_name

),

facilities as (

    select
        district_name,
        count()                             as facilities_total
    from {{ ref('dim_org_unit') }}
    where org_unit_level = 4
    group by district_name

)

select
    r.data_set_name,
    r.month,
    r.district_name,
    r.facilities_reporting,
    f.facilities_total,
    round(100 * r.facilities_reporting / nullIf(f.facilities_total, 0), 1) as reporting_rate_pct
from reported as r
left join facilities as f
    on f.district_name = r.district_name
