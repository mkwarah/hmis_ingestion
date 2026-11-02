{{ config(order_by = '(data_set_name, district_name, month)') }}

-- Reporting rate per monthly data set, month and district:
--   facilities assigned the form that reported any value that month
--   / facilities assigned the form (DHIS2 data set assignments).
-- Every (data set, district, month) appears, so a district that reported
-- nothing shows 0%, instead of silently dropping out of the denominator.
-- Months are those in which the data set has any data at all.
-- "Any value" is a proxy for a submitted report; DHIS2's own reporting rate
-- uses completeness registrations, which are not ingested yet.

with assigned as (

    select
        ifNull(data_set_name, '')           as data_set_name,
        ifNull(district_name, '')           as district_name,
        org_unit_id
    from {{ ref('lake_dhis2__data_set_assignments') }}
    where data_set_period_type = 'Monthly'

),

expected as (

    select data_set_name, district_name, count() as facilities_expected
    from assigned
    group by data_set_name, district_name

),

values as (

    select
        ifNull(data_set_name, '')           as data_set_name,
        toDate(ifNull(period_start_date, toDate32('1970-01-01'))) as month,
        ifNull(district_name, '')           as district_name,
        org_unit_id
    from {{ ref('lake_dhis2__data_values') }}
    where data_set_period_type = 'Monthly'
      and is_expected_period_type

),

months as (

    select distinct data_set_name, month
    from values

),

reported as (

    select
        v.data_set_name,
        v.month,
        v.district_name,
        uniqExact(v.org_unit_id)                          as facilities_reporting_any,
        uniqExactIf(v.org_unit_id, a.org_unit_id != '')   as facilities_reporting
    from values as v
    left join assigned as a
        on  a.org_unit_id = v.org_unit_id
        and a.data_set_name = v.data_set_name
    group by v.data_set_name, v.month, v.district_name

),

grid as (

    select e.data_set_name, m.month, e.district_name, e.facilities_expected
    from expected as e
    inner join months as m on m.data_set_name = e.data_set_name

)

select
    g.data_set_name                                       as data_set_name,
    g.month                                               as month,
    g.district_name                                       as district_name,
    ifNull(r.facilities_reporting, 0)                     as facilities_reporting,
    g.facilities_expected                                 as facilities_expected,
    round(100 * ifNull(r.facilities_reporting, 0) / nullIf(g.facilities_expected, 0), 1) as reporting_rate_pct,
    ifNull(r.facilities_reporting_any, 0) - ifNull(r.facilities_reporting, 0) as facilities_reporting_unassigned
from grid as g
left join reported as r
    on  r.data_set_name = g.data_set_name
    and r.month = g.month
    and r.district_name = g.district_name
