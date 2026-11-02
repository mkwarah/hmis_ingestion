-- Data-quality check: each value's period should match its data set's
-- period type (a Monthly form should hold monthly periods).
-- The DHIS2 demo breaks this twice: IDSR Weekly (defined as ISO Weekly)
-- also holds Wednesday weeks (2026WedW1...), and Reproductive Health
-- (Monthly) holds a few daily values. Summing such a mix double-counts or misplaces data,
-- so the marts must decide how to treat it. Warns, one row per mismatch.

{{ config(severity = 'warn') }}

select
    ds.data_set_name,
    ds.period_type          as expected_period_type,
    dv.period_type          as actual_period_type,
    count(*)                as data_values,
    min(dv.period)          as first_period,
    max(dv.period)          as last_period
from {{ ref('stg_dhis2__data_values') }} as dv
join {{ ref('stg_dhis2__data_sets') }}   as ds on ds.data_set_id = dv.data_set_id
where dv.period_type <> ds.period_type
group by all
