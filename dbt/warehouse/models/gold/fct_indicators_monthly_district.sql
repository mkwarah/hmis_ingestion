{{ config(order_by = '(indicator_code, district_name, month)') }}

-- Derived HMIS indicators per district and month, in long format: one row
-- per indicator x district x month with its NUMERATOR and DENOMINATOR.
--
-- Why keep both parts: rates must be re-aggregated from their parts, never
-- averaged. National BCG coverage = sum(doses) / sum(target population),
-- not the mean of 13 district percentages. Dashboards compute
--   sum(numerator) / sum(denominator) * multiplier
-- for any grouping; `value` is the ready-made district-month figure.
--
-- Coverage denominators come from the yearly Population data set, spread
-- evenly over the year (yearly target / 12 per month), the usual way monthly
-- coverage is annualised in DHIS2.

with monthly as (

    select
        toDate(ifNull(period_start_date, toDate32('1970-01-01'))) as month,
        ifNull(district_name, '')                                as district_name,
        sumIf(value_numeric, data_element_name = 'BCG doses given')        as bcg,
        sumIf(value_numeric, data_element_name = 'Penta1 doses given')     as penta1,
        sumIf(value_numeric, data_element_name = 'Penta3 doses given')     as penta3,
        sumIf(value_numeric, data_element_name = 'Measles doses given')    as measles,
        sumIf(value_numeric, data_element_name = 'ANC 1st visit')          as anc1,
        sumIf(value_numeric, data_element_name = 'ANC 4th or more visits') as anc4,
        sumIf(value_numeric, data_element_name = 'Live births')            as live_births,
        sumIf(value_numeric, data_element_name = 'Still births')           as still_births,
        sumIf(value_numeric, data_element_name = 'Low birth weight in PHU') as low_birth_weight
    from {{ ref('lake_dhis2__data_values') }}
    where data_set_period_type = 'Monthly'
      and period_type = 'Monthly'
      and is_expected_period_type
    group by month, district_name

),

population as (

    select
        toYear(ifNull(period_start_date, toDate32('1970-01-01')))   as year,
        ifNull(district_name, '')                                   as district_name,
        sumIf(value_numeric, data_element_name = 'Total population < 1 year') as under1_population,
        sumIf(value_numeric, data_element_name = 'Expected pregnancies')      as expected_pregnancies
    from {{ ref('lake_dhis2__data_values') }}
    where data_set_name = 'Population'
      and period_type = 'Yearly'
    group by year, district_name

),

inputs as (

    select
        m.*,
        p.under1_population / 12       as monthly_target_under1,
        p.expected_pregnancies / 12    as monthly_expected_pregnancies
    from monthly as m
    left join population as p
        on p.year = toYear(m.month) and p.district_name = m.district_name

),

indicators as (

    -- Immunisation coverage (annualised): doses / (under-1 population / 12)
    select month, district_name, 'IMM_BCG_COV' as indicator_code,
           'BCG coverage' as indicator_name, 'Immunisation' as category,
           bcg as numerator, monthly_target_under1 as denominator, 100 as multiplier, '%' as unit
    from inputs
    union all
    select month, district_name, 'IMM_PENTA3_COV', 'Penta3 coverage', 'Immunisation',
           penta3, monthly_target_under1, 100, '%'
    from inputs
    union all
    select month, district_name, 'IMM_MEASLES_COV', 'Measles coverage', 'Immunisation',
           measles, monthly_target_under1, 100, '%'
    from inputs
    union all
    -- Dropout: share of children who started the series but did not finish
    select month, district_name, 'IMM_PENTA_DROPOUT', 'Penta1 to Penta3 dropout', 'Immunisation',
           penta1 - penta3, penta1, 100, '%'
    from inputs
    union all
    -- Maternal health
    select month, district_name, 'MAT_ANC1_COV', 'ANC 1st visit coverage', 'Maternal health',
           anc1, monthly_expected_pregnancies, 100, '%'
    from inputs
    union all
    select month, district_name, 'MAT_ANC_DROPOUT', 'ANC1 to ANC4 dropout', 'Maternal health',
           anc1 - anc4, anc1, 100, '%'
    from inputs
    union all
    select month, district_name, 'MAT_LBW_PCT', 'Low birth weight (PHU) per live births', 'Maternal health',
           low_birth_weight, live_births, 100, '%'
    from inputs
    union all
    select month, district_name, 'MAT_STILLBIRTH_RATE', 'Stillbirth rate', 'Maternal health',
           still_births, live_births + still_births, 1000, 'per 1,000 births'
    from inputs

)

select
    month,
    district_name,
    indicator_code,
    indicator_name,
    category,
    numerator,
    denominator,
    multiplier,
    unit,
    round(numerator / nullIf(denominator, 0) * multiplier, 1) as value
from indicators
-- A district-month with no input data at all for an indicator (e.g. no
-- Child Health report that month) has no indicator, not a zero one.
where not (numerator is null and denominator is null)
