-- Reconciliation: aggregating must not lose or invent data. For each data
-- set, the gold total must equal the total of the lake rows it is built
-- from: monthly values of the data set's own period type, and the weekly
-- values already harmonised to ISO weeks. Returns data sets that disagree.

with lake as (

    select data_set_name, sum(value_numeric) as lake_total
    from {{ ref('lake_dhis2__data_values') }}
    where data_set_period_type = 'Monthly'
      and is_expected_period_type
    group by data_set_name

    union all

    select data_set_name, sum(value_numeric) as lake_total
    from {{ ref('lake_dhis2__weekly_values_iso') }}
    where data_set_name = 'IDSR Weekly'
    group by data_set_name

),

gold as (

    select data_set_name, sum(value) as gold_total
    from {{ ref('fct_monthly_indicators_district') }}
    group by data_set_name

    union all

    select 'IDSR Weekly' as data_set_name, sum(value) as gold_total
    from {{ ref('fct_weekly_idsr_district') }}

)

select
    l.data_set_name,
    l.lake_total,
    g.gold_total
from lake as l
left join gold as g on g.data_set_name = l.data_set_name
where abs(l.lake_total - ifNull(g.gold_total, 0)) > 0.001
