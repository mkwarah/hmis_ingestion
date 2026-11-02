-- One report per week: an ISO week of a data set must be built from a
-- single source week type. Mixing ISO and Wednesday reports for the same
-- week double-counts it (first version of int_dhis2__weekly_values_iso did
-- exactly that: weeks 2026W11, W24 and W46 came out at twice the usual total).

select
    data_set_name,
    iso_week,
    count(distinct source_period_type) as source_week_types
from {{ ref('int_dhis2__weekly_values_iso') }}
group by data_set_name, iso_week
having count(distinct source_period_type) > 1
