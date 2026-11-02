-- Every harmonised week must run Monday to Sunday.

select *
from {{ ref('int_dhis2__weekly_values_iso') }}
where isodow(iso_week_start) <> 1
   or iso_week_end - iso_week_start <> 6
