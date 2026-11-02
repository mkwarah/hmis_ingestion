-- A reporting rate counts only assigned facilities in its numerator, so it
-- can never exceed 100% or the number of facilities expected.

select *
from {{ ref('fct_reporting_monthly_district') }}
where reporting_rate_pct > 100
   or facilities_reporting > facilities_expected
