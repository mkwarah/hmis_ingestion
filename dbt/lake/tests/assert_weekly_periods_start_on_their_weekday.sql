-- Checks the period macro: every weekly period must start on the weekday its
-- type says (ISO: Monday = 1 ... Sunday = 7) and last exactly 7 days.
-- Returns the offending rows; dbt fails the test if any come back.

select distinct
    period,
    period_type,
    period_start_date,
    period_end_date,
    isodow(period_start_date) as start_dow
from {{ ref('stg_dhis2__data_values') }}
where period_type like 'Weekly%'
  and (
        isodow(period_start_date) <> case period_type
            when 'Weekly'          then 1
            when 'WeeklyWednesday' then 3
            when 'WeeklyThursday'  then 4
            when 'WeeklySaturday'  then 6
            when 'WeeklySunday'    then 7
        end
     or period_end_date - period_start_date <> 6
  )
