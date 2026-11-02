{#
    DHIS2 period IDs -> period type, start date, end date.

      20260915     Daily
      2026W6       Weekly            (weeks start Monday, ISO 8601)
      2026WedW6    WeeklyWednesday   (also ThuW, SatW, SunW)
      202609       Monthly
      2026Q3       Quarterly
      2026S2       SixMonthly
      2026         Yearly

    Weekly rule (checked against the DHIS2 API): week 1 is the week that
    contains 4 January, so it starts on the first start-day on or after
    29 December. Example: 2026WedW1 = 2025-12-31 .. 2026-01-06.

    Usage in a model:
      {{ dhis2_period_type('period') }}                      as period_type
      {{ dhis2_period_start_date('period', 'period_type') }} as period_start_date
      {{ dhis2_period_end_date('period_start_date', 'period_type') }} as period_end_date
#}

{% macro dhis2_period_type(period) -%}
    case
        when regexp_matches({{ period }}, '^\d{8}$')            then 'Daily'
        when regexp_matches({{ period }}, '^\d{4}W\d{1,2}$')    then 'Weekly'
        when regexp_matches({{ period }}, '^\d{4}WedW\d{1,2}$') then 'WeeklyWednesday'
        when regexp_matches({{ period }}, '^\d{4}ThuW\d{1,2}$') then 'WeeklyThursday'
        when regexp_matches({{ period }}, '^\d{4}SatW\d{1,2}$') then 'WeeklySaturday'
        when regexp_matches({{ period }}, '^\d{4}SunW\d{1,2}$') then 'WeeklySunday'
        when regexp_matches({{ period }}, '^\d{6}$')            then 'Monthly'
        when regexp_matches({{ period }}, '^\d{4}Q[1-4]$')      then 'Quarterly'
        when regexp_matches({{ period }}, '^\d{4}S[12]$')       then 'SixMonthly'
        when regexp_matches({{ period }}, '^\d{4}$')            then 'Yearly'
    end
{%- endmacro %}


{# ISO day of week each weekly type starts on (Monday = 1 ... Sunday = 7). #}
{% macro _dhis2_week_start_dow(period_type) -%}
    case {{ period_type }}
        when 'Weekly'          then 1
        when 'WeeklyWednesday' then 3
        when 'WeeklyThursday'  then 4
        when 'WeeklySaturday'  then 6
        when 'WeeklySunday'    then 7
    end
{%- endmacro %}


{% macro dhis2_period_start_date(period, period_type) -%}
    {%- set yr = "cast(left(" ~ period ~ ", 4) as integer)" -%}
    {%- set jan4 = "make_date(" ~ yr ~ ", 1, 4)" -%}
    case
        when {{ period_type }} = 'Daily'
            then strptime({{ period }}, '%Y%m%d')::date
        when {{ period_type }} like 'Weekly%'
            -- start of week 1, then add (week - 1) weeks
            then {{ jan4 }}
                 - cast((isodow({{ jan4 }}) - {{ _dhis2_week_start_dow(period_type) }} + 7) % 7 as integer)
                 + 7 * (cast(regexp_extract({{ period }}, 'W(\d+)$', 1) as integer) - 1)
        when {{ period_type }} = 'Monthly'
            then make_date({{ yr }}, cast(substr({{ period }}, 5, 2) as integer), 1)
        when {{ period_type }} = 'Quarterly'
            then make_date({{ yr }}, (cast(right({{ period }}, 1) as integer) - 1) * 3 + 1, 1)
        when {{ period_type }} = 'SixMonthly'
            then make_date({{ yr }}, (cast(right({{ period }}, 1) as integer) - 1) * 6 + 1, 1)
        when {{ period_type }} = 'Yearly'
            then make_date({{ yr }}, 1, 1)
    end
{%- endmacro %}


{% macro dhis2_period_end_date(start_date, period_type) -%}
    case
        when {{ period_type }} = 'Daily'      then {{ start_date }}
        when {{ period_type }} like 'Weekly%' then {{ start_date }} + 6
        when {{ period_type }} = 'Monthly'    then cast({{ start_date }} + interval 1 month as date) - 1
        when {{ period_type }} = 'Quarterly'  then cast({{ start_date }} + interval 3 month as date) - 1
        when {{ period_type }} = 'SixMonthly' then cast({{ start_date }} + interval 6 month as date) - 1
        when {{ period_type }} = 'Yearly'     then cast({{ start_date }} + interval 1 year as date) - 1
    end
{%- endmacro %}
