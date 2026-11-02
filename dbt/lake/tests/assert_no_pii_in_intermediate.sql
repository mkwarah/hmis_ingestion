-- PII boundary: no intermediate model may carry personal data, so nothing
-- downstream (the ClickHouse gold layer, dashboards) can expose it.
-- Fails if any intermediate model has a column that holds or derives from
-- the data-entry user or the free-text comment.
-- (Depends on the intermediate models so it runs after they are built.)
-- depends_on: {{ ref('int_dhis2__data_values_enriched') }}
-- depends_on: {{ ref('int_dhis2__weekly_values_iso') }}
-- depends_on: {{ ref('int_dhis2__data_set_assignments') }}
-- depends_on: {{ ref('int_dhis2__org_unit_hierarchy') }}

select
    table_name,
    column_name
from information_schema.columns
where table_schema = 'intermediate'
  and (
        lower(column_name) like '%stored_by%'
     or lower(column_name) = 'comment'
     or lower(column_name) like '%username%'
     or lower(column_name) like '%email%'
     or lower(column_name) like '%phone%'
  )
