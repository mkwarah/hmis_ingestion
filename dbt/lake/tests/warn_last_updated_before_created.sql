-- Data-quality check: a value cannot be updated before it was created.
-- The DHIS2 demo database fails this (its dates were shifted when it was
-- copied forward), so it warns instead of failing the build. On a real
-- national instance, make it an error.

{{ config(severity = 'warn') }}

select
    data_value_key,
    data_set_id,
    period,
    created_at,
    last_updated_at
from {{ ref('stg_dhis2__data_values') }}
where last_updated_at < created_at
