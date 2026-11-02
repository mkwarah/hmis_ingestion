{{ config(order_by = 'org_unit_id') }}

-- One row per organisation unit with its place in the hierarchy.
-- ClickHouse reads Parquet columns as Nullable; ifNull() makes the key
-- columns non-nullable so they can be used in ORDER BY.

select
    ifNull(org_unit_id, '')         as org_unit_id,
    org_unit_name,
    ifNull(org_unit_level, 0)       as org_unit_level,
    country_name,
    district_id,
    district_name,
    chiefdom_id,
    chiefdom_name,
    facility_id,
    facility_name
from {{ ref('lake_dhis2__org_unit_hierarchy') }}
