-- Doorway from the lake: intermediate data values in Silo, read live.
-- A view, so ClickHouse re-reads the Parquet each time the marts build.
select *
from {{ silo_parquet('intermediate', 'int_dhis2__data_values_enriched') }}
