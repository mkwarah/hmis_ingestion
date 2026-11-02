-- Doorway from the lake: the org unit hierarchy in Silo, read live.
select *
from {{ silo_parquet('intermediate', 'int_dhis2__org_unit_hierarchy') }}
