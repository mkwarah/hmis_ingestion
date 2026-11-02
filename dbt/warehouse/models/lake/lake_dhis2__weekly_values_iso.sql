-- Doorway from the lake: weekly values already harmonised to ISO weeks.
select *
from {{ silo_parquet('intermediate', 'int_dhis2__weekly_values_iso') }}
