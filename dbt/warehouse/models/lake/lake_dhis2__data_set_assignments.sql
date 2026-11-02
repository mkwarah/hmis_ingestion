-- Doorway from the lake: which facilities are assigned each form.
select *
from {{ silo_parquet('intermediate', 'int_dhis2__data_set_assignments') }}
