{#
    Read a lake model's Parquet file from Silo.

      {{ silo_parquet('intermediate', 'int_dhis2__data_values_enriched') }}
      -> s3(silo, filename = 'intermediate/dhis2/int_dhis2__data_values_enriched.parquet', format = 'Parquet')

    `silo` is a ClickHouse named collection (clickhouse/config.d/silo.xml)
    holding the bucket URL and credentials, so no secret appears in SQL.
    The path matches what ../lake writes (its macros/external_location.sql).
#}
{% macro silo_parquet(layer, model_name, domain='dhis2') -%}
    s3(silo, filename = '{{ layer }}/{{ domain }}/{{ model_name }}.parquet', format = 'Parquet')
{%- endmacro %}
