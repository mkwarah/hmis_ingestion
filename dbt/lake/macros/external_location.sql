{#
    Where dbt-duckdb writes each `external` model. The adapter's default is
    <external_root>/<model>.parquet, all in one folder. This override files
    each model under its layer so the bucket mirrors the medallion layout:

      s3://hmis-datalake/staging/dhis2/stg_dhis2__data_values.parquet
      s3://hmis-datalake/intermediate/dhis2/int_dhis2__data_values_enriched.parquet

    external_root (the bucket) is set in profiles.yml; relation.schema is the
    layer (staging / intermediate) from dbt_project.yml.
#}
{%- macro external_location(relation, config) -%}
    {%- set format = config.get('format', 'parquet') -%}
    {%- set folder = adapter.external_root() ~ '/' ~ relation.schema ~ '/dhis2' -%}
    {%- if config.get('options', {}).get('partition_by') is none -%}
        {{- folder }}/{{ relation.identifier }}.{{ format }}
    {%- else -%}
        {{- folder }}/{{ relation.identifier }}
    {%- endif -%}
{%- endmacro -%}
