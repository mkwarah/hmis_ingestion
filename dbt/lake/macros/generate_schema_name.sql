{#
    By default dbt prefixes custom schemas with the target schema
    (main_staging, main_intermediate). Use the layer name on its own so the
    database reads like the medallion diagram: staging.*, intermediate.*
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
