{#
    Metadata is landed as a full snapshot on every run. Staging uses only the
    newest one:  select ... from {{ source(...) }} {{ latest_snapshot() }}
#}
{% macro latest_snapshot() -%}
    qualify run_id = max(run_id) over ()
{%- endmacro %}
