{#
    Pseudonymise a personal identifier: salted SHA-256 of the trimmed,
    lower-cased value. The same person always gets the same code (so values
    can still be grouped per data-entry user), but the code cannot be turned
    back into the name without the salt.

    The salt is a secret (HMIS_HASH_SALT) handed to DuckDB by the hmis_pii
    plugin as a session variable, so it never appears in compiled SQL. If it
    is missing the query fails rather than silently producing NULL hashes.

      {{ hash_pii('storedBy') }}   ->   64-character hex string, or NULL
#}
{% macro hash_pii(column) -%}
    case
        when {{ column }} is null or trim({{ column }}) = '' then null
        else sha256(
            coalesce(getvariable('hmis_hash_salt'),
                     error('hmis_hash_salt is not set: is the hmis_pii plugin enabled and HMIS_HASH_SALT exported?'))
            || lower(trim({{ column }}))
        )
    end
{%- endmacro %}
