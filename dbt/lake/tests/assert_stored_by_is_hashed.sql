-- Every value whose landed payload names a data-entry user must carry a
-- hash in staging, and the hash must look like SHA-256 (64 hex characters).
-- Catches a missing salt or a broken hashing macro before PII handling is
-- silently skipped.

with landed as (

    select count(*) as values_with_user
    from (
        select unnest(dataValues) as dv
        from {{ source('landing', 'data_values') }}
    )
    where nullif(trim(dv.storedBy), '') is not null

),

staged as (

    select
        count(*) filter (where stored_by_hash is not null)            as hashed,
        count(*) filter (where stored_by_hash is not null
                           and not regexp_matches(stored_by_hash, '^[0-9a-f]{64}$')) as malformed
    from {{ ref('stg_dhis2__data_values') }}

)

select *
from landed, staged
where (landed.values_with_user > 0 and staged.hashed = 0)
   or staged.malformed > 0
