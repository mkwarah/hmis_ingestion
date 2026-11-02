-- Latest disaggregation (category option combo) snapshot.

with source as (

    select * from {{ source('landing', 'category_option_combos') }}
    {{ latest_snapshot() }}

)

select
    id                                  as category_option_combo_id,
    nullif(trim(code), '')              as category_option_combo_code,
    trim(name)                          as category_option_combo_name,
    try_cast(lastUpdated as timestamp)  as last_updated_at,
    run_id

from source
