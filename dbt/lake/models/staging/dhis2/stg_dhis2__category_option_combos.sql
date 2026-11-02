with source as (

    select * from {{ source('landing', 'category_option_combos') }}

)

select
    id                                  as category_option_combo_id,
    nullif(trim(code), '')              as category_option_combo_code,
    trim(name)                          as category_option_combo_name,
    cast(last_updated as timestamp)     as last_updated_at,
    ingested_at

from source
