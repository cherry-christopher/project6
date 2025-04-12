with source as (
    select * from {{ source('finance_economics_events_stg', 'state_exp_data_1991_2024_key') }}
),

renamed as (
    select
        the_abbreviation,
        the_full_name
    from source
)

select * from renamed
