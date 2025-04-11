with source as (
    select * from {{ source('finance_economics_events_stg', 'state_exp_data_1991_2024') }}
),

renamed as (
    select
        * 
    from source
)

select * from renamed
