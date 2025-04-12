with source as (
    select * from {{ source('finance_economics_events_stg', 'state_level_unemployment_data') }}
),

renamed as (
    select
        year,
        fips_code,
        state_and_area,
        employment,
        unemployment
    from source
)

select * from renamed
