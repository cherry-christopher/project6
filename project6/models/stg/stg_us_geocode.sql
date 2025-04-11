with source as (
    select * from {{ source('finance_economics_events_stg', 'us_geocode') }}
),

renamed as (
    select
        state_abbreviation,
        state_full_name,
        latitude,
        longitude
    from source
)

select * from renamed
