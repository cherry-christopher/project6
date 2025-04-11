with source as (
    select * from {{ source('finance_economics_events_stg', 'states_cities') }}
),

renamed as (
    select
        state_id,
        cities,
        state_name,
        latitude,
        longitude,
        total_population,
        avg_density,
        timezones
    from source
)

select * from renamed
