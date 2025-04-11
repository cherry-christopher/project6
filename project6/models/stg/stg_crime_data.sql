with source as (
    select * from {{ source('finance_economics_events_stg', 'crime_data') }}
),

renamed as (
    select
        year,
        state,
        population,
        crime,
        crime_against_persons,
        crime_against_property,
        crime_against_society,
        _data_source,
        _load_time
    from source
)

select * from renamed
