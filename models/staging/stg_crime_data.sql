{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'crime_data') }}
),

filtered as (
    select *
    from source

    {% if is_incremental() %}
      where _load_time > (select max(_load_time) from {{ this }})
    {% endif %}
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
    from filtered
)

select * from renamed
