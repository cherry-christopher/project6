{{ config(
    materialized='incremental',
    unique_key='state_id'
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'states_cities') }}
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
        state_id,
        cities,
        state_name,
        latitude,
        longitude,
        total_population,
        avg_density,
        timezones,
        _data_source,
        _load_time
    from filtered
)

select * from renamed
