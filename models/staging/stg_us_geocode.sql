{{ config(
    materialized='incremental',
    unique_key='state_abbreviation'
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'us_geocode') }}
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
        state_abbreviation,
        state_full_name,
        latitude,
        longitude,
        _data_source,
        _load_time
    from filtered
)

select * from renamed
