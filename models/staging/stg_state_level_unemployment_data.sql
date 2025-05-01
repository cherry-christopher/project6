{{ config(
    materialized='incremental',
    unique_key=['fips_code', 'year']
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'state_level_unemployment_data') }}
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
        fips_code,
        state_and_area,
        employment,
        unemployment,
        _data_source,
        _load_time
    from filtered
)

select * from renamed
