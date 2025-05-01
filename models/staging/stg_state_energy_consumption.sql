{{ config(
    materialized='incremental',
    unique_key=['state', 'year', 'sector']
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'state_energy_consumption') }}
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
        value,
        sector,
        _data_source,
        _load_time
    from filtered
)

select * from renamed
