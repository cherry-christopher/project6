{{ config(
    materialized='incremental',
    unique_key=['state', 'year', 'category']
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'gdp') }}
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
        category,
        value,
        _data_source,
        _load_time
    from filtered
)

select * from renamed

