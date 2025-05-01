{{ config(
    materialized='incremental',
    unique_key=['state', 'year', 'month', 'indicator']
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'mortality_and_natality') }}
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
        month,
        period,
        indicator,
        data_value,
        _data_source,
        _load_time
    from filtered
)

select * from renamed
