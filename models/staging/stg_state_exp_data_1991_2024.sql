{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'state_exp_data_1991_2024') }}
),

filtered as (
    select *
    from source
    where year is not null
    {% if is_incremental() %}
      and _load_time > (select max(_load_time) from {{ this }})
    {% endif %}
)

select * from filtered
