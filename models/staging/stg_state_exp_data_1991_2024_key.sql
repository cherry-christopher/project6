{{ config(
    materialized='incremental',
    unique_key='the_abbreviation'
) }}

with source as (
    select * from {{ source('finance_economics_events_stg', 'state_exp_data_1991_2024_key') }}
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
        the_abbreviation,
        the_full_name,
        _data_source,
        _load_time
    from filtered
)

select * from renamed
