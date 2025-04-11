with source as (
    select * from {{ source('finance_economics_events_stg', 'mortality_and_natality') }}
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
    from source
)

select * from renamed
