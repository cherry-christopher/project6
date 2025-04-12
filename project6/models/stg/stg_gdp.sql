with source as (
    select * from {{ source('finance_economics_events_stg', 'gdp') }}
),

renamed as (
    select
        year,
        state,
        category,
        value,
        _data_source,
        _load_time
    from source
)

select * from renamed
