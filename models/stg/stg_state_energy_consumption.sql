with source as (
    select * from {{ source('finance_economics_events_stg', 'state_energy_consumption') }}
),

renamed as (
    select
        year,
        state,
        value,
        sector,
        _data_source,
        _load_time
    from source
)

select * from renamed
