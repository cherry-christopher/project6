{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

WITH base AS (
    SELECT
        state,
        CAST(year AS INT64) AS year,
        value AS energy_consumption,
        _load_time
    FROM {{ ref('State_Energy_Consumption') }}
    {% if is_incremental() %}
      WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
    {% endif %}
),

aggregated AS (
    SELECT
        state,
        year,
        AVG(energy_consumption) AS avg_energy_consumption
    FROM base
    GROUP BY state, year
)

SELECT *
FROM aggregated
