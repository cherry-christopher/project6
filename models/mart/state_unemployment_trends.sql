{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

WITH base AS (
    SELECT
        LOWER(REPLACE(state_and_area, ' ', '_')) AS state,
        CAST(year AS INT64) AS year,
        unemployment,
        _load_time
    FROM {{ ref('State_Level_Unemployment_Data') }}
    {% if is_incremental() %}
      WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
    {% endif %}
),

aggregated AS (
    SELECT
        state,
        year,
        AVG(unemployment) AS avg_unemployment_rate
    FROM base
    GROUP BY state, year
)

SELECT *
FROM aggregated
