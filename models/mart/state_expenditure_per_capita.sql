{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

WITH base AS (
    SELECT
        state,
        CAST(year AS INT64) AS year,
        total_cap AS state_expenditure_per_capita,
        _load_time
    FROM {{ ref('State_Exp_Data_1991_2024') }}
    {% if is_incremental() %}
      WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
    {% endif %}
),

aggregated AS (
    SELECT
        state,
        year,
        AVG(state_expenditure_per_capita) AS avg_state_expenditure_per_capita
    FROM base
    GROUP BY state, year
)

SELECT *
FROM aggregated
