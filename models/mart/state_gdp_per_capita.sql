{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

WITH base AS (
    SELECT
        state,
        CAST(year AS INT64) AS year,
        real_gdp,
        _load_time
    FROM {{ ref('Gdp') }}
    {% if is_incremental() %}
      WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
    {% endif %}
),

aggregated AS (
    SELECT
        state,
        year,
        AVG(real_gdp) AS avg_gdp_per_capita
    FROM base
    GROUP BY state, year
)

SELECT *
FROM aggregated
