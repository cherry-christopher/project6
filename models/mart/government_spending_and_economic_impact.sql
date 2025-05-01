{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

WITH exp_data AS (
    SELECT
        SAFE_CAST(EXTRACT(YEAR FROM _load_time) AS INT64) AS year,
        LOWER(REPLACE(state, ' ', '_')) AS state,
        total_cap,
        _data_source,
        _load_time AS exp_load_time
    FROM {{ ref('State_Exp_Data_1991_2024') }}
    WHERE total_cap IS NOT NULL
    {% if is_incremental() %}
      AND _load_time > (SELECT MAX(_load_time) FROM {{ this }})
    {% endif %}
),

gdp_data AS (
    SELECT
        year,
        LOWER(REPLACE(state, ' ', '_')) AS state,
        current_dollar_gdp,
        _data_source,
        _load_time AS gdp_load_time
    FROM {{ ref('Gdp') }}
    {% if is_incremental() %}
      WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
    {% endif %}
),

joined AS (
    SELECT
        e.state,
        e.year,
        e.total_cap,
        g.current_dollar_gdp,
        e.exp_load_time,
        g.gdp_load_time
    FROM exp_data e
    INNER JOIN gdp_data g
      ON e.state = g.state
     AND e.year = g.year
)

SELECT
    state,
    year,
    AVG(total_cap) AS avg_state_expenditure,
    AVG(current_dollar_gdp) AS avg_gdp,
    MAX(exp_load_time) AS exp_load_time,
    MAX(gdp_load_time) AS gdp_load_time
FROM joined
GROUP BY state, year
ORDER BY state, year
