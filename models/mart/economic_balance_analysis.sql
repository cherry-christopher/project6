{{ config(
    materialized='incremental',
    unique_key=['state'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH gdp_data AS (
  SELECT
    LOWER(REPLACE(state, ' ', '_')) AS state,
    AVG(personal_income_gdp) AS avg_personal_income_gdp,
    AVG(real_gdp) AS avg_real_gdp,
    AVG(current_dollar_gdp) AS avg_current_dollar_gdp
  FROM {{ ref('Gdp') }}
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
  GROUP BY state
),

energy_data AS (
  SELECT
    LOWER(REPLACE(state, ' ', '_')) AS state,
    AVG(value) AS avg_energy_consumption
  FROM {{ ref('State_Energy_Consumption') }}
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
  GROUP BY state
),

unemployment_data AS (
  SELECT
    LOWER(REPLACE(state_and_area, ' ', '_')) AS state,
    AVG(unemployment) AS avg_unemployment_rate
  FROM {{ ref('State_Level_Unemployment_Data') }}
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
  GROUP BY state
)

SELECT
  gdp.state,
  gdp.avg_personal_income_gdp,
  gdp.avg_real_gdp,
  gdp.avg_current_dollar_gdp,
  energy.avg_energy_consumption,
  unemployment.avg_unemployment_rate
FROM gdp_data AS gdp
LEFT JOIN energy_data AS energy ON gdp.state = energy.state
LEFT JOIN unemployment_data AS unemployment ON gdp.state = unemployment.state
ORDER BY gdp.state
