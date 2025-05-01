{{ config(
    materialized='incremental',
    unique_key='state'
) }}

WITH joined AS (
  SELECT
    g.state,
    g.real_gdp,
    e.value AS energy_consumption,
    u.unemployment,
    g._load_time AS gdp_load_time,
    e._load_time AS energy_load_time,
    u._load_time AS unemployment_load_time
  FROM {{ ref('Gdp') }} g
  JOIN {{ ref('State_Energy_Consumption') }} e
    ON g.state = e.state
  JOIN {{ ref('State_Level_Unemployment_Data') }} u
    ON g.state = u.state_and_area
  {% if is_incremental() %}
    WHERE GREATEST(g._load_time, e._load_time, u._load_time) > (
      SELECT COALESCE(MAX(_load_time), '1900-01-01') FROM {{ this }}
    )
  {% endif %}
),

aggregated AS (
  SELECT
    state,
    AVG(real_gdp) AS avg_gdp,
    AVG(energy_consumption) AS avg_energy_consumption,
    AVG(unemployment) AS avg_unemployment_rate,
    MAX(GREATEST(gdp_load_time, energy_load_time, unemployment_load_time)) AS latest_load_time
  FROM joined
  GROUP BY state
)

SELECT
  state,
  avg_gdp,
  avg_energy_consumption,
  avg_unemployment_rate,
  latest_load_time AS _load_time
FROM aggregated
ORDER BY state
