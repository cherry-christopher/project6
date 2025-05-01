{{ config(
    materialized='incremental',
    unique_key=['state', 'year']
) }}

WITH base AS (
  SELECT
    c.state,
    c.year,
    c.crime AS crime_rate,
    g.real_gdp,
    u.unemployment,
    c._load_time AS crime_load_time,
    g._load_time AS gdp_load_time,
    u._load_time AS unemployment_load_time
  FROM {{ ref('Crime_Data') }} c
  JOIN {{ ref('Gdp') }} g
    ON c.state = g.state AND c.year = g.year
  JOIN {{ ref('State_Level_Unemployment_Data') }} u
    ON c.state = u.state_and_area AND c.year = u.year
),

filtered AS (
  SELECT *
  FROM base
  {% if is_incremental() %}
  WHERE
    GREATEST(crime_load_time, gdp_load_time, unemployment_load_time) > (
      SELECT COALESCE(MAX(_load_time), '1900-01-01')
      FROM {{ this }}
    )
  {% endif %}
)

SELECT
  state,
  year,
  AVG(crime_rate) AS avg_crime_rate,
  AVG(real_gdp) AS avg_gdp,
  AVG(unemployment) AS avg_unemployment_rate,
  MAX(GREATEST(crime_load_time, gdp_load_time, unemployment_load_time)) AS _load_time
FROM filtered
GROUP BY state, year
