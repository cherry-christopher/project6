{{ config(
    materialized='incremental',
    unique_key=['year', 'state'],
    constraints=[
      {
        "type": "primary_key",
        "columns": ["year", "state"]
      },
      {
        "type": "foreign_key",
        "columns": ["state"],
        "to": "us_geocode",
        "to_columns": ["state_full_name"]
      }
    ]
) }}

WITH base AS (
  SELECT
    CAST(FLOOR(year) AS INT64) AS year,
    LOWER(REPLACE(state, ' ', '_')) AS state,
    category,
    value,
    _data_source,
    _load_time AS gdp_load_time
  FROM {{ ref('stg_gdp') }}
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
),

filtered AS (
  SELECT *
  FROM base
  WHERE state IN (
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
    'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa',
    'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan',
    'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new_hampshire', 'new_jersey', 'new_mexico', 'new_york', 'north_carolina',
    'north_dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode_island',
    'south_carolina', 'south_dakota', 'tennessee', 'texas', 'utah', 'vermont',
    'virginia', 'washington', 'west_virginia', 'wisconsin', 'wyoming'
  )
),

categorized AS (
  SELECT
    year,
    state,
    MAX(CASE WHEN category = 'personal_income_gdp' THEN value END) AS personal_income_gdp,
    MAX(CASE WHEN category = 'Real gross domestic product' THEN value END) AS real_gdp,
    MAX(CASE WHEN category = 'Current-dollar gross domestic product' THEN value END) AS current_dollar_gdp,
    _data_source,
    MAX(gdp_load_time) AS gdp_load_time
  FROM filtered
  GROUP BY year, state, _data_source
),

normalized AS (
  SELECT
    year,
    state,
    AVG(personal_income_gdp) * 4 AS personal_income_gdp,
    AVG(real_gdp) * 4 AS real_gdp,
    AVG(current_dollar_gdp) * 4 AS current_dollar_gdp,
    _data_source,
    MAX(gdp_load_time) AS gdp_load_time
  FROM categorized
  WHERE year != 2024
  GROUP BY year, state, _data_source
)

SELECT
  year,
  state,
  personal_income_gdp,
  real_gdp,
  current_dollar_gdp,
  _data_source,
  gdp_load_time AS _load_time
FROM normalized
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY 
    year,
    state,
    CAST(personal_income_gdp AS STRING),
    CAST(real_gdp AS STRING),
    CAST(current_dollar_gdp AS STRING)
  ORDER BY gdp_load_time DESC
) = 1
ORDER BY year, state
