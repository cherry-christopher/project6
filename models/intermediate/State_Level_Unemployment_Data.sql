{{ config(
    materialized='incremental',
    unique_key=['year', 'state_and_area'],
    constraints=[
      {
        "type": "primary_key",
        "columns": ["year", "state_and_area"]
      },
      {
        "type": "foreign_key",
        "columns": ["state_and_area"],
        "to": "us_geocode",
        "to_columns": ["state_full_name"]
      }
    ]
) }}

WITH base AS (
  SELECT
    CAST(FLOOR(year) AS INT64) AS year,
    LOWER(REPLACE(state_and_area, ' ', '_')) AS state_and_area,
    unemployment,
    _data_source,
    _load_time AS unemployment_load_time
  FROM {{ ref('stg_state_level_unemployment_data') }}
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
  AND LOWER(REPLACE(state_and_area, ' ', '_')) IN (
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
    'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa',
    'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan',
    'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new_hampshire', 'new_jersey', 'new_mexico', 'new_york', 'north_carolina',
    'north_dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode_island',
    'south_carolina', 'south_dakota', 'tennessee', 'texas', 'utah', 'vermont',
    'virginia', 'washington', 'west_virginia', 'wisconsin', 'wyoming'
  )
)

SELECT
  year,
  state_and_area,
  AVG(unemployment) AS unemployment,
  ANY_VALUE(_data_source) AS _data_source,
  MAX(unemployment_load_time) AS _load_time
FROM base
GROUP BY year, state_and_area
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY year, state_and_area
  ORDER BY MAX(unemployment_load_time) DESC
) = 1
ORDER BY year, state_and_area
