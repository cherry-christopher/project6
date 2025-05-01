{{ config(
    materialized='incremental',
    unique_key=['state_id', 'cities'],
    constraints=[
      {
        "type": "primary_key",
        "columns": ["state_id", "cities"]
      }
    ]
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_states_cities') }}
),

transformed AS (
  SELECT
    state_id,
    LOWER(REPLACE(cities, ' ', '_')) AS cities,
    LOWER(REPLACE(state_name, ' ', '_')) AS state_name,
    latitude,
    longitude,
    total_population,
    avg_density,
    timezones,
    _data_source,
    _load_time AS _load_time
  FROM source
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
)

SELECT
  state_id,
  cities,
  state_name,
  latitude,
  longitude,
  total_population,
  avg_density,
  timezones,
  _data_source,
  _load_time AS _load_time
FROM transformed
ORDER BY state_id, cities
