{{ config(
    materialized='incremental',
    unique_key=['state_full_name'],
    constraints=[
      {
        "type": "primary_key",
        "columns": ["state_full_name"]
      }
    ]
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_us_geocode') }}
),

transformed AS (
  SELECT
    LOWER(REPLACE(state_abbreviation, ' ', '_')) AS state_abbreviation,
    LOWER(REPLACE(state_full_name, ' ', '_')) AS state_full_name,
    latitude,
    longitude,
    _data_source,
    _load_time AS _load_time
  FROM source
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
)

SELECT
  state_abbreviation,
  state_full_name,
  latitude,
  longitude,
  _data_source,
  _load_time AS _load_time
FROM transformed
ORDER BY state_full_name
