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

WITH source AS (
  SELECT * FROM {{ ref('stg_crime_data') }}
),

-- Use alias to disambiguate _load_time
transformed AS (
  SELECT
    CAST(FLOOR(year) AS INT64) AS year,
    LOWER(REPLACE(state, ' ', '_')) AS state,
    population,
    crime,
    crime_against_persons,
    crime_against_property,
    crime_against_society,
    _data_source,
    _load_time AS crime_load_time
  FROM source
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
  AND LOWER(REPLACE(state, ' ', '_')) IN (
    'alabama', 'alaska', 'arizona', 'arkansas', 'california',
    'colorado', 'connecticut', 'delaware', 'florida', 'georgia',
    'hawaii', 'idaho', 'illinois', 'indiana', 'iowa',
    'kansas', 'kentucky', 'louisiana', 'maine', 'maryland',
    'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri',
    'montana', 'nebraska', 'nevada', 'new_hampshire', 'new_jersey',
    'new_mexico', 'new_york', 'north_carolina', 'north_dakota', 'ohio',
    'oklahoma', 'oregon', 'pennsylvania', 'rhode_island', 'south_carolina',
    'south_dakota', 'tennessee', 'texas', 'utah', 'vermont',
    'virginia', 'washington', 'west_virginia', 'wisconsin', 'wyoming'
  )
),

deduped AS (
  SELECT
    year,
    state,
    population,
    crime,
    crime_against_persons,
    crime_against_property,
    crime_against_society,
    _data_source,
    crime_load_time,
    ROW_NUMBER() OVER (
      PARTITION BY
        year,
        CAST(population AS STRING),
        state,
        CAST(crime AS STRING),
        CAST(crime_against_persons AS STRING),
        CAST(crime_against_property AS STRING),
        CAST(crime_against_society AS STRING)
      ORDER BY crime_load_time DESC
    ) AS row_num
  FROM transformed
)

SELECT
  year,
  state,
  population,
  crime,
  crime_against_persons,
  crime_against_property,
  crime_against_society,
  _data_source,
  crime_load_time AS _load_time
FROM deduped
WHERE row_num = 1
ORDER BY year, state
