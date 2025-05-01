{{ config(
    materialized='incremental',
    unique_key=['state', 'year'],
    constraints=[
      {
        "type": "primary_key",
        "columns": ["state", "year"]
      },
      {
        "type": "foreign_key",
        "columns": ["state"],
        "to": "us_geocode",
        "to_columns": ["state_full_name"]
      }
    ]
) }}

WITH joined AS (
  SELECT
    c.state,
    c.year,
    c.crime_against_persons,
    c.crime_against_property,
    g.real_gdp,
    c._load_time AS crime_load_time,
    g._load_time AS gdp_load_time
  FROM {{ ref('Crime_Data') }} c
  JOIN {{ ref('Gdp') }} g
    ON c.state = g.state
   AND c.year = g.year
  {% if is_incremental() %}
    WHERE c._load_time > (SELECT MAX(_load_time) FROM {{ this }})
       OR g._load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
)

SELECT
  state,
  year,
  AVG(crime_against_persons) AS crime_against_persons,
  AVG(crime_against_property) AS crime_against_property,
  AVG(real_gdp) AS avg_gdp,
  MAX(GREATEST(crime_load_time, gdp_load_time)) AS _load_time
FROM joined
GROUP BY state, year
ORDER BY state, year
