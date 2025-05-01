{{ config(
    materialized='incremental',
    unique_key=['year', 'state', 'month', 'period', 'indicator']
) }}

WITH source AS (
  SELECT * 
  FROM {{ ref('Mortality_And_Natality') }}
),

transformed AS (
  SELECT
    CAST(year AS INT64) AS year,
    LOWER(REPLACE(state, ' ', '_')) AS state,
    month,
    period,
    indicator,
    data_value,
    _data_source,
    _load_time
  FROM source
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
)

SELECT
  year,
  state,
  month,
  period,
  indicator,
  data_value,
  _data_source,
  _load_time
FROM transformed
ORDER BY year, state, month, period, indicator
