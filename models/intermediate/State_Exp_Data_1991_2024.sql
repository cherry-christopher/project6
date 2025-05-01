{{ config(
    materialized='incremental',
    unique_key=['year', 'state'],
    constraints=[
      {"type": "primary_key", "columns": ["year", "state"]}
    ]
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_state_exp_data_1991_2024') }}
),

transformed AS (
  SELECT
    CAST(FLOOR(year) AS INT64) AS year,
    LOWER(REPLACE(state, ' ', '_')) AS state,
    total_cap,
    _data_source,
    _load_time AS _load_time
  FROM source
  {% if is_incremental() %}
    WHERE _load_time > (SELECT MAX(_load_time) FROM {{ this }})
  {% endif %}
)

SELECT
  year,
  state,
  total_cap,
  _data_source,
  _load_time AS _load_time
FROM transformed
