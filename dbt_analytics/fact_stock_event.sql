{{ config(materialized='table', schema='analytics') }}

SELECT
{{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} AS event_id,
symbol,
date,
close,
z_score,
is_outlier,
CASE
WHEN z_score >= 2.5 THEN 'up'
WHEN z_score <= -2.5 THEN 'down'
ELSE NULL
END AS direction
FROM {{ source('raw_data', 'stock_outlier_raw') }}
WHERE is_outlier = TRUE