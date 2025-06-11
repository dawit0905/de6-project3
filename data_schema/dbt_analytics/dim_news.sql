{{ config(materialized='table', schema='analytics') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['title']) }} AS news_id,
    {{ dbt_utils.generate_surrogate_key(['symbol', 'ref_event_date']) }} AS event_id,
    title,
    published_at,
    link
FROM {{ source('raw_data', 'news_raw') }}
WHERE ref_event_date IS NOT NULL;

