{{ config(materialized='table', schema='analytics') }}

WITH ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY symbol, ref_event_date ORDER BY tfidf_score DESC) AS rank
    FROM {{ source('raw_data', 'news_keywords_raw') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['symbol', 'ref_event_date', 'keyword']) }} AS keyword_id,
    {{ dbt_utils.generate_surrogate_key(['symbol', 'ref_event_date']) }} AS event_id,
    keyword,
    tfidf_score,
    rank
FROM ranked;