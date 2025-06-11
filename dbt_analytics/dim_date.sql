{{ config(materialized='table', schema='analytics') }}

WITH dates AS (
    SELECT DATEADD(DAY, seq4(), '2024-01-01') AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 366))  -- 2024년은 윤년
),

base AS (
    SELECT
        date,
        YEAR(date) AS year,
        MONTH(date) AS month,
        DAY(date) AS day,
        TO_CHAR(date, 'DY') AS weekday,
        DAYOFWEEK(date) AS day_of_week,
        CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN date IN (
            '2024-01-01', '2024-02-09', '2024-02-12', '2024-03-01',
            '2024-04-10', '2024-05-01',
            '2024-05-05', '2024-05-15', '2024-06-06', '2024-08-15',
            '2024-09-16', '2024-09-17', '2024-09-18', '2024-10-03',
            '2024-10-09', '2024-12-25', '2024-12-31',
        ) THEN TRUE ELSE FALSE END AS is_holiday
    FROM dates
)

SELECT * FROM base;