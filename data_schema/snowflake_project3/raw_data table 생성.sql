-- 먼저 해당 데이터베이스와 스키마를 선택하세요
USE DATABASE project3_dev;
USE SCHEMA raw_data;

-- stock_prices_raw
CREATE OR REPLACE TABLE raw_data.stock_outlier_raw (
    date DATE,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    open FLOAT,
    volume BIGINT,
    ma20 FLOAT,
    std20 FLOAT,
    z_score FLOAT,
    is_outlier BOOLEAN,
    symbol STRING
);


-- news_raw
CREATE OR REPLACE TABLE raw_data.news_raw (
    symbol STRING,
    stock_name STRING,
    ref_event_date DATE,           -- 이상치 발생일
    title STRING,
    link STRING,
    published_at TIMESTAMP,
    source STRING
);


-- news_keywords_raw
CREATE OR REPLACE TABLE raw_data.news_keywords_raw (
    symbol STRING,
    ref_event_date DATE,
    title string,
    keyword STRING,
    tfidf_score FLOAT,
    z_score FLOAT,
    importance_score FLOAT
);

