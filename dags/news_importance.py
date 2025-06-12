from airflow.decorators import dag, task
from airflow.utils.dates import datetime
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
import boto3
from io import StringIO
import os

# 환경변수에서 S3 설정 읽기
S3_BUCKET_NAME = os.getenv("S3_BUCKET")           
NEWS_PREFIX = os.getenv("NEWS_PREFIX", "news")
ANOMALY_PREFIX = os.getenv("ANOMALY_PREFIX", "stock-anomaly")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "news-importance")

TICKERS = [
    "000660.KS", "005930.KS", "011200.KS", "034020.KQ", "035720.KQ",
    "035760.KQ", "068270.KQ", "086790.KQ", "247540.KQ", "373220.KQ"
]

# S3 클라이언트 생성 함수
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-northeast-2")
    )

# 중요도 계산 함수 (TF-IDF + Z-score)
def compute_news_importance(news_df: pd.DataFrame, stock_df: pd.DataFrame, top_k_keywords: int = 5):

    corpus = news_df['title'].fillna("")
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(corpus)

    news_df['tfidf_score'] = X.sum(axis=1).A1
    feature_names = vectorizer.get_feature_names_out()

    def extract_keywords(row_idx):
        row = X[row_idx].toarray().flatten()
        top_indices = row.argsort()[::-1][:top_k_keywords]
        return [feature_names[i] for i in top_indices if row[i] > 0]

    news_df['top_keywords'] = [extract_keywords(i) for i in range(X.shape[0])]
    news_df['top_keywords'] = news_df['top_keywords'].apply(lambda x: ', '.join(x))

    merged = pd.merge(news_df, stock_df[['ticker', 'date', 'z_score']], on=['ticker', 'date'], how='left')
    merged['z_score_abs'] = merged['z_score'].abs().fillna(0)
    merged['importance_score'] = merged['tfidf_score'] * merged['z_score_abs']

    return merged.sort_values(by='importance_score', ascending=False)

# DAG 정의
@dag(
    dag_id='news_importance_pipeline',
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['stock', 'importance'],
)
def news_importance_pipeline():

    @task
    def load_news_with_anomalies(ds: str):
        s3 = get_s3_client()
        all_news_records = []

        for ticker in TICKERS:
            s3_key = f"{NEWS_PREFIX}/{ticker}_news.csv"
            try:
                response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                df = pd.read_csv(response['Body'])
                all_news_records.extend(df.to_dict(orient="records"))
            except Exception as e:
                print(f"⚠️ Error loading {s3_key}: {e}")
                continue

        return all_news_records

    @task
    def load_anomaly_scores(ds: str):
        s3 = get_s3_client()
        all_records = []

        for ticker in TICKERS:
            s3_key = f"{ANOMALY_PREFIX}/{ticker}_anomaly.csv"
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            df = pd.read_csv(response['Body'])
            df['ticker'] = ticker
            all_records.extend(df.to_dict(orient="records"))

        return all_records

    @task
    def compute(news_data: list, anomaly_data: list, ds: str = None):
        df_news = pd.DataFrame(news_data)
        df_all = pd.DataFrame(anomaly_data)

        # 날짜 정리
        df_news['date'] = pd.to_datetime(df_news['date']).dt.date
        df_all = df_all.rename(columns={'Date': 'date'})
        df_all['date'] = pd.to_datetime(df_all['date']).dt.date

        result_df = compute_news_importance(df_news, df_all)

        save_columns = ['ticker', 'date', 'title', 'top_keywords', 'tfidf_score', 'z_score', 'importance_score']
        result = result_df[save_columns].to_dict(orient="records")
        return result

    @task
    def save_to_s3(results: list, ds: str = None):
        df = pd.DataFrame(results)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3 = get_s3_client()
        s3_key = f"{OUTPUT_PREFIX}/output.csv"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
        print(f" Saved to s3://{S3_BUCKET_NAME}/{s3_key}")

    news = load_news_with_anomalies()
    anomalies = load_anomaly_scores()
    result = compute(news_data=news, anomaly_data=anomalies)
    save_to_s3(results=result)

news_importance_pipeline()

