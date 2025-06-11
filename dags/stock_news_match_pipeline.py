import io
import os
import feedparser
import pandas as pd
import boto3
from io import StringIO
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import datetime

from common import stock_keyword
from stock_utils import S3_FOLDER, S3_BUCKET


@dag(
    dag_id='stock_news_match_pipeline',
    start_date=datetime(2024, 5, 21),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['stock', 'news'],
)
def stock_news_match_pipeline():

    @task
    def load_csv_files(s3_object, bucket_name: str, prefix: str) -> list:
        """S3 anomaly/ 폴더 내 모든 CSV 파일 경로 수집"""
        paginator = s3_object.get_paginator('list_objects_v2')
        csv_files = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') and not key.endswith('/'):
                    csv_files.append(key)
        return csv_files

    @task
    def load_anomalies(s3_object, bucket_name: str, csv_key: str) -> dict:
        # 파일 내용을 바이트로 읽어서 pandas로 처리
        response = s3_object.get_object(
            Bucket=bucket_name,
            Key=csv_key
        )
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        ticker = csv_key.split('/')[-1].split('_')[0]
        anomalies = []

        if 'outlier' in df.columns:
            filtered = df[df['outlier'] == True]
            for _, row in filtered.iterrows():
                anomalies.append({
                    'ticker': ticker,
                    'stock_name': stock_keyword.get(ticker, 'N/A'),
                    'date': row['date']
                })
        return {'csv_key': csv_key, 'anomalies': anomalies}


    @task
    def fetch_news(anomaly_item: dict):
        """
        keyword와 날짜(date)를 기준으로 뉴스 수집
        - date 기준 -3일 ~ 당일 범위로 설정 가능
        """
        # anomay_item: {"ticker": ..., "stock_name": ..., "date": "0000-00-00"}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://www.google.com/'
        }
        start_date = datetime.strptime(anomaly_item['date'], "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        query = f"{anomaly_item['stock_name']}+after:{start_date:%Y-%m-%d}+before:{end_date:%Y-%m-%d}"
        url = f"https://news.google.com/rss/search?q={query}"
        feed = feedparser.parse(url, request_headers=headers)
        return [{
            'ticker': anomaly_item["ticker"],
            'stock_name': anomaly_item["stock_name"] if anomaly_item["stock_name"] != "CJ%20ENM" else "CJ ENM",
            'date': anomaly_item["date"],
            'title': entry.title,
            'link': entry.link,
            'published': entry.get('published', ''),
            'source': entry.get('source', {}).get('title', ''),
        } for entry in feed.entries]

    @task
    def upload_news(s3_object, news: list[list[dict]]) -> str:
        # s3으로 news를 csv 형태로 업로드하고 해당 s3 key를 리턴하는 함수
        flattened = [item for sublist in news for item in sublist]
        df = pd.DataFrame(flattened)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_key = f"news/{flattened['ticker']}_news.csv"
        try:
            s3_object.upload_fileobj(
                io.BytesIO(csv_buffer.getvalue().encode()),
                bucket_name,
                s3_key
            )
        except Exception as e:
            print(str(e), f"{s3_key} data fail!")
            raise
        return s3_key  # 다음 DAG에서 사용할 키

    bucket_name = S3_BUCKET
    prefix = S3_FOLDER
    s3 = boto3.client(
        's3'
    )

    csv_keys = load_csv_files(
        s3_object=s3,
        bucket_name=bucket_name, prefix=prefix
    )
    anomalies = load_anomalies.partial(
        s3_object=s3,
        bucket_name=bucket_name
    ).expand(csv_key=csv_keys)
    fetched_news = fetch_news.expand(anomaly_item=anomalies)
    news_upload_path = upload_news(s3_object=s3, news=fetched_news)

    csv_keys >> anomalies >> fetched_news >> news_upload_path

stock_news_match_pipeline()

