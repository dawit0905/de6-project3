import io
import json
import feedparser
import pandas as pd
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common import stock_keyword

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


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
    def load_anomalies(s3_key: str, bucket_name: str) -> list[dict]:
        """
        리턴: [{'ticker': '005930.KS', 'stock_name': '삼성', 'date': '2024-05-21'}, ...]
        """
        hook = S3Hook('aws_conn_id')
        s3_csv_obj = hook.get_key(key=s3_key, bucket_name=bucket_name)

        # 파일 내용을 바이트로 읽어서 pandas로 처리
        file_content = s3_csv_obj.get()['Body'].read()
        df = pd.read_csv(io.BytesIO(file_content))
        df = df[['ticker', 'date']]
        df['stock_name'] = df['ticker'].map(stock_keyword)

        # 컬럼 순서 재정렬
        df = df[['ticker', 'stock_name', 'date']]

        # 딕셔너리 리스트로 변환
        result = df.to_dict(orient='records')

        return result


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
            'title': entry.title,
            'link': entry.link,
            'published': entry.get('published', ''),
            'source': entry.get('source', {}).get('title', ''),
            'ticker': anomaly_item["ticker"],
            'anomaly_date': anomaly_item["date"]
        } for entry in feed.entries]

    @task
    def collect_news(news: list[list[dict]]) -> str:
        # s3으로 news를 json 형태로 업로드하고 해당 s3 key를 리턴하는 함수
        flattened = [item for sublist in news for item in sublist]
        hook = S3Hook('aws_conn_id')
        s3_key = f"news/{flattened[0]['ticker']}_{flattened[0]['date']}.json"
        hook.load_string(
            string_data=json.dumps(flattened),
            key=s3_key,
            bucket_name='bucket_name',
            replace=True
        )
        return s3_key  # 다음 DAG에서 사용할 키


    anomalies = load_anomalies('s3_key', 'bucket_name')
    fetched_news = fetch_news.expand(anomaly_item=anomalies)
    news_s3_key = collect_news(fetched_news)

    # news 파일이 올라간 s3 key를 다음 DAG로 전달
    s3_key_trigger = TriggerDagRunOperator(
        task_id="",
        trigger_dag_id="",
        conf={"s3_key": news_s3_key}
    )

    load_anomalies >> fetched_news >> news_s3_key >> s3_key_trigger

stock_news_match_pipeline()

