from airflow.decorators import dag, task
from airflow.utils.dates import datetime

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
    def load_anomalies(ds: str) -> list:
        """
        TODO: DAG1에서 저장된 이상치 발생 정보 csv 파일을 읽어 anomaly 리스트 반환
        리턴 예: [{'ticker': '005930.KS', 'date': '2024-05-21'}, ...]
        """
        return []

    @task
    def fetch_news(anomaly: dict):
        """
        TODO: ticker와 날짜(date)를 기준으로 뉴스 수집
        - date 기준 -3일 ~ 당일 범위로 설정 가능
        """
        pass

    anomalies = load_anomalies()
    fetch_news.expand(anomaly=anomalies)

stock_news_match_pipeline()

