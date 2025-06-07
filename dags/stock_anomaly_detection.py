from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import datetime
from datetime import timedelta

# 상위 시가총액 + 테마주 + 업종다양화 추천종목
TICKERS = [
        '005930.KS', # 삼성전자 
        '000660.KS', # SK하이닉스 
        '373220.KQ', # LG에너지솔루션 
        '247540.KQ', # 에코프로비엠 
        '068270.KQ', # 셀트리온 
        '035720.KQ', # 카카오 
        '034020.KQ', # 두산에너빌리티 
        '011200.KS', # HMM 
        '086790.KQ', # 하나금융지주 
        '035760.KQ', # CJ ENM
]
   
@dag(
    dag_id='stock_anomaly_detection',
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 1),
    catchup=True,
    max_active_runs=1,
    tags=['stock', 'anomaly']
)

def stock_anomaly_detection():

    @task
    def fetch_stock(ticker: str, ds: str):
        # yfinance 등으로 주가 수집
        return 

    @task
    def detect_anomaly(stock_data: dict):
        # 이상치 판단 로직
        return 

    @task.branch
    def choose_branch(ds: str):
        return "run_pipeline" if ds >= "2024-05-21" else "fetch_only"

    fetch_only = EmptyOperator(task_id="fetch_only")
    run_pipeline = EmptyOperator(task_id="run_pipeline")

    fetched = fetch_stock.expand(ticker=TICKERS)
    anomalies = detect_anomaly.expand(stock_data=fetched)

    branch = choose_branch()
    branch >> [fetch_only, run_pipeline]
    fetched >> anomalies >> run_pipeline

stock_anomaly_detection()
