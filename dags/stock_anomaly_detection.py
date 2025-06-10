from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import datetime
from datetime import timedelta
from stock_utils import fetch_stock_data, detect_anomaly, upload_to_s3

# 상위 시가총액 + 테마주 + 업종다양화 추천종목
TICKERS = [
    '005930.KS',  # 삼성전자
    '000660.KS',  # SK하이닉스
    '373220.KQ',  # LG에너지솔루션
    '247540.KQ',  # 에코프로비엠
    '068270.KQ',  # 셀트리온
    '035720.KQ',  # 카카오
    '034020.KQ',  # 두산에너빌리티
    '011200.KS',  # HMM
    '086790.KQ',  # 하나금융지주
    '035760.KQ',  # CJ ENM
]

START_DATE_FIXED = "2024-06-01"
END_DATE_FIXED = "2024-12-31"


@dag(
    dag_id='stock_anomaly_detection',
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=['stock', 'anomaly']
)
def stock_anomaly_detection():
    from airflow.decorators import task

    @task.branch
    def choose_branch(ds: str):
        return "run_pipeline" if ds >= "2024-05-21" else "fetch_only"

    fetch_only = EmptyOperator(task_id="fetch_only")
    run_pipeline = EmptyOperator(task_id="run_pipeline")

    # 병렬 주가 수집
    fetched_paths = fetch_stock_data.partial(
        start_date=START_DATE_FIXED,
        end_date=END_DATE_FIXED
    ).expand(ticker=TICKERS)

    # 이상치 탐지
    anomaly_paths = detect_anomaly.expand(csv_path=fetched_paths)

    # S3 업로드
    uploaded_paths = upload_to_s3.expand(file_path=anomaly_paths)

    # 분기 태스크 → fetch_only or run_pipeline
    branch = choose_branch()
    branch >> [fetch_only, run_pipeline]
    uploaded_paths >> run_pipeline


stock_anomaly_detection()
