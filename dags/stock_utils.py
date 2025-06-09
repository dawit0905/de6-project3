# stock_utils.py
from airflow.decorators import task
import yfinance as yf
import pandas as pd
import os

DATA_DIR = "/opt/airflow/dags/files"

@task
def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> str:
    df = yf.download(ticker, start=start_date, end=end_date, group_by='column')

    if df.empty:
        print(f"[{ticker}] 데이터가 비어 있습니다.")
        return ""

    df = df.reset_index()

    
    try:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
    except Exception as e:
        print(f"[{ticker}] 컬럼 처리 중 오류 발생: {e}")

    os.makedirs(DATA_DIR, exist_ok=True)
    path = f"{DATA_DIR}/{ticker}.csv"
    df.to_csv(path, index=False)
    print(f"[{ticker}] CSV 저장 완료 → {path}")
    return path

@task
def detect_anomaly(csv_path: str) -> str:

    if not csv_path:
        print(f"[입력 누락] csv_path가 비어있습니다 → 생략")
        return ""
    
    if not os.path.exists(csv_path):
        print(f"[{csv_path}] CSV 없음 → 생략")
        return ""

    try:
        df = pd.read_csv(csv_path, parse_dates=['Date'], index_col='Date')
    except Exception as e:
        print(f"[{csv_path}] CSV 로딩 실패 → {e}")
        return ""
    
    if df.empty:
        print(f"[{csv_path}] 데이터 없음 → 생략")
        return ""

    try:
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['STD20'] = df['Close'].rolling(window=20).std()
        df['z_score'] = (df['Close'] - df['MA20']) / df['STD20']
        df['is_outlier'] = df['z_score'].abs() > 2.5
    except Exception as e:
        print(f"[{csv_path}] 이상치 분석 실패 → {e}")
        return ""

    output_path = csv_path.replace(".csv", "_anomaly.csv")
    df.to_csv(output_path)
    print(f"[{csv_path}] 이상 탐지 완료 → {output_path}")
    return output_path