"""
Airflow DAG: large_file_processor

Processes a multi-GB transaction log — computes daily totals and flags
anomalies over a rolling 7-day window.

BUG (inject_6): The whole file is loaded into RAM with a single pd.read_csv().
Works fine on small test data. Fails with MemoryError in production when the
log hits 4-8 GB. Fix requires chunked reading with chunksize parameter.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DATA_DIR = Path(os.getenv("TRIAGE_DATA_DIR", "/opt/airflow/sample_data"))

@dag(dag_id="large_file_processor", schedule="@daily", start_date=days_ago(1), catchup=False, tags=["etl", "analytics"])
def large_file_processor():

    @task()
    def compute_daily_totals() -> dict:
        log_file = DATA_DIR / "transactions_full.csv"
        # BUG: loads 4-8 GB file entirely into RAM -> MemoryError in production
        df = pd.read_csv(str(log_file))
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)
        totals = df.groupby("date")["amount"].sum().to_dict()
        return totals

    @task()
    def flag_anomalies(totals: dict) -> None:
        values = list(totals.values())
        mean = sum(values) / len(values) if values else 0
        for date, total in totals.items():
            if total > mean * 2:
                print(f"[large_file_processor] ANOMALY on {date}: ${total:,.2f} (mean ${mean:,.2f})")

    totals = compute_daily_totals()
    flag_anomalies(totals)

large_file_processor()
