"""
Airflow DAG: order_etl

Reads daily orders CSV, calculates revenue totals per region, and loads
to a summary table.

BUG (inject_2): Upstream team renamed column 'order_amount' to 'total_amount'
in the January 2024 schema update. The task crashes with KeyError on the
first access.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DATA_DIR = Path(os.getenv("TRIAGE_DATA_DIR", "/opt/airflow/sample_data"))

@dag(dag_id="order_etl", schedule="@daily", start_date=days_ago(1), catchup=False, tags=["etl", "orders"])
def order_etl():

    @task()
    def load_orders() -> str:
        path = str(DATA_DIR / "orders_2024_01.csv")
        return path

    @task()
    def calculate_revenue(file_path: str) -> dict:
        df = pd.read_csv(file_path)
        # BUG: column was renamed; this KeyError kills the task
        df["revenue"] = df["order_amount"] * df["quantity"]
        summary = df.groupby("region")["revenue"].sum().to_dict()
        return summary

    @task()
    def print_summary(summary: dict) -> None:
        for region, total in summary.items():
            print(f"[order_etl] {region}: ${total:,.2f}")

    path = load_orders()
    summary = calculate_revenue(path)
    print_summary(summary)

order_etl()
