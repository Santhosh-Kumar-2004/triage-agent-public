"""
Airflow DAG: db_loader

Reads a processed CSV and bulk-loads rows into a PostgreSQL table.

BUG (inject_4): The database connection is opened once with no retry. Under
load the connection pool times out and the task fails permanently. Should use
exponential back-off so a transient connection blip does not kill the DAG run.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DATA_DIR = Path(os.getenv("TRIAGE_DATA_DIR", "/opt/airflow/sample_data"))
DB_CONN_STRING = os.getenv("DB_CONN_STRING", "postgresql://user:pass@db-host:5432/warehouse")

@dag(dag_id="db_loader", schedule="@hourly", start_date=days_ago(1), catchup=False, tags=["etl", "db"])
def db_loader():

    @task()
    def load_to_db(file_path: str = str(DATA_DIR / "orders_2024_01.csv")) -> int:
        from sqlalchemy import create_engine, text
        df = pd.read_csv(file_path)

        # BUG: single connection attempt, no retry on transient failures
        engine = create_engine(DB_CONN_STRING, connect_args={"connect_timeout": 5})
        with engine.connect() as conn:
            df.to_sql("orders_staging", conn, if_exists="append", index=False)
            conn.execute(text("COMMIT"))
        return len(df)

    @task()
    def log_result(row_count: int) -> None:
        print(f"[db_loader] inserted {row_count} rows into orders_staging")

    count = load_to_db()
    log_result(count)

db_loader()
