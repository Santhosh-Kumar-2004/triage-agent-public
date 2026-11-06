"""
Airflow DAG: file_ingestion

Picks up a specifically named daily drop file, validates it, and archives it.

BUG (inject_3): The expected file path is hardcoded. When the upstream delivery
is late or the date partition is wrong, FileNotFoundError kills the task instead
of marking the run Skipped so the next scheduled run can retry cleanly.
"""
from __future__ import annotations
import os
import shutil
from pathlib import Path
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DATA_DIR = Path(os.getenv("TRIAGE_DATA_DIR", "/opt/airflow/sample_data"))
ARCHIVE  = Path(os.getenv("TRIAGE_ARCHIVE_DIR", "/opt/airflow/archive"))

@dag(dag_id="file_ingestion", schedule="@daily", start_date=days_ago(1), catchup=False, tags=["ingestion"])
def file_ingestion():

    @task()
    def validate_file() -> str:
        # BUG: no existence check -> FileNotFoundError when file is late
        expected = DATA_DIR / "orders_2024_03.csv"
        df = pd.read_csv(str(expected))
        assert len(df) > 0, "Empty file"
        return str(expected)

    @task()
    def archive_file(file_path: str) -> None:
        ARCHIVE.mkdir(parents=True, exist_ok=True)
        shutil.move(file_path, str(ARCHIVE / Path(file_path).name))
        print(f"[file_ingestion] archived {file_path}")

    path = validate_file()
    archive_file(path)

file_ingestion()
