"""
Airflow DAG: csv_processor

Reads every file dropped into DATA_DIR, parses it as CSV,
and writes a row-count summary.

BUG (inject_1): No file-format check — a PDF or Excel file dropped into the
directory causes pandas to raise ParserError and the whole task fails.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DATA_DIR = Path(os.getenv("TRIAGE_DATA_DIR", "/opt/airflow/sample_data"))

@dag(dag_id="csv_processor", schedule="@hourly", start_date=days_ago(1), catchup=False, tags=["etl"])
def csv_processor():

    @task()
    def list_files() -> list[str]:
        files = [str(p) for p in DATA_DIR.iterdir() if p.is_file()]
        if not files:
            raise FileNotFoundError(f"No files found in {DATA_DIR}")
        return files

    @task()
    def parse_and_count(file_paths: list[str]) -> dict[str, int]:
        results = {}
        for path in file_paths:
            df = pd.read_csv(path)          # BUG: no extension check
            results[Path(path).name] = len(df)
        return results

    @task()
    def write_summary(counts: dict[str, int]) -> None:
        for name, count in counts.items():
            print(f"[csv_processor] {name}: {count} rows")

    files = list_files()
    counts = parse_and_count(files)
    write_summary(counts)

csv_processor()
