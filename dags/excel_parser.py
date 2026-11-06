"""
Airflow DAG: excel_parser

Finance team drops monthly Excel reports; this DAG parses them and
produces a normalised CSV for the data warehouse.

BUG (inject_5): openpyxl is not installed in the Airflow worker image.
pandas.read_excel() raises ModuleNotFoundError at runtime. Easy to miss
because it only surfaces when the .xlsx file actually arrives.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DATA_DIR = Path(os.getenv("TRIAGE_DATA_DIR", "/opt/airflow/sample_data"))
OUT_DIR  = Path(os.getenv("TRIAGE_OUT_DIR", "/opt/airflow/output"))

@dag(dag_id="excel_parser", schedule="@monthly", start_date=days_ago(1), catchup=False, tags=["finance"])
def excel_parser():

    @task()
    def parse_excel() -> str:
        src = DATA_DIR / "finance_report.xlsx"
        # BUG: openpyxl not installed -> ModuleNotFoundError at runtime
        df = pd.read_excel(str(src), engine="openpyxl")
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        out = str(OUT_DIR / "finance_normalised.csv")
        df.to_csv(out, index=False)
        return out

    @task()
    def report(out_path: str) -> None:
        df = pd.read_csv(out_path)
        print(f"[excel_parser] {len(df)} rows written to {out_path}")

    out = parse_excel()
    report(out)

excel_parser()
