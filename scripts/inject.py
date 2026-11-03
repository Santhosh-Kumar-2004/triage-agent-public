"""Inject a fake DAG failure into the mock Airflow server.

Usage:
    python scripts/inject.py
    python scripts/inject.py csv_processor parse_and_count "pandas.errors.ParserError: Error tokenizing data"
    python scripts/inject.py my_dag my_task "KeyError: 'customer_id'"
"""

import sys
import json
import urllib.request

MOCK_URL = "http://127.0.0.1:8088/mock/fail"

dag_id  = sys.argv[1] if len(sys.argv) > 1 else "csv_processor"
task_id = sys.argv[2] if len(sys.argv) > 2 else "parse_and_count"
error   = sys.argv[3] if len(sys.argv) > 3 else "pandas.errors.ParserError: Error tokenizing data"

body = json.dumps({"dag_id": dag_id, "task_id": task_id, "error": error}).encode()
req = urllib.request.Request(MOCK_URL, data=body,
                              headers={"Content-Type": "application/json"},
                              method="POST")
with urllib.request.urlopen(req, timeout=5) as resp:
    result = json.loads(resp.read())

print(f"Failure injected!")
print(f"  DAG    : {dag_id}")
print(f"  Task   : {task_id}")
print(f"  Error  : {error}")
print(f"  Run ID : {result['run_id']}")
print()
print("The agent will pick this up on its next poll (within 30s).")
