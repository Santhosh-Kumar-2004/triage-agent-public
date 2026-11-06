"""
inject_1.py — Level 1: Wrong file format

Scenario : An upstream team dropped a PDF into the CSV ingestion folder.
           pandas.read_csv() chokes on it immediately.
Problem  : No format guard before read_csv()
Fix      : Skip non-.csv files, log a warning, continue with the rest
Difficulty: LOW — single guard clause, one location in the code
"""
import json, urllib.request, sys

MOCK_URL = "http://127.0.0.1:8088/mock/fail"

TRACEBACK = """\
Traceback (most recent call last):
  File "/opt/airflow/dags/csv_processor.py", line 33, in parse_and_count
    df = pd.read_csv(path)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 912, in read_csv
    return _read(filepath_or_buffer, kwds)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 577, in _read
    parser = TextFileReader(filepath_or_buffer, **kwds)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 1407, in __init__
    self._engine = self._make_engine(f, self.engine)
pandas.errors.ParserError: Error tokenizing data. C error: Expected 1 fields in line 5, saw 3
"""

payload = json.dumps({
    "dag_id":    "csv_processor",
    "task_id":   "parse_and_count",
    "error":     "pandas.errors.ParserError: Error tokenizing data. C error: Expected 1 fields in line 5, saw 3",
    "traceback": TRACEBACK,
}).encode()

req = urllib.request.Request(MOCK_URL, data=payload,
                              headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(req, timeout=5) as resp:
    result = json.loads(resp.read())

print("inject_1 — Wrong file format")
print(f"  DAG  : csv_processor  |  Task: parse_and_count")
print(f"  Error: ParserError — non-CSV file in data directory")
print(f"  Run  : {result['run_id']}")
print()
print("Expected fix: add extension check, skip non-.csv files with a warning")
