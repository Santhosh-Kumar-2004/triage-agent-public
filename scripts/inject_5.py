"""
inject_5.py — Level 5: Missing dependency (openpyxl not installed)

Scenario : The finance team started sending .xlsx files. The DAG was updated to
           call pd.read_excel() but nobody added openpyxl to the worker image's
           requirements. It works fine in the developer's venv (openpyxl installed
           globally) but fails in production on the first monthly run.
Problem  : ModuleNotFoundError surfaces only at runtime, not at deploy time
Fix      : Catch ImportError, log which package is missing with pip install hint,
           raise AirflowException so the run fails cleanly with an actionable message
Difficulty: MEDIUM-HIGH — model must distinguish this from a code bug (the fix is
            NOT to rewrite the read_excel call). It needs to add an import guard
            and raise with a message that tells the operator exactly what to install,
            without changing the data-processing logic at all.
"""
import json, urllib.request

MOCK_URL = "http://127.0.0.1:8088/mock/fail"

TRACEBACK = """\
Traceback (most recent call last):
  File "/opt/airflow/dags/excel_parser.py", line 30, in parse_excel
    df = pd.read_excel(str(src), engine="openpyxl")
  File "/usr/local/lib/python3.10/site-packages/pandas/io/excel/_base.py", line 496, in read_excel
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/excel/_base.py", line 1591, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/excel/_openpyxl.py", line 552, in __init__
    import openpyxl
ModuleNotFoundError: No module named 'openpyxl'
"""

payload = json.dumps({
    "dag_id":    "excel_parser",
    "task_id":   "parse_excel",
    "error":     "ModuleNotFoundError: No module named 'openpyxl'",
    "traceback": TRACEBACK,
}).encode()

req = urllib.request.Request(MOCK_URL, data=payload,
                              headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(req, timeout=5) as resp:
    result = json.loads(resp.read())

print("inject_5 — Missing dependency (openpyxl not in worker image)")
print(f"  DAG  : excel_parser  |  Task: parse_excel")
print(f"  Error: ModuleNotFoundError: No module named 'openpyxl'")
print(f"  Run  : {result['run_id']}")
print()
print("Expected fix: ImportError guard with actionable 'pip install openpyxl' message")
