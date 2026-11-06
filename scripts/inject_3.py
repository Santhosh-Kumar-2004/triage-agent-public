"""
inject_3.py — Level 3: File not delivered (late upstream)

Scenario : The orders_2024_03.csv file was not delivered by the upstream team
           on time. The DAG tries to open it and crashes hard instead of waiting
           for the next scheduled run.
Problem  : FileNotFoundError kills the run as Failed; should be Skipped
Fix      : Check if file exists; raise AirflowSkipException with a clear message
           so the run shows as Skipped (not Failed) and the alert is not fired
Difficulty: MEDIUM — model must know AirflowSkipException exists and use it
            correctly (different from AirflowException). It is an Airflow API
            question, not just a Python question.
"""
import json, urllib.request

MOCK_URL = "http://127.0.0.1:8088/mock/fail"

TRACEBACK = """\
Traceback (most recent call last):
  File "/opt/airflow/dags/file_ingestion.py", line 29, in validate_file
    df = pd.read_csv(str(expected))
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 912, in read_csv
    return _read(filepath_or_buffer, kwds)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 557, in _read
    parser = TextFileReader(filepath_or_buffer, **kwds)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 1407, in __init__
    self._engine = self._make_engine(f, self.engine)
  File "/opt/airflow/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 1661, in _make_engine
    self.handles = get_handle(f, mode, encoding=self.options.get("encoding", None), ...)
FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/sample_data/orders_2024_03.csv'
"""

payload = json.dumps({
    "dag_id":    "file_ingestion",
    "task_id":   "validate_file",
    "error":     "FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/sample_data/orders_2024_03.csv'",
    "traceback": TRACEBACK,
}).encode()

req = urllib.request.Request(MOCK_URL, data=payload,
                              headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(req, timeout=5) as resp:
    result = json.loads(resp.read())

print("inject_3 — File not delivered (late upstream)")
print(f"  DAG  : file_ingestion  |  Task: validate_file")
print(f"  Error: FileNotFoundError — expected daily drop file absent")
print(f"  Run  : {result['run_id']}")
print()
print("Expected fix: guard with Path.exists(), raise AirflowSkipException (not AirflowException)")
