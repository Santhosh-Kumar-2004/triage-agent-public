"""
inject_6.py — Level 6: Memory error on large file (no chunking)

Scenario : In dev the transactions log is 50 MB. In production after six months
           of data accumulation it is 6 GB. The Airflow worker has 8 GB RAM;
           pd.read_csv() tries to load the whole thing and the worker gets OOM-killed.
Problem  : Full file loaded into RAM in one shot
Fix      : Replace pd.read_csv(file) with a chunksize loop that accumulates
           daily totals incrementally, keeping peak RAM usage constant regardless
           of file size. Output shape of compute_daily_totals() must stay the same.
Difficulty: HIGH — model must rewrite the core algorithm (not just add a guard).
            The chunked aggregation pattern is non-trivial: it must
            (a) iterate chunks, (b) group by date within each chunk, (c) accumulate
            into a running dict before returning. Many models produce syntactically
            correct but logically broken versions (wrong aggregation, losing rows
            that span chunk boundaries, etc.).
"""
import json, urllib.request

MOCK_URL = "http://127.0.0.1:8088/mock/fail"

TRACEBACK = """\
Traceback (most recent call last):
  File "/opt/airflow/dags/large_file_processor.py", line 29, in compute_daily_totals
    df = pd.read_csv(str(log_file))
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/readers.py", line 912, in read_csv
    return _read(filepath_or_buffer, kwds)
  File "/usr/local/lib/python3.10/site-packages/pandas/io/parsers/c_parser_wrapper.py", line 76, in __init__
    self._reader = parsers.TextReader(src, **kwds)
  File "pandas/_libs/parsers.pyx", line 544, in pandas._libs.parsers.TextReader.__init__
    self._reader = parsers.TextReader(src, **kwds)
numpy.core._exceptions._ArrayMemoryError: Unable to allocate 5.82 GiB for an array
  with shape (6180212, 12) and data type float64
MemoryError
"""

payload = json.dumps({
    "dag_id":    "large_file_processor",
    "task_id":   "compute_daily_totals",
    "error":     "MemoryError: Unable to allocate 5.82 GiB for an array",
    "traceback": TRACEBACK,
}).encode()

req = urllib.request.Request(MOCK_URL, data=payload,
                              headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(req, timeout=5) as resp:
    result = json.loads(resp.read())

print("inject_6 — Memory error (full file into RAM, no chunking)")
print(f"  DAG  : large_file_processor  |  Task: compute_daily_totals")
print(f"  Error: MemoryError — 5.82 GiB allocation on 6M-row CSV")
print(f"  Run  : {result['run_id']}")
print()
print("Expected fix: pd.read_csv(chunksize=50_000) loop with running dict aggregation")
