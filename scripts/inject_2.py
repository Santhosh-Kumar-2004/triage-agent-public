"""
inject_2.py — Level 2: Schema change (column renamed upstream)

Scenario : The upstream orders system renamed 'order_amount' to 'total_amount'
           in a January 2024 schema migration. Nobody updated the DAG.
Problem  : df["order_amount"] raises KeyError — the column is gone
Fix      : Validate expected columns exist before accessing; raise AirflowException
           with a diff of expected vs actual columns so on-call knows immediately
Difficulty: MEDIUM — model must understand the full column-access chain and add
            schema validation before the first access, not just catch the KeyError
"""
import json, urllib.request

MOCK_URL = "http://127.0.0.1:8088/mock/fail"

TRACEBACK = """\
Traceback (most recent call last):
  File "/opt/airflow/dags/order_etl.py", line 32, in calculate_revenue
    df["revenue"] = df["order_amount"] * df["quantity"]
  File "/usr/local/lib/python3.10/site-packages/pandas/core/frame.py", line 3505, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/usr/local/lib/python3.10/site-packages/pandas/core/indexes/base.py", line 3623, in get_loc
    raise KeyError(key) from err
KeyError: 'order_amount'
"""

payload = json.dumps({
    "dag_id":    "order_etl",
    "task_id":   "calculate_revenue",
    "error":     "KeyError: 'order_amount'",
    "traceback": TRACEBACK,
}).encode()

req = urllib.request.Request(MOCK_URL, data=payload,
                              headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(req, timeout=5) as resp:
    result = json.loads(resp.read())

print("inject_2 — Schema change (column renamed)")
print(f"  DAG  : order_etl  |  Task: calculate_revenue")
print(f"  Error: KeyError: 'order_amount' (column renamed to 'total_amount')")
print(f"  Run  : {result['run_id']}")
print()
print("Expected fix: validate required columns before access, raise AirflowException with column diff")
