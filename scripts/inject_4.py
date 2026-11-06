"""
inject_4.py — Level 4: Database connection timeout (no retry)

Scenario : The warehouse DB is under heavy load at 9 AM UTC. The connection
           pool times out after 5 seconds. The task fails permanently even though
           it would have succeeded if retried 30 seconds later.
Problem  : No retry logic — one attempt, permanent failure on a transient blip
Fix      : Wrap the connection in a retry loop: 3 attempts with 10s/30s/60s back-off;
           raise AirflowException after all retries exhausted
Difficulty: MEDIUM-HIGH — model must generate correct retry loop with time.sleep,
            handle the specific SQLAlchemy exception hierarchy, and not break the
            existing to_sql() call inside the retry block
"""
import json, urllib.request

MOCK_URL = "http://127.0.0.1:8088/mock/fail"

TRACEBACK = """\
Traceback (most recent call last):
  File "/opt/airflow/dags/db_loader.py", line 32, in load_to_db
    with engine.connect() as conn:
  File "/usr/local/lib/python3.10/site-packages/sqlalchemy/engine/base.py", line 3166, in connect
    return self._connection_cls(self, close_with_result=close_with_result)
  File "/usr/local/lib/python3.10/site-packages/sqlalchemy/engine/base.py", line 96, in __init__
    else engine.raw_connection()
  File "/usr/local/lib/python3.10/site-packages/sqlalchemy/engine/base.py", line 3245, in raw_connection
    return self._wrap_pool_connect(self.pool.connect, _connection)
  File "/usr/local/lib/python3.10/site-packages/psycopg2/__init__.py", line 122, in connect
    conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
psycopg2.OperationalError: could not connect to server: Connection refused
	Is the server running on host "db-host" (10.0.1.42) and accepting
	TCP/IP connections on port 5432?
"""

payload = json.dumps({
    "dag_id":    "db_loader",
    "task_id":   "load_to_db",
    "error":     "psycopg2.OperationalError: could not connect to server: Connection refused",
    "traceback": TRACEBACK,
}).encode()

req = urllib.request.Request(MOCK_URL, data=payload,
                              headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(req, timeout=5) as resp:
    result = json.loads(resp.read())

print("inject_4 — DB connection timeout (no retry)")
print(f"  DAG  : db_loader  |  Task: load_to_db")
print(f"  Error: psycopg2.OperationalError — connection refused, no retry logic")
print(f"  Run  : {result['run_id']}")
print()
print("Expected fix: retry loop, 3 attempts, 10s/30s/60s backoff, raise AirflowException after exhaustion")
