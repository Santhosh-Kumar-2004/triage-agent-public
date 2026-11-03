"""Smoke test — runs without needing an Anthropic API key or live email.

Tests:
  1. All imports work
  2. Hamilton diagnosis pipeline runs end-to-end on a synthetic email
  3. All 11 error classifications are triggered correctly
  4. Burr state machine builds and steps through to completion

Run:
    .venv/Scripts/python smoke_test.py
"""
from __future__ import annotations

import sys
import traceback

PASS = "  PASS"
FAIL = "  FAIL"


def check(label: str, fn):
    try:
        result = fn()
        print(f"{PASS}  {label}" + (f" → {result}" if result is not None else ""))
        return True
    except Exception as exc:
        print(f"{FAIL}  {label}")
        traceback.print_exc()
        return False


# ── 1. imports ────────────────────────────────────────────────────────────────
print("\n── 1. Imports ──")
ok = True
ok &= check("agent.diagnosis", lambda: __import__("agent.diagnosis"))
ok &= check("agent.fixer",     lambda: __import__("agent.fixer"))
ok &= check("agent.notifier",  lambda: __import__("agent.notifier"))
ok &= check("agent.config",    lambda: __import__("agent.config"))
ok &= check("agent.machine",   lambda: __import__("agent.machine"))
ok &= check("hamilton.driver", lambda: __import__("hamilton.driver"))
ok &= check("burr.core",       lambda: __import__("burr.core"))


# ── 2. Hamilton diagnosis pipeline ───────────────────────────────────────────
print("\n── 2. Hamilton diagnosis DAG ──")

SAMPLE_EMAIL = """\
DAG: csv_processor
Task: parse_and_count
execution_date: 2024-01-15T10:00:00

Traceback (most recent call last):
  File "/opt/airflow/dags/csv_processor.py", line 18, in parse_and_count
    df = pd.read_csv(path)
  File "pandas/io/parsers/readers.py", line 912, in read_csv
    return _read(filepath_or_buffer, kwds)
pandas.errors.ParserError: Error tokenizing data. C error: EOF inside string
"""

def run_hamilton():
    from hamilton.driver import Driver
    from agent import diagnosis
    dr = Driver({}, diagnosis)
    result = dr.execute(
        ["fix_context"],
        inputs={
            "email_body":   SAMPLE_EMAIL,
            "github_token": "dummy",
            "github_repo":  "dummy/repo",
        },
    )
    ctx = result["fix_context"]
    assert ctx["dag_id"] == "csv_processor", f"dag_id={ctx['dag_id']}"
    assert ctx["error_type"] == "format_mismatch", f"error_type={ctx['error_type']}"
    assert "parse_and_count" in ctx["dag_code"], "dag_code missing task function"
    return f"dag_id={ctx['dag_id']}  error_type={ctx['error_type']}"

ok &= check("full pipeline — csv_processor format_mismatch", run_hamilton)


# ── 3. Error classification ───────────────────────────────────────────────────
print("\n── 3. Error classification (all 11 types) ──")

from agent.diagnosis import error_type as classify

CASES = [
    ("format_mismatch",   "pandas.errors.ParserError: Error tokenizing data"),
    ("format_mismatch",   "UnicodeDecodeError: 'utf-8' codec can't decode"),
    ("missing_file",      "FileNotFoundError: [Errno 2] No such file or directory"),
    ("missing_file",      "No files found in /data/incoming"),
    ("permission_error",  "PermissionError: [Errno 13] Permission denied: '/data/secret.csv'"),
    ("schema_change",     "KeyError: 'customer_id'"),
    ("schema_change",     "Column not found in DataFrame: expected_col"),
    ("connection_error",  "ConnectionRefusedError: [Errno 111] Connection refused"),
    ("connection_error",  "OperationalError: could not connect to server: Connection timed out"),
    ("import_error",      "ModuleNotFoundError: No module named 'great_expectations'"),
    ("import_error",      "ImportError: cannot import name 'DataFrame' from 'pandas'"),
    ("memory_error",      "MemoryError"),
    ("memory_error",      "Killed (OOM kill)"),
    ("type_error",        "TypeError: unsupported operand type(s) for +: 'int' and 'str'"),
    ("value_error",       "ValueError: could not convert string to float: 'N/A'"),
    ("dependency_failed", "upstream task failed, not running"),
    ("unknown",           "RecursionError: maximum recursion depth exceeded"),
]

for expected, snippet in CASES:
    got = classify(snippet)
    def _check(exp=expected, snip=snippet):
        got = classify(snip)
        assert got == exp, f"got '{got}', expected '{exp}' for: {snip[:60]}"
        return got
    ok &= check(f"{expected:<20} ← {snippet[:55]}", _check)


# ── 4. Burr state machine ─────────────────────────────────────────────────────
print("\n── 4. Burr state machine ──")

def run_burr():
    import uuid
    from agent.machine import build_machine, PHASE
    m = build_machine(f"smoke-{uuid.uuid4().hex[:6]}", enable_tracker=False)
    # detect
    m.step(inputs={"email_id": "msg-1", "dag_id": "csv_processor",
                   "task_id": "parse_and_count", "error_msg": "ParserError"})
    # diagnose
    m.step(inputs={"error_type": "format_mismatch", "dag_code": "# dummy"})
    # fix
    m.step(inputs={"patch": "--- a\n+++ b\n@@ -1 +1 @@\n-old\n+new"})
    # pr
    m.step(inputs={"pr_url": "patches/csv_processor-format_mismatch.diff"})
    # complete
    m.step()
    state = m.state
    return f"phase={state[PHASE]}"

ok &= check("4-step triage run → complete", run_burr)


# ── 5. Gemini connectivity (optional — skipped if GEMINI_API_KEY not set) ─────
print("\n── 5. Gemini API connectivity ──")

import os
_gemini_key = os.environ.get("GEMINI_API_KEY", "")

if not _gemini_key or _gemini_key.startswith("sk-ant"):
    print("  SKIP  GEMINI_API_KEY not set — load .env and re-run to test Gemini")
else:
    def test_gemini():
        from google import genai
        client = genai.Client(api_key=_gemini_key)
        resp = client.models.generate_content(
            model="gemini-3.8-flash",
            contents="Reply with exactly: OK",
        )
        assert resp.text.strip(), "empty response"
        return resp.text.strip()[:40]
    ok &= check("gemini-3.8-flash responds", test_gemini)


# ── summary ───────────────────────────────────────────────────────────────────
print()
if ok:
    print("All checks passed.")
    sys.exit(0)
else:
    print("Some checks FAILED — see above.")
    sys.exit(1)
