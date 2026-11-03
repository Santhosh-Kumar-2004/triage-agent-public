"""Minimal Airflow REST API mock.

Trigger a failure: open dag_status.txt and change 'pass' to 'fail', save.
The agent will detect it within 30s, fix it, and write 'pass' back automatically.

Start:
    .venv/Scripts/python -m airflow_mock.server
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
    import uvicorn
except ImportError:
    raise SystemExit("fastapi and uvicorn are required: pip install fastapi uvicorn")

app = FastAPI(title="Airflow Mock")

# ── in-memory state (kept for the /mock/fail inject endpoint) ─────────────────
_runs: dict[str, dict] = {}

# ── dag_status.txt ────────────────────────────────────────────────────────────
# Lives in the project root (one level above airflow_mock/).
# Write 'fail' and save → agent detects it, fixes it, writes 'pass' back.
_STATUS_FILE = Path(__file__).parent.parent / "dag_status.txt"


def _status_file_run() -> dict | None:
    """Return a synthetic failed run when dag_status.txt contains 'fail'."""
    try:
        content = _STATUS_FILE.read_text().strip().lower()
    except OSError:
        return None
    if content != "fail":
        return None
    # File mtime → unique run_id per save, so each new 'fail' save is a fresh failure.
    mtime = int(_STATUS_FILE.stat().st_mtime)
    return {
        "dag_run_id": f"status_run_{mtime}",
        "dag_id":     "csv_processor",
        "state":      "failed",
        "start_date": datetime.now(timezone.utc).isoformat(),
        "end_date":   datetime.now(timezone.utc).isoformat(),
        "task_id":    "parse_and_count",
        "error":      "pandas.errors.ParserError: Error tokenizing data",
        "traceback":  _default_traceback(
            "csv_processor", "parse_and_count",
            "pandas.errors.ParserError: Error tokenizing data",
        ),
    }


class FailureRequest(BaseModel):
    dag_id: str
    task_id: str = "unknown_task"
    error: str = "Simulated Airflow failure"
    traceback: Optional[str] = None


# ── Airflow-compatible endpoints ──────────────────────────────────────────────

@app.get("/api/v1/dags/{dag_id}/dagRuns")
def list_dag_runs(dag_id: str, state: Optional[str] = None):
    runs = [r for r in _runs.values() if r["dag_id"] == dag_id]
    if state:
        runs = [r for r in runs if r["state"] == state]
    return {"dag_runs": runs, "total_entries": len(runs)}


@app.get("/api/v1/dagRuns")
def list_all_dag_runs(state: Optional[str] = None):
    """Returns all runs — from in-memory store AND dag_status.txt."""
    runs = list(_runs.values())
    file_run = _status_file_run()
    if file_run and file_run["dag_run_id"] not in _runs:
        runs.append(file_run)
    if state:
        runs = [r for r in runs if r["state"] == state]
    return {"dag_runs": runs, "total_entries": len(runs)}


@app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1")
def get_task_log(dag_id: str, run_id: str, task_id: str):
    run = _runs.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"content": run.get("traceback") or run.get("error", "No log available")}


# ── mock control endpoints ────────────────────────────────────────────────────

@app.post("/mock/fail", status_code=201)
def inject_failure(req: FailureRequest):
    run_id = f"manual_run_{req.dag_id}_{uuid.uuid4().hex[:8]}"
    _runs[run_id] = {
        "dag_run_id": run_id,
        "dag_id": req.dag_id,
        "state": "failed",
        "start_date": datetime.now(timezone.utc).isoformat(),
        "end_date":   datetime.now(timezone.utc).isoformat(),
        "task_id":    req.task_id,
        "error":      req.error,
        "traceback":  req.traceback or _default_traceback(req.dag_id, req.task_id, req.error),
    }
    return {"run_id": run_id, "message": f"Failure injected for DAG '{req.dag_id}'"}


@app.delete("/mock/fail/{dag_id}")
def clear_failure(dag_id: str):
    """Remove failed runs for a DAG. Also writes 'pass' back to dag_status.txt."""
    removed = [k for k, v in _runs.items() if v["dag_id"] == dag_id]
    for k in removed:
        del _runs[k]
    # Write 'pass' back so the file reflects the fix
    try:
        _STATUS_FILE.write_text("pass\n")
    except OSError:
        pass
    return {"removed": len(removed)}


@app.get("/mock/runs")
def list_mock_runs():
    runs = list(_runs.values())
    file_run = _status_file_run()
    if file_run:
        runs.append(file_run)
    return {"runs": runs}


@app.get("/health")
def health():
    return {"status": "ok"}


# ── helpers ───────────────────────────────────────────────────────────────────

def _default_traceback(dag_id: str, task_id: str, error: str) -> str:
    return f"""\
Traceback (most recent call last):
  File "/opt/airflow/dags/{dag_id}.py", line 18, in {task_id}
    df = pd.read_csv(path)
{error}
"""


if __name__ == "__main__":
    uvicorn.run("airflow_mock.server:app", host="127.0.0.1", port=8088, reload=False)
