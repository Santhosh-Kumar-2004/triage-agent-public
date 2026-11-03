"""Minimal Airflow REST API mock.

Runs on http://localhost:8080.
The agent polls it for failed DAG runs.
You trigger failures manually via the /mock/* endpoints.

Start:
    .venv/Scripts/python -m airflow_mock.server

Trigger a failure (in another terminal):
    curl -X POST http://localhost:8088/mock/fail -H "Content-Type: application/json" \
         -d '{"dag_id":"csv_processor","task_id":"parse_and_count","error":"pandas.errors.ParserError: Error tokenizing data"}'

Reset (mark run as fixed / remove it):
    curl -X DELETE http://localhost:8088/mock/fail/csv_processor
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
    import uvicorn
except ImportError:
    raise SystemExit("fastapi and uvicorn are required: pip install fastapi uvicorn")

app = FastAPI(title="Airflow Mock")

# ── in-memory state ───────────────────────────────────────────────────────────
# dag_id → run record
_runs: dict[str, dict] = {}


class FailureRequest(BaseModel):
    dag_id: str
    task_id: str = "unknown_task"
    error: str = "Simulated Airflow failure"
    traceback: Optional[str] = None


# ── Airflow-compatible endpoints ──────────────────────────────────────────────

@app.get("/api/v1/dags/{dag_id}/dagRuns")
def list_dag_runs(dag_id: str, state: Optional[str] = None):
    """Returns runs for a DAG, optionally filtered by state."""
    runs = [r for r in _runs.values() if r["dag_id"] == dag_id]
    if state:
        runs = [r for r in runs if r["state"] == state]
    return {"dag_runs": runs, "total_entries": len(runs)}


@app.get("/api/v1/dagRuns")
def list_all_dag_runs(state: Optional[str] = None):
    """Returns all runs across all DAGs, optionally filtered by state."""
    runs = list(_runs.values())
    if state:
        runs = [r for r in runs if r["state"] == state]
    return {"dag_runs": runs, "total_entries": len(runs)}


@app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1")
def get_task_log(dag_id: str, run_id: str, task_id: str):
    """Returns the task log (error traceback) for a specific task instance."""
    run = _runs.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"content": run.get("traceback") or run.get("error", "No log available")}


# ── mock control endpoints ────────────────────────────────────────────────────

@app.post("/mock/fail", status_code=201)
def inject_failure(req: FailureRequest):
    """Inject a failed DAG run. The agent will pick this up on its next poll."""
    run_id = f"manual_run_{req.dag_id}_{uuid.uuid4().hex[:8]}"
    _runs[run_id] = {
        "dag_run_id": run_id,
        "dag_id": req.dag_id,
        "state": "failed",
        "start_date": datetime.now(timezone.utc).isoformat(),
        "end_date": datetime.now(timezone.utc).isoformat(),
        "task_id": req.task_id,
        "error": req.error,
        "traceback": req.traceback or _default_traceback(req.dag_id, req.task_id, req.error),
    }
    return {"run_id": run_id, "message": f"Failure injected for DAG '{req.dag_id}'"}


@app.delete("/mock/fail/{dag_id}")
def clear_failure(dag_id: str):
    """Remove all failed runs for a DAG (simulate it being resolved)."""
    removed = [k for k, v in _runs.items() if v["dag_id"] == dag_id]
    for k in removed:
        del _runs[k]
    return {"removed": len(removed)}


@app.get("/mock/runs")
def list_mock_runs():
    """Inspect all current runs (debug helper)."""
    return {"runs": list(_runs.values())}


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
