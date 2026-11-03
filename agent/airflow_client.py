"""Thin wrapper around the Airflow REST API.

Works against both the real Airflow webserver and the local mock.
"""

from __future__ import annotations

import urllib.request
import json
import base64
import logging
from dataclasses import dataclass

log = logging.getLogger("triage.airflow")


@dataclass
class FailedRun:
    run_id: str
    dag_id: str
    task_id: str
    error_msg: str
    traceback: str


class AirflowClient:
    def __init__(self, base_url: str, username: str = "admin", password: str = "admin"):
        self.base_url = base_url.rstrip("/")
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        self._auth = f"Basic {token}"

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{qs}"
        req = urllib.request.Request(url, headers={
            "Authorization": self._auth,
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())

    def get_failed_runs(self) -> list[FailedRun]:
        """Return all currently failed DAG runs across all DAGs."""
        try:
            data = self._get("/api/v1/dagRuns", {"state": "failed"})
        except Exception as exc:
            log.error("Could not reach Airflow at %s: %s", self.base_url, exc)
            return []

        results = []
        for run in data.get("dag_runs", []):
            dag_id  = run["dag_id"]
            run_id  = run["dag_run_id"]
            task_id = run.get("task_id", "unknown_task")
            error   = run.get("error", "")
            tb      = run.get("traceback", "") or self._fetch_log(dag_id, run_id, task_id)

            if not tb:
                tb = f"DAG: {dag_id}\nTask: {task_id}\n\nTraceback (most recent call last):\n  {error}"

            results.append(FailedRun(
                run_id=run_id,
                dag_id=dag_id,
                task_id=task_id,
                error_msg=error,
                traceback=tb,
            ))
        return results

    def _delete(self, path: str) -> None:
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, headers={
            "Authorization": self._auth,
        }, method="DELETE")
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()

    def resolve_run(self, dag_id: str) -> None:
        """Clear all failed runs for a DAG after the fix is applied.

        On the mock server this calls DELETE /mock/fail/{dag_id}.
        On a real Airflow server this is a no-op (clearing runs is done via the UI).
        """
        try:
            self._delete(f"/mock/fail/{dag_id}")
            log.info("Resolved (cleared) failed runs for DAG '%s'", dag_id)
        except Exception as exc:
            log.debug("resolve_run skipped (not a mock server or already clear): %s", exc)

    def _fetch_log(self, dag_id: str, run_id: str, task_id: str) -> str:
        try:
            path = f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
            data = self._get(path)
            return data.get("content", "")
        except Exception:
            return ""
