"""Hamilton DAG — turns a raw Airflow failure email into a fix context.

Pipeline
--------
  email_body          raw text of the Airflow alert email
      ↓
  dag_info            dag_id, task_id, execution_date extracted by regex
      ↓
  error_snippet       the traceback / error line from the email
      ↓
  error_type          classify into one of 11 categories
      ↓
  dag_file_content    read the failing DAG from the local repo
      ↓
  fix_context         dict ready to pass to Claude
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional


# ── node 1 ────────────────────────────────────────────────────────────────────

def dag_info(email_body: str) -> dict:
    """Pull dag_id, task_id, execution_date out of an Airflow failure email."""
    dag_id = _find(r"DAG[:\s]+([^\s\n,]+)", email_body) or "unknown_dag"
    task_id = _find(r"Task[:\s]+([^\s\n,]+)", email_body) or "unknown_task"
    exec_date = _find(r"execution[_ ]date[:\s]+([^\s\n,]+)", email_body, re.I) or ""
    return {"dag_id": dag_id, "task_id": task_id, "execution_date": exec_date}


# ── node 2 ────────────────────────────────────────────────────────────────────

def error_snippet(email_body: str) -> str:
    """Extract the traceback block from the email."""
    match = re.search(r"(Traceback.*?)(?:\n\n|\Z)", email_body, re.S)
    if match:
        return match.group(1).strip()
    lines = [l for l in email_body.splitlines() if l.strip()]
    return "\n".join(lines[-10:])


# ── node 3 ────────────────────────────────────────────────────────────────────

def error_type(error_snippet: str) -> str:
    """Classify the error into one of 11 actionable categories.

    Categories
    ----------
    format_mismatch   - wrong file format (CSV expected, PDF/Excel/JSON received)
    missing_file      - FileNotFoundError, no files in directory
    permission_error  - PermissionError, EACCES
    schema_change     - KeyError on column, column not found, unexpected schema
    connection_error  - DB/API timeout, connection refused, host unreachable
    import_error      - ModuleNotFoundError, ImportError, pip install missing
    memory_error      - MemoryError, OOM kill
    type_error        - TypeError, wrong type passed to function
    value_error       - ValueError (not a format issue)
    dependency_failed - upstream task failed, depends_on_past blocked
    unknown           - catch-all, lets Claude free-analyse the traceback
    """
    s = error_snippet.lower()

    if any(k in s for k in ("parsererror", "tokenizing data", "utf-8", "codec can",
                             "unicodedecodeerror", "not a valid csv")):
        return "format_mismatch"

    if any(k in s for k in ("filenotfounderror", "no such file", "no files found",
                             "path does not exist", "enoent")):
        return "missing_file"

    if any(k in s for k in ("permissionerror", "permission denied", "eacces")):
        return "permission_error"

    if any(k in s for k in ("keyerror", "column not found", "does not exist in",
                             "unexpected column", "dataframe has no column",
                             "schemamismatch", "schema mismatch")):
        return "schema_change"

    if any(k in s for k in ("connectionerror", "connectionrefusederror", "timed out",
                             "timeout expired", "operationalerror", "can't connect",
                             "cannot connect", "host unreachable", "network is unreachable",
                             "broken pipe", "ssl", "certificate")):
        return "connection_error"

    if any(k in s for k in ("modulenotfounderror", "importerror", "no module named",
                             "cannot import name")):
        return "import_error"

    if any(k in s for k in ("memoryerror", "out of memory", "oom kill", "killed")):
        return "memory_error"

    if "typeerror" in s:
        return "type_error"

    if "valueerror" in s:
        return "value_error"

    if any(k in s for k in ("upstream task", "depends_on_past", "all dependencies",
                             "task depends on", "upstream failed")):
        return "dependency_failed"

    return "unknown"


# ── node 4 ────────────────────────────────────────────────────────────────────

def dag_file_content(dag_info: dict, github_token: str, github_repo: str) -> str:
    """Read the DAG source from the local repo (GITHUB_REPO_LOCAL_PATH/dags/<dag_id>.py).

    Falls back to a synthetic snippet when the file is not found, so the agent
    still produces a fix during a demo run without a real GitHub setup.
    """
    import os

    dag_id = dag_info["dag_id"]
    local_root = os.environ.get("GITHUB_REPO_LOCAL_PATH", "")
    local_path = Path(local_root) / "dags" / f"{dag_id}.py" if local_root else None

    if local_path and local_path.exists():
        return local_path.read_text(encoding="utf-8")

    # Try GitHub only when a real token/repo is configured
    if github_token not in ("dummy", "", "ghp_xxxx") and github_repo not in ("dummy/repo", ""):
        import urllib.request, json, base64
        path = f"dags/{dag_id}.py"
        url = f"https://api.github.com/repos/{github_repo}/contents/{path}"
        try:
            req = urllib.request.Request(
                url,
                headers={"Authorization": f"Bearer {github_token}",
                         "Accept": "application/vnd.github.v3+json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                return base64.b64decode(data["content"]).decode()
        except Exception:
            pass

    return _demo_dag_code(dag_id)


# ── node 5 ────────────────────────────────────────────────────────────────────

def fix_context(dag_info: dict, error_snippet: str, error_type: str, dag_file_content: str) -> dict:
    """Package everything Claude needs to write the fix."""
    return {
        "dag_id":        dag_info["dag_id"],
        "task_id":       dag_info["task_id"],
        "error_type":    error_type,
        "error_snippet": error_snippet,
        "dag_code":      dag_file_content,
    }


# ── helpers ───────────────────────────────────────────────────────────────────

def _find(pattern: str, text: str, flags: int = 0) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def _demo_dag_code(dag_id: str) -> str:
    return f'''\
# DAG: {dag_id}  (demo fallback — set GITHUB_REPO_LOCAL_PATH to use the real file)
import pandas as pd
from pathlib import Path

DATA_DIR = Path("/opt/airflow/sample_data")

def parse_and_count(file_paths):
    results = {{}}
    for path in file_paths:
        df = pd.read_csv(path)   # BUG: no format check
        results[Path(path).name] = len(df)
    return results
'''
