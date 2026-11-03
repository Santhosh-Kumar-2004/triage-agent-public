"""Manually run the fix pipeline for a DAG — no agent polling needed.

Usage:
    python scripts/fix.py
    python scripts/fix.py csv_processor parse_and_count "pandas.errors.ParserError: Error tokenizing data"
    python scripts/fix.py my_dag my_task "KeyError: 'customer_id'"

The patch is saved to patches/<dag_id>-<error_type>.diff
"""

import sys
import logging
import os

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(_root)
sys.path.insert(0, _root)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fix")

dag_id  = sys.argv[1] if len(sys.argv) > 1 else "csv_processor"
task_id = sys.argv[2] if len(sys.argv) > 2 else "parse_and_count"
error   = sys.argv[3] if len(sys.argv) > 3 else "pandas.errors.ParserError: Error tokenizing data"

traceback = f"""\
Traceback (most recent call last):
  File "dags/{dag_id}.py", line 18, in {task_id}
    df = pd.read_csv(path)
{error}
"""

from hamilton.driver import Driver
from agent import diagnosis
from agent.config import Settings
from agent.fixer import generate_fix
from pathlib import Path

settings = Settings()

# ── 1. Diagnose ───────────────────────────────────────────────────────────────
email_body = f"DAG: {dag_id}\nTask: {task_id}\n\n{traceback}"
log.info("Running diagnosis for DAG '%s' task '%s'...", dag_id, task_id)

dr = Driver({}, diagnosis)
result = dr.execute(
    ["fix_context"],
    inputs={
        "email_body":   email_body,
        "github_token": settings.github_token,
        "github_repo":  settings.github_repo,
    },
)
ctx = result["fix_context"]
log.info("Error classified as: %s", ctx["error_type"])

# ── 2. Generate fix ───────────────────────────────────────────────────────────
log.info("Asking %s to generate a fix...", settings.llm_provider)
api_key = settings.gemini_api_key if settings.llm_provider == "gemini" else settings.anthropic_api_key
patch = generate_fix(
    ctx, api_key,
    provider=settings.llm_provider,
    model=settings.gemini_model if settings.llm_provider == "gemini" else "",
)

# ── 3. Save patch ─────────────────────────────────────────────────────────────
patches_dir = Path(settings.github_repo_local_path or ".") / "patches"
patches_dir.mkdir(exist_ok=True)
out = patches_dir / f"{dag_id}-{ctx['error_type']}.diff"
out.write_text(patch, encoding="utf-8")

print()
print("=" * 60)
print(f"  Patch saved to: {out}")
print(f"  Error type    : {ctx['error_type']}")
print()
print("Diff preview:")
print(patch[:800])
print("=" * 60)
