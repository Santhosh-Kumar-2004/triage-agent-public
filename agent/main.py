"""Triage agent entry point — monitors Airflow 24/7.

Flow
----
  1. Poll Airflow REST API every POLL_INTERVAL_SECONDS
  2. When a failed run is found (first time we see its run_id):
       a. Send you a "DAG failed, working on a fix" email
       b. Run Hamilton diagnosis DAG → classify error → fetch DAG code
       c. Ask Gemini to generate the unified diff fix
       d. Save patch to patches/<dag_id>-<error_type>.diff
       e. Send you a "fix ready" email with the patch path
  3. If anything goes wrong → send you a "human needed" escalation email

Run:
    # Terminal 1 — start the mock Airflow server
    .venv/Scripts/python -m airflow_mock.server

    # Terminal 2 — start the agent
    .venv/Scripts/python -m agent.main

    # Terminal 3 — inject a failure to watch the agent respond
    curl -X POST http://localhost:8080/mock/fail ^
         -H "Content-Type: application/json" ^
         -d "{\"dag_id\":\"csv_processor\",\"task_id\":\"parse_and_count\",\"error\":\"pandas.errors.ParserError: Error tokenizing data\"}"
"""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path

from hamilton.driver import Driver

from agent import diagnosis
from agent.airflow_client import AirflowClient
from agent.config import Settings
from agent.fixer import generate_fix
from agent.machine import build_machine, PHASE
from agent import notifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("triage")


def _save_patch(dag_id: str, error_type: str, patch: str, base_path: str) -> str:
    patches_dir = Path(base_path or ".") / "patches"
    patches_dir.mkdir(exist_ok=True)
    out = patches_dir / f"{dag_id}-{error_type}.diff"
    out.write_text(patch, encoding="utf-8")
    return str(out)


def run_triage(run, settings: Settings, client: "AirflowClient | None" = None) -> str:
    """Full triage cycle for one failed DAG run. Returns patch path or 'needs_human'."""
    triage_id = f"{run.dag_id}-{uuid.uuid4().hex[:8]}"
    log.info("[%s] triage started — dag=%s run=%s", triage_id, run.dag_id, run.run_id)

    machine = build_machine(triage_id, enable_tracker=settings.enable_burr_tracker)

    # ── step 1: detect ───────────────────────────────────────────────────────
    machine.step(inputs={
        "email_id":  run.run_id,
        "dag_id":    run.dag_id,
        "task_id":   run.task_id,
        "error_msg": run.error_msg,
    })

    # Send failure notification to the user
    notifier.dag_failed(run.dag_id, run.task_id, run.error_msg, settings)
    log.info("[%s] failure alert sent", triage_id)

    # ── step 2: diagnose (Hamilton DAG) ──────────────────────────────────────
    email_body = (
        f"DAG: {run.dag_id}\n"
        f"Task: {run.task_id}\n\n"
        f"{run.traceback}"
    )
    try:
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
    except Exception as exc:
        log.error("[%s] diagnosis failed: %s", triage_id, exc)
        notifier.escalated(run.dag_id, f"diagnosis failed: {exc}", settings)
        return "needs_human"

    machine.step(inputs={"error_type": ctx["error_type"], "dag_code": ctx["dag_code"]})
    log.info("[%s] diagnosed — error_type=%s", triage_id, ctx["error_type"])

    # ── step 3: fix (Gemini) ─────────────────────────────────────────────────
    try:
        api_key = settings.gemini_api_key if settings.llm_provider == "gemini" else settings.anthropic_api_key
        patch = generate_fix(
            ctx, api_key,
            provider=settings.llm_provider,
            model=settings.gemini_model if settings.llm_provider == "gemini" else "",
        )
    except Exception as exc:
        log.error("[%s] fix generation failed: %s", triage_id, exc)
        notifier.escalated(run.dag_id, f"LLM fix failed: {exc}", settings)
        return "needs_human"

    machine.step(inputs={"patch": patch})
    log.info("[%s] fix generated (%d chars)", triage_id, len(patch))

    # ── step 4: save patch ───────────────────────────────────────────────────
    try:
        patch_path = _save_patch(run.dag_id, ctx["error_type"], patch,
                                 settings.github_repo_local_path)
    except Exception as exc:
        log.error("[%s] could not save patch: %s", triage_id, exc)
        notifier.escalated(run.dag_id, f"patch save failed: {exc}", settings)
        return "needs_human"

    machine.step(inputs={"pr_url": patch_path})
    machine.step()  # complete
    log.info("[%s] DONE — patch: %s", triage_id, patch_path)

    # Clear the failure from Airflow so next poll shows 0 failed runs
    if client is not None:
        client.resolve_run(run.dag_id)

    notifier.pr_raised(run.dag_id, ctx["error_type"], patch_path, settings)
    return patch_path


def main() -> None:
    settings = Settings()
    client = AirflowClient(
        base_url=settings.airflow_url,
        username=settings.airflow_username,
        password=settings.airflow_password,
    )

    seen_runs: set[str] = set()   # run_ids already processed this session
    log.info("Triage agent started — watching Airflow at %s every %ds",
             settings.airflow_url, settings.poll_interval_seconds)

    while True:
        failed = client.get_failed_runs()
        new = [r for r in failed if r.run_id not in seen_runs]

        if new:
            log.info("Found %d new failed run(s)", len(new))

        for run in new:
            seen_runs.add(run.run_id)
            try:
                run_triage(run, settings, client)
            except Exception as exc:
                log.exception("Unexpected error triaging %s: %s", run.dag_id, exc)

        time.sleep(settings.poll_interval_seconds)


if __name__ == "__main__":
    main()
