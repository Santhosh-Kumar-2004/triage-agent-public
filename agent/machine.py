"""Burr state machine — one instance per failed DAG run.

States
------
  detected      Email found; triage has started.
  diagnosing    Hamilton pipeline is running.
  fixing        Claude is generating the patch.
  creating_pr   Pushing branch and opening PR on GitHub.
  done          PR raised successfully.
  needs_human   Agent could not fix it; a person must look.
"""

from __future__ import annotations

import uuid
from typing import Optional

from burr.core import Application, ApplicationBuilder, State, action

# ── state keys ────────────────────────────────────────────────────────────────
EMAIL_ID    = "email_id"
DAG_ID      = "dag_id"
TASK_ID     = "task_id"
ERROR_MSG   = "error_msg"
ERROR_TYPE  = "error_type"      # e.g. "format_mismatch"
DAG_CODE    = "dag_code"
PATCH       = "patch"           # unified diff the agent will commit
PR_URL      = "pr_url"
PHASE       = "phase"
FAIL_REASON = "fail_reason"

# ── phase constants ────────────────────────────────────────────────────────────
DETECTED    = "detected"
DIAGNOSING  = "diagnosing"
FIXING      = "fixing"
CREATING_PR = "creating_pr"
DONE        = "done"
NEEDS_HUMAN = "needs_human"


@action(reads=[], writes=[PHASE, EMAIL_ID, DAG_ID, TASK_ID, ERROR_MSG])
def detect(state: State, email_id: str, dag_id: str, task_id: str, error_msg: str):
    update = {PHASE: DETECTED, EMAIL_ID: email_id,
              DAG_ID: dag_id, TASK_ID: task_id, ERROR_MSG: error_msg}
    return update, state.update(**update)


@action(reads=[DAG_ID, ERROR_MSG], writes=[PHASE, ERROR_TYPE, DAG_CODE])
def diagnose(state: State, error_type: str, dag_code: str):
    update = {PHASE: DIAGNOSING, ERROR_TYPE: error_type, DAG_CODE: dag_code}
    return update, state.update(**update)


@action(reads=[ERROR_TYPE, DAG_CODE], writes=[PHASE, PATCH])
def fix(state: State, patch: str):
    update = {PHASE: FIXING, PATCH: patch}
    return update, state.update(**update)


@action(reads=[PATCH, DAG_ID], writes=[PHASE, PR_URL])
def create_pr(state: State, pr_url: str):
    update = {PHASE: CREATING_PR, PR_URL: pr_url}
    return update, state.update(**update)


@action(reads=[], writes=[PHASE])
def complete(state: State):
    update = {PHASE: DONE}
    return update, state.update(**update)


@action(reads=[], writes=[PHASE, FAIL_REASON])
def escalate(state: State, reason: str):
    update = {PHASE: NEEDS_HUMAN, FAIL_REASON: reason}
    return update, state.update(**update)


def build_machine(run_id: str, enable_tracker: bool = False) -> Application:
    builder = (
        ApplicationBuilder()
        .with_actions(detect, diagnose, fix, create_pr, complete, escalate)
        .with_transitions(
            ("detect",     "diagnose"),
            ("diagnose",   "fix"),
            ("fix",        "create_pr"),
            ("create_pr",  "complete"),
        )
        .with_state(**{
            PHASE: "", EMAIL_ID: "", DAG_ID: "", TASK_ID: "",
            ERROR_MSG: "", ERROR_TYPE: "", DAG_CODE: "",
            PATCH: "", PR_URL: "", FAIL_REASON: "",
        })
        .with_entrypoint("detect")
        .with_identifiers(app_id=run_id, partition_key="triage")
    )
    if enable_tracker:
        from burr.tracking import LocalTrackingClient
        builder = builder.with_tracker(LocalTrackingClient(project="triage-agent"))
    return builder.build()
