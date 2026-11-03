"""Send email alerts via SMTP (Gmail App Password)."""

from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText

log = logging.getLogger("triage.notifier")


def send_alert(
    *,
    smtp_host: str,
    smtp_port: int,
    username: str,
    password: str,
    to: str,
    subject: str,
    body: str,
) -> None:
    """Send one plain-text email. Logs and swallows errors so the agent never crashes."""
    if not username or not password or not to:
        log.info("SMTP not configured — skipping alert: %s", subject)
        return
    try:
        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"] = username
        msg["To"] = to

        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as smtp:
            smtp.starttls()
            smtp.login(username, password)
            smtp.sendmail(username, [to], msg.as_string())

        log.info("Alert sent → %s: %s", to, subject)
    except Exception as exc:
        log.warning("Email alert failed (fix still runs): %s", exc)


def dag_failed(dag_id: str, task_id: str, error_msg: str, settings) -> None:
    send_alert(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        username=settings.smtp_username,
        password=settings.smtp_password,
        to=settings.alert_to,
        subject=f"[Triage Agent] DAG '{dag_id}' failed — working on a fix",
        body=(
            f"The triage agent detected a failure in DAG '{dag_id}'.\n\n"
            f"Task      : {task_id}\n"
            f"Error     : {error_msg}\n\n"
            "Working on an automatic fix now. You will get another email once the patch is ready."
        ),
    )


def pr_raised(dag_id: str, error_type: str, pr_url: str, settings) -> None:
    send_alert(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        username=settings.smtp_username,
        password=settings.smtp_password,
        to=settings.alert_to,
        subject=f"[Triage Agent] PR raised for {dag_id}",
        body=(
            f"The triage agent automatically fixed a failure in DAG '{dag_id}'.\n\n"
            f"Error type : {error_type}\n"
            f"PR         : {pr_url}\n\n"
            "Please review the diff before merging."
        ),
    )


def escalated(dag_id: str, reason: str, settings) -> None:
    send_alert(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        username=settings.smtp_username,
        password=settings.smtp_password,
        to=settings.alert_to,
        subject=f"[Triage Agent] Human needed for {dag_id}",
        body=(
            f"The triage agent could not automatically fix the failure in DAG '{dag_id}'.\n\n"
            f"Reason: {reason}\n\n"
            "Please investigate manually."
        ),
    )
