"""Poll an IMAP mailbox for Airflow failure alert emails."""

from __future__ import annotations

import email
import imaplib
import re
from dataclasses import dataclass


@dataclass
class AirflowAlert:
    email_id: str
    dag_id: str
    task_id: str
    error_msg: str
    raw_body: str


def poll_inbox(
    host: str,
    port: int,
    username: str,
    password: str,
    folder: str = "INBOX",
    subject_filter: str = "Airflow alert",
) -> list[AirflowAlert]:
    """Return all unread Airflow failure emails and mark them as read."""
    alerts: list[AirflowAlert] = []

    conn = imaplib.IMAP4_SSL(host, port)
    conn.login(username, password)
    conn.select(folder)

    # Search for unread messages whose subject contains the filter string
    _, msg_ids = conn.search(None, f'(UNSEEN SUBJECT "{subject_filter}")')
    for mid in msg_ids[0].split():
        _, data = conn.fetch(mid, "(RFC822)")
        msg = email.message_from_bytes(data[0][1])
        body = _extract_body(msg)
        alert = _parse_alert(mid.decode(), body)
        if alert:
            alerts.append(alert)
            # Mark as read so we don't re-process
            conn.store(mid, "+FLAGS", "\\Seen")

    conn.logout()
    return alerts


def _extract_body(msg: email.message.Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                return part.get_payload(decode=True).decode(errors="replace")
    return msg.get_payload(decode=True).decode(errors="replace")


def _parse_alert(email_id: str, body: str) -> AirflowAlert | None:
    dag_id  = _find(r"DAG[:\s]+([^\s\n,]+)", body)
    task_id = _find(r"Task[:\s]+([^\s\n,]+)", body) or ""
    if not dag_id:
        return None
    # Grab last ~20 lines as the error snippet
    lines = [l for l in body.splitlines() if l.strip()]
    error_msg = "\n".join(lines[-20:])
    return AirflowAlert(email_id=email_id, dag_id=dag_id,
                        task_id=task_id, error_msg=error_msg, raw_body=body)


def _find(pattern: str, text: str) -> str | None:
    m = re.search(pattern, text, re.I)
    return m.group(1).strip() if m else None
