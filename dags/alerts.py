"""Slack alerting for Airflow task/DAG failure callbacks.

Reads the webhook URL from the SLACK_WEBHOOK_URL env var (wired through
docker-compose.yml). If unset, failures are logged instead of raising -
alerting is best-effort and must never mask the underlying task failure.
"""

from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 10


def build_failure_message(context: dict) -> str:
    ti = context["task_instance"]
    return (
        f":red_circle: *{ti.dag_id}* failed\n"
        f"Task: `{ti.task_id}` | Run: `{context['run_id']}`\n"
        f"Log: {ti.log_url}"
    )


def alert_on_failure(context: dict) -> None:
    message = build_failure_message(context)
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set; skipping Slack alert. Failure was: %s", message)
        return

    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("failed to send Slack alert for %s", context["task_instance"].task_id)
