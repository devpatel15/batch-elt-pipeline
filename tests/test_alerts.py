import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import alerts  # noqa: E402


def _fake_context():
    ti = MagicMock()
    ti.dag_id = "daily_elt_dag"
    ti.task_id = "extract_gharchive"
    ti.log_url = "http://localhost:8080/log"
    return {"task_instance": ti, "run_id": "manual__2026-08-06T00:00:00"}


def test_build_failure_message_includes_dag_task_and_log_url():
    message = alerts.build_failure_message(_fake_context())
    assert "daily_elt_dag" in message
    assert "extract_gharchive" in message
    assert "http://localhost:8080/log" in message


def test_alert_on_failure_skips_post_when_webhook_not_configured(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with patch.object(alerts.requests, "post") as mock_post:
        alerts.alert_on_failure(_fake_context())
        mock_post.assert_not_called()


def test_alert_on_failure_posts_to_webhook_when_configured(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/T000/B000/xxx")
    with patch.object(alerts.requests, "post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=lambda: None)
        alerts.alert_on_failure(_fake_context())
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://hooks.slack.example/T000/B000/xxx"
        assert "daily_elt_dag" in kwargs["json"]["text"]


def test_alert_on_failure_swallows_request_errors(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/T000/B000/xxx")
    with patch.object(alerts.requests, "post", side_effect=alerts.requests.RequestException("boom")):
        alerts.alert_on_failure(_fake_context())  # must not raise
