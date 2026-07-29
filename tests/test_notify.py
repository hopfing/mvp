"""Tests for Discord webhook notify module."""

from unittest.mock import patch

import pytest
import requests

from mvp import notify


@pytest.fixture
def env_set(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/webhook")
    monkeypatch.setenv(
        "DISCORD_PREDICTIONS_WEBHOOK_URL", "https://discord.test/predictions"
    )


@pytest.fixture
def env_unset(monkeypatch):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DISCORD_PREDICTIONS_WEBHOOK_URL", raising=False)


def test_post_failure_targets_main_webhook(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_failure("mvp-live", "stage 1 blew up")
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://discord.test/webhook"
    content = kwargs["json"]["content"]
    assert "mvp-live FAILED" in content
    assert "stage 1 blew up" in content


def test_post_predictions_skips_zero_count(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_predictions("mvp-live", 0)
    mock_post.assert_not_called()


def test_post_predictions_skips_negative_count(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_predictions("mvp-live", -3)
    mock_post.assert_not_called()


def test_post_predictions_targets_predictions_webhook(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_predictions("mvp-live", 5)
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://discord.test/predictions"
    assert "5 new predictions" in kwargs["json"]["content"]


def test_post_alerts_skips_empty(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_alerts("mvp-live", {})
    mock_post.assert_not_called()


def test_post_alerts_targets_predictions_webhook(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_alerts("mvp-live", {"Clay challenger value": 2})
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://discord.test/predictions"
    assert "Clay challenger value" in kwargs["json"]["content"]
    assert "2 predictions" in kwargs["json"]["content"]


def test_post_alerts_singular_count(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_alerts("mvp-live", {"Short prices": 1})
    _, kwargs = mock_post.call_args
    assert "1 prediction" in kwargs["json"]["content"]
    assert "1 predictions" not in kwargs["json"]["content"]


def test_post_alerts_lists_every_rule(env_set):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_alerts("mvp-live", {"Rule A": 2, "Rule B": 1})
    _, kwargs = mock_post.call_args
    content = kwargs["json"]["content"]
    assert "Rule A" in content
    assert "Rule B" in content


def test_no_op_when_env_unset(env_unset):
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_failure("mvp-live", "msg")
        notify.post_predictions("mvp-live", 5)
        notify.post_alerts("mvp-live", {"Rule A": 1})
    mock_post.assert_not_called()


def test_swallows_connection_error(env_set):
    with patch.object(
        notify.requests, "post", side_effect=ConnectionError("network down")
    ):
        notify.post_failure("mvp-live", "msg")  # must not raise
        notify.post_predictions("mvp-live", 1)  # must not raise


def test_swallows_http_error(env_set):
    class FakeResp:
        def raise_for_status(self):
            raise requests.HTTPError("server died")

    with patch.object(notify.requests, "post", return_value=FakeResp()):
        notify.post_failure("mvp-live", "msg")  # must not raise


def test_content_truncated_at_discord_limit(env_set):
    long_msg = "x" * 5000
    with patch.object(notify.requests, "post") as mock_post:
        notify.post_failure("mvp-live", long_msg)
    args, kwargs = mock_post.call_args
    assert len(kwargs["json"]["content"]) <= 2000
