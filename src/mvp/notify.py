"""Discord webhook notifications for the live pipeline.

Webhook URLs are read from environment variables, set on the host via
``~/.config/mvp/notify.env`` and sourced + exported by the cron wrapper
(``ops/live/bin/run-job.sh``).

Posts are best-effort: if the env var is unset or the request fails, the
function logs and returns rather than raising. The pipeline must never fail
because the webhook is unreachable.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

_DISCORD_CONTENT_LIMIT = 2000


def _post(webhook_url: str | None, content: str) -> None:
    if not webhook_url:
        logger.debug("Webhook URL unset; skipping post.")
        return
    body = {"content": content[:_DISCORD_CONTENT_LIMIT]}
    try:
        resp = requests.post(webhook_url, json=body, timeout=5)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Discord post failed: %s", e)


def post_failure(name: str, message: str) -> None:
    """Post a failure alert to ``DISCORD_WEBHOOK_URL``."""
    content = f"**{name} FAILED**\n```\n{message}\n```"
    _post(os.environ.get("DISCORD_WEBHOOK_URL"), content)


def post_predictions(name: str, count: int) -> None:
    """Post a 'new predictions synced' alert to ``DISCORD_PREDICTIONS_WEBHOOK_URL``.

    No-op if ``count <= 0``.
    """
    if count <= 0:
        return
    content = f"**{name}** — {count} new predictions synced"
    _post(os.environ.get("DISCORD_PREDICTIONS_WEBHOOK_URL"), content)
