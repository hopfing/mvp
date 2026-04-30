#!/usr/bin/env bash
# Hourly heartbeat check: alerts if the live pipeline hasn't successfully
# completed in over an hour. The marker file (~/logs/.last-success) is
# touched by run-job.sh on every exit-zero pipeline run.

set -uo pipefail

source ~/.config/mvp/notify.env

if [[ -z "${DISCORD_WEBHOOK_URL:-}" ]]; then
    echo "ERROR: DISCORD_WEBHOOK_URL not set after sourcing notify.env" >&2
    exit 1
fi

LAST="$HOME/logs/.last-success"

if [[ ! -f "$LAST" ]]; then
    curl -s -H "Content-Type: application/json" \
        -d '{"content":"**mvp-live STALE** — no successful run recorded"}' \
        "$DISCORD_WEBHOOK_URL" > /dev/null
    exit 0
fi

AGE=$(( $(date +%s) - $(stat -c %Y "$LAST") ))
if [[ $AGE -gt 3600 ]]; then
    MINS=$(( AGE / 60 ))
    curl -s -H "Content-Type: application/json" \
        -d "{\"content\":\"**mvp-live STALE** — no successful run in ${MINS} minutes\"}" \
        "$DISCORD_WEBHOOK_URL" > /dev/null
fi
