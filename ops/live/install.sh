#!/usr/bin/env bash
# Install/refresh the live pipeline cron entries on the current host.
# Idempotent: safe to re-run after every `git pull`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NOTIFY_ENV="$HOME/.config/mvp/notify.env"

echo "==> ops/live/install.sh"

mkdir -p "$HOME/logs" "$HOME/.config/mvp"

if [[ ! -s "$NOTIFY_ENV" ]]; then
    echo "    notify.env missing — copying template to $NOTIFY_ENV"
    cp "$SCRIPT_DIR/config/notify.env.example" "$NOTIFY_ENV"
    chmod 600 "$NOTIFY_ENV"
    echo "    fill in the two webhook URLs (no leading whitespace) and re-run."
    exit 1
fi

# Verify notify.env actually exports both vars.
# Catches the leading-whitespace bug we hit on 2026-04-30.
set +u
# shellcheck disable=SC1090
source "$NOTIFY_ENV"
if [[ -z "${DISCORD_WEBHOOK_URL:-}" || -z "${DISCORD_PREDICTIONS_WEBHOOK_URL:-}" ]]; then
    echo "    ERROR: notify.env did not export both webhook vars."
    echo "    Check for leading whitespace on the assignment lines."
    exit 1
fi
set -u

echo "==> installing crontab from $SCRIPT_DIR/crontab"
crontab "$SCRIPT_DIR/crontab"

echo "==> active crontab:"
crontab -l
echo "==> done."
