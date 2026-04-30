#!/usr/bin/env bash
# Generic cron wrapper: runs a command, logs stdout+stderr, marks success.
#
# Failure and prediction notifications are handled by the Python pipeline
# itself (see src/mvp/notify.py). The wrapper exports the webhook URLs so
# the Python child process can read them, but it does not post directly.
# Hard crashes that bypass Python's exception handler are caught by the
# hourly heartbeat check (~/projects/mvp/ops/live/bin/check-heartbeat.sh).

set -uo pipefail

# Cron's default PATH is sparse (/usr/bin:/bin); user-local installs
# like ~/.local/bin/poetry aren't reachable. Prepend the standard
# user-local bin dirs so the wrapped command can find them.
export PATH="$HOME/.local/bin:$HOME/bin:$PATH"

NAME=""
DIR=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --name) NAME="$2"; shift 2 ;;
        --dir)  DIR="$2";  shift 2 ;;
        --)     shift; break ;;
        *)      break ;;
    esac
done

if [[ -z "$NAME" || -z "$DIR" ]]; then
    echo "Usage: run-job.sh --name <job-name> --dir <working-dir> -- <command...>" >&2
    exit 1
fi

source ~/.config/mvp/notify.env
export DISCORD_WEBHOOK_URL DISCORD_PREDICTIONS_WEBHOOK_URL

LOG_DIR="$HOME/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$NAME-$(date +%Y-%m-%d).log"

cd "$DIR"
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

EXIT_CODE=0
"$@" >> "$LOG_FILE" 2>&1 || EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
    touch "$HOME/logs/.last-success"
fi

exit $EXIT_CODE
