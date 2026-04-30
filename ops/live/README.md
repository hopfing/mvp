# ops/live

Cron entries and shell wrappers for the live pipeline host. The host runs the
scripts directly out of this repo — no copies in `~/bin/`. "Deploy" is
`git pull && ops/live/install.sh`.

Currently deployed on: Beelink (Linux, user `happybees`).

## Layout

- `bin/run-job.sh` — generic cron wrapper. Runs a command in a working dir,
  appends stdout+stderr to a daily log, touches the heartbeat marker on
  success. Exports the Discord webhook URLs so the Python child can post.
- `bin/check-heartbeat.sh` — hourly check; alerts to Discord if no successful
  run is recorded in over an hour.
- `config/notify.env.example` — template for the two Discord webhooks. The
  real file lives at `~/.config/mvp/notify.env`, has mode 0600, and is **not**
  in version control.
- `crontab` — the three cron entries (15-min run, daily log cleanup, hourly
  heartbeat).
- `install.sh` — validates `notify.env` and replaces the user crontab with
  this repo's `crontab`. Idempotent.

## Where alerts come from

Two paths:

1. **Python (`src/mvp/notify.py`).** The pipeline itself posts:
   - `post_predictions` — when a sheets sync produces N>0 new bets.
   - `post_failure` — when any caught exception ends `mvp live` non-zero.
   These fire in real-time and use proper JSON encoding (no shell escaping
   pitfalls). Best-effort: a webhook outage never fails the pipeline.

2. **Bash (`bin/check-heartbeat.sh`, hourly cron).** Catches the cases Python
   can't catch — hard crashes, OOM-kills, the host going down. If
   `~/logs/.last-success` is older than an hour, posts a STALE alert.

The heartbeat is the safety net. If anything ever silently kills the
pipeline, the staleness alert fires within ~60 minutes regardless.

## Deploy

On the live host:

```
cd ~/projects/mvp
git pull
ops/live/install.sh
```

## First-time setup on a new host

1. Clone the repo to `~/projects/mvp` and run `poetry install`.
2. Run `ops/live/install.sh` — it will copy the `notify.env.example` template
   to `~/.config/mvp/notify.env` and exit 1.
3. Edit `~/.config/mvp/notify.env` and fill in the two webhook URLs. **No
   leading whitespace on the assignment lines** — `source` will silently
   treat indented lines as commands and the vars end up empty (this bit us
   on 2026-04-30).
4. Re-run `ops/live/install.sh`.

## Rotating webhooks

Edit `~/.config/mvp/notify.env` directly on the host. Don't put real webhook
URLs in this repo or in chat.
