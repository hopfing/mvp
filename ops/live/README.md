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
- `crontab` — the cron entries: two 15-min jobs (`mvp-live` — ATP fetch +
  predict + publish, run off-VPN via `mullvad-exclude`; and `mvp-books` — odds
  scraping, run on the VPN), plus daily log cleanup and the hourly heartbeat.
- `install.sh` — validates `notify.env` and replaces the user crontab with
  this repo's `crontab`. Idempotent.

## Egress split (books vs ATP)

The pipeline runs as **two** 15-min cron jobs with opposite egress needs:

- `mvp-live` (off-VPN, via `mullvad-exclude`) — ATP data fetch + predictions +
  Sheets sync. The Infosys match-centre endpoint 403s the Mullvad exit IP, so
  this must run on the bare ISP IP; atptour.com still clears Cloudflare via the
  browser solver on that IP.
- `mvp-books` (on the VPN) — sportsbook odds scraping, which must stay on the
  VPN (book anti-bot / account constraints).

Linux split tunnelling (Mullvad `mullvad-exclude`) is whole-process, so the two
cannot share a process. They hand off through the staged odds parquet on the
local filesystem: the books job touches a completion sentinel
(`pipeline/.books_done`) when done, and the main job waits (bounded, ~120s) for
a fresh sentinel before mapping/matching odds, flagging the run `books_stale`
if it times out. The ATP fetch normally outlasts the books job, so the wait is
usually a near-no-op.

Only `mvp-live` marks the heartbeat (`.last-success`); `mvp-books` runs with
`--no-heartbeat`, so a books outage can't keep the heartbeat fresh and mask a
main-job hard crash. A books outage surfaces via its own `mvp-books` Discord
alert (plus a sustained-0-entry alert after several consecutive empty runs).

## Where alerts come from

Two paths:

1. **Python (`src/mvp/notify.py`).** The pipeline itself posts:
   - `post_predictions` — when a sheets sync produces N>0 new bets.
   - `post_failure` — when a caught exception ends the main (`mvp-live`) or
     books (`mvp-books`) job non-zero, each on its own labelled alert.
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
