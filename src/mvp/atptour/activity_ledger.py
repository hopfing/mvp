"""Record of which (player, match) pairs have had their activity fetched.

Activity JSON is the only source of ITF results, and ITF feeds the rating replay that
every challenger prediction depends on. The question this answers is "have we asked
about this player since this match became predictable" — which a clock cannot answer,
because a fetch made before ATP published caches a "no" that elapsed time gives no
reason to re-ask.

**A pair mints exactly when a prediction first becomes possible.** `match_uid` is null
while either side is a placeholder, and a prediction needs two resolved players. So the
trigger and the thing it protects are gated on the same condition.

**Append-only JSONL, one line per pair per attempt.** The access pattern is write-once
per event and read-once per run, which is what a line-oriented log is for; `runs.jsonl`
and `alerts.jsonl` in the same directory are the precedent. An append is a single
syscall, so a crash mid-run loses at most the line being written rather than
invalidating the file — and nothing rewrites what is already there, so cost does not
grow with the log.

Outcome is recorded, not just the attempt. `_fetch_player` has three results and only
two are distinguishable without this: an empty `Activity` payload returned None exactly
as success did, so a silently-empty player would settle its pairs and never be asked
again, banking a permanent hole in the data this exists to keep current.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

LEDGER_NAME = "activity_fetch_log.jsonl"

OK = "ok"
EMPTY = "empty"
ERROR = "error"

# Outcomes meaning "asked and answered". An error or an empty payload leaves the pair
# open so the next run retries it — a 403 must not burn a pair's only fetch.
SETTLED = frozenset({OK})

# ...but not forever. A player whose Activity payload is always empty would otherwise
# keep an unsettleable pair at the head of the queue on every run, re-requesting a host
# that has IP-flagged this machine, and blocking every player behind it. After this
# many attempts the pair is treated as exhausted and stops being retried.
MAX_ATTEMPTS = 3


def ledger_path(data_root: Path) -> Path:
    return data_root / "pipeline" / LEDGER_NAME


def iter_records(data_root: Path):
    """Yield each recorded attempt. Unparseable lines are skipped, not raised.

    A torn final line is the expected residue of a crash mid-append; refusing to read
    the whole log because of it would turn a recoverable state into an outage.
    """
    path = ledger_path(data_root)
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed ledger line %d in %s", n, path)


def closed_pairs(
    data_root: Path, max_attempts: int = MAX_ATTEMPTS
) -> set[tuple[str, str]]:
    """Pairs that should no longer be fetched: succeeded, or tried too many times.

    Exhaustion is not success. It exists only so an unsettleable pair cannot hold the
    head of the queue indefinitely; the ledger keeps every attempt, so which pairs gave
    up and why stays inspectable.
    """
    attempts: dict[tuple[str, str], int] = {}
    done: set[tuple[str, str]] = set()
    for r in iter_records(data_root):
        key = (r["player_id"], r["match_uid"])
        attempts[key] = attempts.get(key, 0) + 1
        if r.get("outcome") in SETTLED:
            done.add(key)
    return done | {k for k, n in attempts.items() if n >= max_attempts}


def settled_pairs(data_root: Path) -> set[tuple[str, str]]:
    """(player_id, match_uid) pairs already fetched successfully."""
    return {
        (r["player_id"], r["match_uid"])
        for r in iter_records(data_root)
        if r.get("outcome") in SETTLED
    }


def record(
    data_root: Path,
    player_id: str,
    match_uids: list[str],
    outcome: str,
    detail: str | None = None,
) -> None:
    """Append one line per pair, immediately.

    Written per fetch rather than batched at end of stage: a mid-tick crash must not
    cause the next run to re-fetch everything already done. One HTTP call covers every
    open pair a player has, hence a list of uids against a single request.
    """
    if not match_uids:
        return
    now = datetime.now(UTC).isoformat()
    path = ledger_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = "".join(
        json.dumps({
            "player_id": player_id,
            "match_uid": uid,
            "requested_at": now,
            "outcome": outcome,
            "detail": detail,
        }) + "\n"
        for uid in match_uids
    )
    with path.open("a", encoding="utf-8") as fh:
        fh.write(lines)
