"""PlayerActivityExtractor — fetch player activity JSON from the ATP API.

Activity is the ONLY source of ITF match results, and ITF feeds the rating replay every
challenger prediction depends on. What triggers a refetch therefore decides how current
those ratings are at the moment a bet is placed.

**The trigger is a new (player, match) pair, not elapsed time.** A fetch made when ATP
had not yet published a player's ITF result caches a "no", and a clock gives no reason
to re-ask however long it waits. A new match appearing does — and because `match_uid` is
null while either side is a placeholder, a pair mints exactly when a prediction on that
match first becomes possible.

`activity_covers_tournament` is retained for the backfill path only. It is a membership
test for one tournament that returns True on the first hit, so it cannot detect that a
file is missing a player's other matches — which is the whole of ITF recency.
"""

import logging
import time
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.atptour import activity_ledger
from mvp.atptour.pipeline_utils import activity_covers_tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

ACTIVITY_URL = "https://www.atptour.com/en/-/www/activity/sgl/{pid}/?v=1"

ACTIVITY_MAX_AGE_SECONDS = 24 * 60 * 60

# Fetches per run. Steady state is 1-4 players a tick, so this only binds on a cold
# start or after ledger loss, where the open set is the whole scheduled population
# (~800 mid-week) and would overrun the 15-minute tick outright against a host that has
# already flagged this machine. Draining over several ticks, soonest match first, is
# strictly better than one burst.
MAX_FETCHES_PER_RUN = 40

# ...and a wall-clock ceiling, because the cap counts requests and a request is not a
# fixed cost. `BaseExtractor._fetch` sleeps ~1s then retries 3x at a 30s timeout, so a
# degraded ATP -- the exact failure mode this host has seen -- can take ~2 min per
# player. 40 of those would run past the 15-minute tick, and nothing serialises ticks.
MAX_SECONDS_PER_RUN = 240


def _is_fresh(path: Path, max_age: float = ACTIVITY_MAX_AGE_SECONDS) -> bool:
    """Return True if the file was modified within max_age seconds."""
    return (time.time() - path.stat().st_mtime) < max_age


class PlayerActivityExtractor(BaseExtractor):
    """Fetch player activity JSON for players missing or incomplete locally."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def run_for_pairs(
        self,
        pairs: pl.DataFrame,
        *,
        max_fetches: int = MAX_FETCHES_PER_RUN,
        max_seconds: float = MAX_SECONDS_PER_RUN,
    ) -> tuple[list[tuple[str, str]], set[str]]:
        """Fetch activity for players holding an unfetched (player, match) pair.

        Returns `(failures, fetched_ok)`. The second is what the caller must stage —
        staging the whole scheduled population instead would walk hundreds of
        untouched players and then trigger a full activity rebuild on every tick.

        `pairs` comes from `get_scheduled_pairs`. One HTTP call serves every open pair
        a player has, so the cap counts players rather than pairs. Ordered by soonest
        scheduled match, so a capped run serves the matches closest to needing a
        prediction.
        """
        root = self.data_root
        if pairs.is_empty():
            logger.info("Activity: no scheduled pairs")
            return [], set()

        closed = activity_ledger.closed_pairs(root)
        open_pairs = pairs.filter(
            ~pl.struct("player_id", "match_uid").map_elements(
                lambda s: (s["player_id"], s["match_uid"]) in closed,
                return_dtype=pl.Boolean,
            )
        )
        if open_pairs.is_empty():
            logger.info("Activity: %d pairs scheduled, all already fetched",
                        pairs.height)
            return [], set()

        # Rank by the soonest match still to be PLAYED, not the soonest open pair.
        # A player's open pairs include ones already played -- a beaten player keeps
        # theirs forever -- so ranking on the earliest pair puts last week's matches
        # ahead of tonight's. Measured before this fix: 27 of the first 40 selected
        # had no upcoming match at all, while players due within 24h sat at median
        # queue position 316. Past-only players still drain, behind everyone live.
        now = datetime.now()
        by_player = (
            open_pairs.group_by("player_id")
            .agg(
                pl.col("match_uid"),
                pl.col("scheduled_datetime")
                .filter(pl.col("scheduled_datetime") >= now)
                .min()
                .alias("next_match"),
                pl.col("scheduled_datetime").max().alias("last_match"),
            )
            .sort(["next_match", "last_match"],
                  descending=[False, True], nulls_last=True)
        )
        total = by_player.height
        due = by_player.head(max_fetches)
        logger.info(
            "Activity trigger: %d open pairs across %d players, fetching %d "
            "(cap %d, soonest match first)",
            open_pairs.height, total, due.height, max_fetches,
        )

        failed: list[tuple[str, str]] = []
        fetched_ok: set[str] = set()
        deadline = time.monotonic() + max_seconds
        done = 0
        for row in due.iter_rows(named=True):
            if time.monotonic() >= deadline:
                logger.warning(
                    "Activity: %.0fs budget spent after %d/%d fetches, deferring rest",
                    max_seconds, done, due.height,
                )
                break
            pid, uids = row["player_id"], row["match_uid"]
            outcome, detail = self._fetch_player(pid)
            activity_ledger.record(root, pid, uids, outcome, detail)
            done += 1
            if outcome == activity_ledger.OK:
                fetched_ok.add(pid)
            else:
                failed.append((pid, detail or outcome))
        deferred = total - done
        if deferred > 0:
            logger.info("Activity: %d players deferred to a later run", deferred)
        return failed, fetched_ok

    def run(
        self, player_tournaments: dict[str, set[tuple[str, int]]]
    ) -> list[tuple[str, str]]:
        """Coverage-driven fetch. Backfill path only — see the module docstring.

        Backfill asks a different question: it needs a file covering a specific
        historical tournament, not one that is current.
        """
        raw_dir = self.build_path("raw", "activity")
        existing = {p.stem: p for p in self.list_files(raw_dir, "*.json")}

        skipped_fresh = 0
        to_fetch = []
        for pid, tournaments in player_tournaments.items():
            path = existing.get(pid)
            if path is None:
                to_fetch.append(pid)
                continue
            data = self.read_json(path)
            for tid, year in tournaments:
                if not activity_covers_tournament(data, year, tid):
                    if _is_fresh(path):
                        skipped_fresh += 1
                    else:
                        to_fetch.append(pid)
                    break

        logger.info(
            "Player activity: %d players, %d existing, %d to fetch, %d skipped (fresh)",
            len(player_tournaments),
            len(existing),
            len(to_fetch),
            skipped_fresh,
        )

        to_fetch.sort()
        failed: list[tuple[str, str]] = []
        for pid in to_fetch:
            outcome, detail = self._fetch_player(pid)
            if outcome != activity_ledger.OK:
                failed.append((pid, detail or outcome))
        return failed

    def _fetch_player(self, pid: str) -> tuple[str, str | None]:
        """Fetch one player. Returns (outcome, detail).

        An empty `Activity` payload reports EMPTY rather than success. It previously
        returned None — indistinguishable from a successful fetch — so a player who
        consistently returns nothing looked settled and was never asked again, banking
        a permanent hole in exactly the data this exists to keep current.
        """
        url = ACTIVITY_URL.format(pid=pid)
        try:
            data = self.fetch_json(url)
        except Exception as e:
            logger.warning("Failed to fetch activity for %s: %s", pid, e)
            return activity_ledger.ERROR, str(e)
        if data is None or data.get("Activity") is None:
            logger.warning("Empty activity response for %s", pid)
            return activity_ledger.EMPTY, "empty Activity payload"
        target = self.build_path("raw", "activity", f"{pid}.json")
        self.save_json(data, target)
        return activity_ledger.OK, None
