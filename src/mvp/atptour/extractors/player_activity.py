"""PlayerActivityExtractor — fetch missing player activity JSON from ATP API."""

import logging
import time
from pathlib import Path

from mvp.atptour.pipeline_utils import activity_covers_tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

ACTIVITY_URL = "https://www.atptour.com/en/-/www/activity/sgl/{pid}/?v=1"

ACTIVITY_MAX_AGE_SECONDS = 24 * 60 * 60


def _is_fresh(path: Path, max_age: float = ACTIVITY_MAX_AGE_SECONDS) -> bool:
    """Return True if the file was modified within max_age seconds."""
    return (time.time() - path.stat().st_mtime) < max_age


class PlayerActivityExtractor(BaseExtractor):
    """Fetch player activity JSON for players missing or incomplete locally."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(
        self, player_tournaments: dict[str, set[tuple[str, int]]]
    ) -> list[tuple[str, str]]:
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
            error = self._fetch_player(pid)
            if error is not None:
                failed.append((pid, error))
        return failed

    def _fetch_player(self, pid: str) -> str | None:
        url = ACTIVITY_URL.format(pid=pid)
        try:
            data = self.fetch_json(url)
        except Exception as e:
            logger.warning("Failed to fetch activity for %s: %s", pid, e)
            return str(e)
        if data is None or data.get("Activity") is None:
            logger.warning("Empty activity response for %s", pid)
            return None
        target = self.build_path("raw", "activity", f"{pid}.json")
        self.save_json(data, target)
        return None
