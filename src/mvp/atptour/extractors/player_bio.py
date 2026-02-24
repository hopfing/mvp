"""PlayerBioExtractor — fetch missing player bio JSON from ATP API."""

import logging

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

HERO_URL = "https://www.atptour.com/en/-/www/players/hero/{pid}?v=1"


class PlayerBioExtractor(BaseExtractor):
    """Fetch player bio JSON for players not yet saved locally."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self, player_ids: list[str]) -> list[tuple[str, str]]:
        """Fetch bio JSON for players missing from raw storage.

        Args:
            player_ids: List of player IDs to ensure are fetched.

        Returns:
            List of (player_id, error_message) tuples for failed fetches.
        """
        raw_dir = self.build_path("raw", "players")
        existing = {p.stem for p in self.list_files(raw_dir, "*.json")}
        normalized = [pid.upper() for pid in player_ids]
        new_ids = [pid for pid in normalized if pid not in existing]
        to_fetch = sorted(new_ids)

        logger.info(
            "Player bios: %d players, %d existing, %d to fetch",
            len(player_ids),
            len(existing),
            len(to_fetch),
        )

        failed: list[tuple[str, str]] = []
        for pid in to_fetch:
            url = HERO_URL.format(pid=pid)
            try:
                data = self.fetch_json(url)
            except Exception as e:
                logger.warning("Failed to fetch bio for %s: %s", pid, e)
                failed.append((pid, str(e)))
                continue

            if data is None:
                logger.warning("Empty bio response for %s", pid)
                continue

            target = self.build_path("raw", "players", f"{pid}.json")
            self.save_json(data, target)

        return failed
