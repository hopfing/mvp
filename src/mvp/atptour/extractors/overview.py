"""OverviewExtractor — fetch tournament overview JSON and build Tournament object."""

import logging

from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor
from mvp.common.enums import Circuit

logger = logging.getLogger(__name__)


class OverviewExtractor(BaseExtractor):
    """Fetch overview JSON from ATP API, build Tournament, save raw JSON."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(
        self,
        tournament_id: str,
        year: int,
        is_archive: bool = False,
        refresh: bool = False,
        circuit: Circuit | None = None,
    ) -> Tournament:
        """Fetch or read cached overview data and return a Tournament.

        For archive tournaments (is_archive=True), checks for a cached
        overview.json on disk first (unless refresh=True). Active tournaments
        always fetch from the API.

        If the API returns null/empty data and a circuit hint is provided,
        returns a fallback Tournament with location "Unknown". Without a hint,
        raises ValueError.
        """
        if is_archive and not refresh:
            for circuit_val in ("tour", "chal"):
                cached = self.build_path(
                    "raw",
                    f"tournaments/{circuit_val}/{tournament_id}/{year}",
                    "overview.json",
                )
                if cached.exists():
                    data = self.read_json(cached)
                    return Tournament.from_overview_data(
                        data=data,
                        tournament_id=tournament_id,
                        year=year,
                        is_archive=is_archive,
                    )

        url = f"https://www.atptour.com/en/-/tournaments/profile/{tournament_id}/overview"
        data = self.fetch_json(url)

        if data is None or data.get("EventType") is None:
            if circuit is None:
                raise ValueError(
                    f"Overview API returned null for tournament {tournament_id} ({year}) "
                    f"and no circuit hint available."
                )
            return Tournament(
                tournament_id=tournament_id,
                year=year,
                circuit=circuit,
                location="Unknown",
                is_archive=is_archive,
            )

        tournament = Tournament.from_overview_data(
            data=data,
            tournament_id=tournament_id,
            year=year,
            is_archive=is_archive,
        )

        path = self.build_path("raw", tournament.path, "overview.json")
        self.save_json(data, path)

        return tournament
