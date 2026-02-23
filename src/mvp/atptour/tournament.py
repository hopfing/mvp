"""Tournament metadata container."""

from dataclasses import dataclass

from mvp.common.enums import Circuit, TournamentType


@dataclass(frozen=True)
class Tournament:
    """Tournament metadata — eliminates parameter threading across pipeline modules."""

    tournament_id: str
    year: int
    circuit: Circuit
    location: str
    is_archive: bool = False
    surface: str | None = None
    indoor: str | None = None

    @property
    def path(self) -> str:
        """Storage path segment: tournaments/{circuit}/{tid}/{year}"""
        return f"tournaments/{self.circuit.value}/{self.tournament_id}/{self.year}"

    @property
    def logging_id(self) -> str:
        """Human-readable identifier for logging."""
        city = self.location.split(",")[0].strip()
        return f"{self.circuit.display_name} {city} {self.year} ({self.tournament_id})"

    @classmethod
    def from_overview_data(
        cls,
        data: dict,
        tournament_id: str,
        year: int,
        is_archive: bool = False,
    ) -> "Tournament":
        """Build from overview API response."""
        try:
            tournament_type = TournamentType(data["EventType"])
        except ValueError:
            raise ValueError(
                f"Unknown EventType '{data['EventType']}' for "
                f"tournament {tournament_id}. "
                f"Add member to TournamentType in enums.py."
            )
        return cls(
            tournament_id=tournament_id,
            year=year,
            circuit=tournament_type.circuit,
            location=data["Location"],
            is_archive=is_archive,
            surface=(data.get("Surface") or "").strip() or None,
            indoor=(data.get("InOutdoor") or "").strip() or None,
        )
