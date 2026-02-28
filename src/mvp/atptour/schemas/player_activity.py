"""Player activity staged schema."""

from datetime import date, datetime, timedelta
from typing import ClassVar

from pydantic import BaseModel, computed_field, field_validator, model_validator

from mvp.atptour.mappings import create_match_uid, map_player_id, normalize_round
from mvp.atptour.schema_helpers import empty_to_none, parse_indoor
from mvp.common.enums import ActivityEventType, Circuit, Round
from mvp.common.schema_hash import compute_schema_hash

SCHEMA_VERSION = "1.0.1"


def _parse_tournament_date(v) -> date | None:
    """Parse tournament date string to date object.

    Accepts "2023-01-16T00:00:00" format, date objects, or None.
    """
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return datetime.fromisoformat(v).date()
    raise ValueError(f"Cannot parse tournament date: {v!r}")


class PlayerActivityRecord(BaseModel):
    """A single match from a player's activity/results history."""

    SCHEMA_VERSION: ClassVar[str] = SCHEMA_VERSION

    # Context
    player_id: str
    year: int
    tournament_id: str
    event_type: ActivityEventType
    surface: str | None = None
    indoor: bool | None = None
    tournament_start_date: date | None = None
    tournament_end_date: date | None = None
    points: int
    prize_usd: int
    match_id: str
    round: Round
    win_loss: str | None = None
    reason: str | None = None
    player_rank: int | None = None

    # Opponent
    opp_id: str | None = None
    opp_first_name: str | None = None
    opp_last_name: str | None = None
    opp_natl_id: str | None = None
    opp_rank: int | None = None

    # Set scores (5 sets * 4 fields = 20)
    player_set1_games: int | None = None
    opp_set1_games: int | None = None
    player_set1_tiebreak: int | None = None
    opp_set1_tiebreak: int | None = None
    player_set2_games: int | None = None
    opp_set2_games: int | None = None
    player_set2_tiebreak: int | None = None
    opp_set2_tiebreak: int | None = None
    player_set3_games: int | None = None
    opp_set3_games: int | None = None
    player_set3_tiebreak: int | None = None
    opp_set3_tiebreak: int | None = None
    player_set4_games: int | None = None
    opp_set4_games: int | None = None
    player_set4_tiebreak: int | None = None
    opp_set4_tiebreak: int | None = None
    player_set5_games: int | None = None
    opp_set5_games: int | None = None
    player_set5_tiebreak: int | None = None
    opp_set5_tiebreak: int | None = None

    # Flags
    has_stats: bool
    match_stats_url: str | None = None
    is_bye: bool

    # Traceability
    source_file: str
    parsed_at: datetime

    _normalize_round = field_validator("round", mode="before")(normalize_round)
    _normalize_surface = field_validator("surface", mode="before")(empty_to_none)
    _normalize_indoor = field_validator("indoor", mode="before")(parse_indoor)
    _normalize_win_loss = field_validator("win_loss", mode="before")(empty_to_none)
    _empty_to_none_fields = field_validator(
        "reason", "match_stats_url", "opp_first_name", "opp_last_name",
        mode="before",
    )(empty_to_none)

    @field_validator("player_id", mode="before")
    @classmethod
    def _map_player_id(cls, v: str) -> str:
        return map_player_id(v)

    @field_validator("opp_id", mode="before")
    @classmethod
    def _normalize_opp_id(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return map_player_id(v)

    @field_validator("opp_natl_id", mode="before")
    @classmethod
    def _uppercase_opp_natl_id(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return v.upper()

    @field_validator("tournament_start_date", "tournament_end_date", mode="before")
    @classmethod
    def _parse_tournament_dates(cls, v) -> date | None:
        return _parse_tournament_date(v)

    @field_validator("win_loss", mode="after")
    @classmethod
    def _validate_win_loss(cls, v):
        if v is not None and v not in ("W", "L"):
            raise ValueError(f"win_loss must be 'W', 'L', or None, got '{v}'")
        return v

    @model_validator(mode="after")
    def _fix_invalid_end_date(self):
        """Fix obviously invalid tournament_end_date (year < 1900).

        Estimates end_date as start_date + 6 days if start_date is valid.
        """
        if self.tournament_end_date and self.tournament_end_date.year < 1900:
            if self.tournament_start_date:
                self.tournament_end_date = self.tournament_start_date + timedelta(days=6)
            else:
                self.tournament_end_date = None
        return self

    @computed_field
    @property
    def circuit(self) -> Circuit:
        return self.event_type.circuit

    @computed_field
    @property
    def match_uid(self) -> str | None:
        if self.is_bye or not self.opp_id:
            return None
        return create_match_uid(
            self.year,
            self.tournament_id,
            self.round,
            [self.player_id, self.opp_id],
            is_doubles=False,
        )


SCHEMA_HASH = compute_schema_hash(PlayerActivityRecord)
PlayerActivityRecord.SCHEMA_HASH = SCHEMA_HASH
