"""Match Stats staged schema."""

from datetime import date, datetime
from typing import ClassVar

from pydantic import BaseModel, Field, computed_field, field_validator, model_validator

from mvp.atptour.mappings import (
    create_match_uid,
    is_placeholder_id,
    map_player_id,
    normalize_round,
)
from mvp.atptour.schema_helpers import (
    empty_to_none,
    strip_or_none,
    validate_doubles_partners,
    validate_winner_in_players,
)
from mvp.common.enums import Circuit, DrawType, Round
from mvp.common.schema_hash import compute_schema_hash

SCHEMA_VERSION = "1.0.0"

_VALID_REASONS = {"RET", "DEF", "W/O", "UNP"}


class MatchStatsRecord(BaseModel):
    """A single match stats record from an ATP match_stats JSON file."""

    SCHEMA_VERSION: ClassVar[str] = SCHEMA_VERSION

    # Context
    tournament_id: str
    year: int
    circuit: Circuit
    draw_type: DrawType
    round: Round
    round_id: int | None = None
    match_id: str

    # Tournament metadata
    surface: str | None = None
    tournament_start_date: date | None = None
    tournament_end_date: date | None = None
    tournament_city: str | None = None
    prize_money: int | None = None
    currency: str | None = None
    draw_size_singles: int | None = None
    draw_size_doubles: int | None = None

    # Match metadata
    winner_id: str | None = None
    duration_seconds: int | None = Field(default=None, ge=0)
    reason: str | None = None
    number_of_sets: int
    sets_played: int
    is_qualifier: bool | None = None
    scoring_system: str | None = None
    court_name: str | None = None
    umpire_first_name: str | None = None
    umpire_last_name: str | None = None

    # Players
    p1_id: str
    p2_id: str
    p1_partner_id: str | None = None
    p2_partner_id: str | None = None
    p1_seed: int | None = None
    p1_entry: str | None = None
    p2_seed: int | None = None
    p2_entry: str | None = None

    # Stats — P1 service
    p1_svc_aces: int
    p1_svc_double_faults: int
    p1_svc_first_serve_in: int
    p1_svc_first_serve_att: int
    p1_svc_first_serve_pts_won: int
    p1_svc_first_serve_pts_played: int
    p1_svc_second_serve_pts_won: int
    p1_svc_second_serve_pts_played: int
    p1_svc_bp_saved: int
    p1_svc_bp_faced: int
    p1_svc_games_played: int
    p1_svc_serve_rating: int

    # Stats — P1 return
    p1_ret_first_serve_pts_won: int
    p1_ret_first_serve_pts_played: int
    p1_ret_second_serve_pts_won: int
    p1_ret_second_serve_pts_played: int
    p1_ret_bp_converted: int
    p1_ret_bp_opportunities: int
    p1_ret_games_played: int
    p1_ret_return_rating: int

    # Stats — P1 points
    p1_pts_service_pts_won: int
    p1_pts_service_pts_played: int
    p1_pts_return_pts_won: int
    p1_pts_return_pts_played: int
    p1_pts_total_pts_won: int
    p1_pts_total_pts_played: int

    # Stats — P2 service
    p2_svc_aces: int
    p2_svc_double_faults: int
    p2_svc_first_serve_in: int
    p2_svc_first_serve_att: int
    p2_svc_first_serve_pts_won: int
    p2_svc_first_serve_pts_played: int
    p2_svc_second_serve_pts_won: int
    p2_svc_second_serve_pts_played: int
    p2_svc_bp_saved: int
    p2_svc_bp_faced: int
    p2_svc_games_played: int
    p2_svc_serve_rating: int

    # Stats — P2 return
    p2_ret_first_serve_pts_won: int
    p2_ret_first_serve_pts_played: int
    p2_ret_second_serve_pts_won: int
    p2_ret_second_serve_pts_played: int
    p2_ret_bp_converted: int
    p2_ret_bp_opportunities: int
    p2_ret_games_played: int
    p2_ret_return_rating: int

    # Stats — P2 points
    p2_pts_service_pts_won: int
    p2_pts_service_pts_played: int
    p2_pts_return_pts_won: int
    p2_pts_return_pts_played: int
    p2_pts_total_pts_won: int
    p2_pts_total_pts_played: int

    # Traceability
    source_file: str
    parsed_at: datetime

    # Field validators
    _normalize_round = field_validator("round", mode="before")(normalize_round)
    _normalize_ids = field_validator("p1_id", "p2_id", mode="before")(map_player_id)

    @field_validator("winner_id", "p1_partner_id", "p2_partner_id", mode="before")
    @classmethod
    def _normalize_nullable_ids(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return map_player_id(v)

    @field_validator("reason", mode="before")
    @classmethod
    def _validate_reason(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if v not in _VALID_REASONS:
            raise ValueError(
                f"reason must be one of {_VALID_REASONS}, got '{v}'"
            )
        return v

    _empty_to_none = field_validator(
        "court_name", "scoring_system", "currency",
        "umpire_first_name", "umpire_last_name",
        mode="before",
    )(empty_to_none)

    _strip_city = field_validator("tournament_city", mode="before")(strip_or_none)

    # Model validators
    @model_validator(mode="after")
    def check_winner_id(self) -> "MatchStatsRecord":
        if self.winner_id is not None:
            validate_winner_in_players(self.winner_id, self.p1_id, self.p2_id)
        return self

    @model_validator(mode="after")
    def check_doubles_consistency(self) -> "MatchStatsRecord":
        validate_doubles_partners(
            self.draw_type,
            [self.p1_partner_id, self.p2_partner_id],
        )
        return self

    @computed_field
    @property
    def match_uid(self) -> str | None:
        all_ids = [self.p1_id, self.p2_id]
        is_doubles = self.draw_type == DrawType.doubles
        if is_doubles:
            all_ids.extend([self.p1_partner_id, self.p2_partner_id])
        if any(is_placeholder_id(pid) for pid in all_ids if pid is not None):
            return None
        return create_match_uid(
            self.year, self.tournament_id, self.round, all_ids, is_doubles,
        )


SCHEMA_HASH = compute_schema_hash(MatchStatsRecord)
MatchStatsRecord.SCHEMA_HASH = SCHEMA_HASH
