"""Tournament Results staged schema."""

from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

from mvp.atptour.mappings import map_player_id, normalize_round
from mvp.atptour.schema_helpers import (
    validate_doubles_partners,
    validate_match_uid_placeholders,
    validate_winner_in_players,
)
from mvp.common.enums import Circuit, DrawType, ResultType, Round
from mvp.common.schema_hash import compute_schema_hash

SCHEMA_VERSION = "1.0.0"


class ResultRecord(BaseModel):
    """A single match result from a Tournament Results HTML page."""

    tournament_id: str
    year: int = Field(ge=2010)
    circuit: Circuit
    draw_type: DrawType
    round: Round

    match_uid: str | None = None
    match_id: str | None = None

    winner_id: str

    p1_id: str
    p1_name: str
    p1_country: str
    p1_seed: int | None = None
    p1_entry: str | None = None

    p2_id: str
    p2_name: str
    p2_country: str
    p2_seed: int | None = None
    p2_entry: str | None = None

    p1_partner_id: str | None = None
    p1_partner_name: str | None = None
    p1_partner_country: str | None = None
    p2_partner_id: str | None = None
    p2_partner_name: str | None = None
    p2_partner_country: str | None = None

    result_type: ResultType
    duration_seconds: int | None = Field(default=None, ge=0)

    # Tiebreak values use max(7, loser_tb + 2) approximation — inaccurate
    # for non-7-point tiebreaks (e.g., 10-point super tiebreaks).
    p1_set1_games: int | None = Field(default=None, ge=0)
    p1_set1_tiebreak: int | None = Field(default=None, ge=0)
    p1_set2_games: int | None = Field(default=None, ge=0)
    p1_set2_tiebreak: int | None = Field(default=None, ge=0)
    p1_set3_games: int | None = Field(default=None, ge=0)
    p1_set3_tiebreak: int | None = Field(default=None, ge=0)
    p1_set4_games: int | None = Field(default=None, ge=0)
    p1_set4_tiebreak: int | None = Field(default=None, ge=0)
    p1_set5_games: int | None = Field(default=None, ge=0)
    p1_set5_tiebreak: int | None = Field(default=None, ge=0)

    p2_set1_games: int | None = Field(default=None, ge=0)
    p2_set1_tiebreak: int | None = Field(default=None, ge=0)
    p2_set2_games: int | None = Field(default=None, ge=0)
    p2_set2_tiebreak: int | None = Field(default=None, ge=0)
    p2_set3_games: int | None = Field(default=None, ge=0)
    p2_set3_tiebreak: int | None = Field(default=None, ge=0)
    p2_set4_games: int | None = Field(default=None, ge=0)
    p2_set4_tiebreak: int | None = Field(default=None, ge=0)
    p2_set5_games: int | None = Field(default=None, ge=0)
    p2_set5_tiebreak: int | None = Field(default=None, ge=0)

    source_file: str
    parsed_at: datetime

    _normalize_round = field_validator("round", mode="before")(normalize_round)
    _normalize_ids = field_validator("p1_id", "p2_id", "winner_id", mode="before")(
        map_player_id
    )

    @field_validator("p1_partner_id", "p2_partner_id", mode="before")
    @classmethod
    def _normalize_partner_ids(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return map_player_id(v)

    @model_validator(mode="after")
    def check_winner_id(self) -> "ResultRecord":
        validate_winner_in_players(self.winner_id, self.p1_id, self.p2_id)
        return self

    @model_validator(mode="after")
    def check_doubles_consistency(self) -> "ResultRecord":
        validate_doubles_partners(
            self.draw_type,
            [
                self.p1_partner_id,
                self.p1_partner_name,
                self.p1_partner_country,
                self.p2_partner_id,
                self.p2_partner_name,
                self.p2_partner_country,
            ],
        )
        return self

    @model_validator(mode="after")
    def check_match_uid_placeholders(self) -> "ResultRecord":
        all_ids = [self.p1_id, self.p2_id]
        if self.draw_type == DrawType.doubles:
            all_ids.extend([self.p1_partner_id, self.p2_partner_id])
        validate_match_uid_placeholders(self.match_uid, all_ids)
        return self

    @model_validator(mode="after")
    def check_walkover_consistency(self) -> "ResultRecord":
        game_fields = [
            self.p1_set1_games,
            self.p1_set2_games,
            self.p1_set3_games,
            self.p1_set4_games,
            self.p1_set5_games,
            self.p2_set1_games,
            self.p2_set2_games,
            self.p2_set3_games,
            self.p2_set4_games,
            self.p2_set5_games,
        ]
        all_null = all(f is None for f in game_fields)
        if self.result_type == ResultType.walkover and not all_null:
            raise ValueError("All set game fields must be null for walkovers")
        if all_null and self.result_type != ResultType.walkover:
            raise ValueError(
                "result_type must be walkover when all set game fields are null"
            )
        return self

    @model_validator(mode="after")
    def check_set_contiguity(self) -> "ResultRecord":
        p1_games = [
            self.p1_set1_games,
            self.p1_set2_games,
            self.p1_set3_games,
            self.p1_set4_games,
            self.p1_set5_games,
        ]
        seen_none = False
        for g in p1_games:
            if g is None:
                seen_none = True
            elif seen_none:
                raise ValueError("Set scores must be contiguous (no gaps)")
        return self


SCHEMA_HASH = compute_schema_hash(ResultRecord)
