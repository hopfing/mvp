"""Schema for MatchBeats point-level data."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator


class PointResult(StrEnum):
    """Point outcome type."""

    ACE = "A"
    WINNER = "W"
    UNFORCED_ERROR = "UE"
    FORCED_ERROR = "FE"
    DOUBLE_FAULT = "DF"
    UNKNOWN = "N"


class MatchBeatsPointRecord(BaseModel):
    """A single point from a MatchBeats match.

    This is the staged grain - one row per point with denormalized
    match/set/game context for efficient querying.
    """

    # Match context
    tournament_id: str
    year: int
    match_id: str
    is_doubles: bool

    # Player IDs (from playerData)
    p1_id: str  # Team 1 player 1
    p2_id: str  # Team 2 player 1

    # Set context
    set_num: int
    set_winner: str | None = None  # "1" or "2"

    # Game context
    game_num: int
    game_duration: int | None = None  # seconds
    easy_hold: bool | None = None
    difficult_hold: bool | None = None
    multiple_deuces: bool | None = None
    game_winner: str | None = None  # "1" or "2"
    is_tiebreak: bool = False

    # Point identification
    point_num: int
    point_id: str  # e.g., "1_1_1_1" (set_game_point_serve)

    # Point outcome
    result: PointResult
    scorer: str  # "1" or "2"
    server: str  # "1" or "2"

    # Serve data
    serve: int = Field(ge=1, le=2)  # 1st or 2nd serve
    serve_speed: float | None = None  # km/h, None if not tracked
    fault_serve_speed: float | None = None  # 1st serve fault speed

    # Rally data
    p1_rally_shots: int = 0
    p2_rally_shots: int = 0
    rally_length_missing: bool = False

    # Break point context
    is_break_point: bool = False
    break_points_in_game: int = 0
    break_points_lost: int = 0

    # Situation flags
    is_crucial_point: bool = False

    # Score at point
    p1_game_score: str = "0"
    p2_game_score: str = "0"

    # Match duration at point (seconds)
    match_duration_at_point: int = 0

    # Traceability
    source_file: str
    parsed_at: datetime

    @field_validator("match_id", mode="before")
    @classmethod
    def _uppercase_match_id(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("serve_speed", "fault_serve_speed", mode="before")
    @classmethod
    def zero_to_none(cls, v: float | None) -> float | None:
        """Convert 0.0 serve speed to None (not tracked)."""
        if v == 0.0:
            return None
        return v

    @field_validator("result", mode="before")
    @classmethod
    def normalize_result(cls, v: str) -> str:
        """Normalize result to enum value."""
        return v.upper().strip() if isinstance(v, str) else v


# Schema hash for tracking schema changes
SCHEMA_HASH = "match_beats_v1_2026_03_02"
