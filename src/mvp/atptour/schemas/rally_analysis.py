"""Schema for rally_analysis match-level data."""

from datetime import datetime

from pydantic import BaseModel, field_validator

RALLY_LENGTHS = ["short", "medium", "long"]

# Maps rally_analysis category index to rally length bucket
# Short (1-4 shots): Serve, Return, 3rd shot, 4th shot
# Medium (5-8 shots): 5th shot, 6th shot, 7th shot, 8th shot
# Long (9+ shots): 9+ odd shots, 10+ even shots
CATEGORY_TO_LENGTH = {
    0: "short",
    1: "short",
    2: "short",
    3: "short",
    4: "medium",
    5: "medium",
    6: "medium",
    7: "medium",
    8: "long",
    9: "long",
}


class RallyAnalysisRecord(BaseModel):
    """Match-level rally analysis — one row per match.

    Aggregates point-level data into rally length buckets:
    short (1-4 shots), medium (5-8 shots), long (9+ shots).
    """

    # Match context
    tournament_id: str
    year: int
    match_id: str
    is_doubles: bool
    p1_id: str
    p2_id: str

    # Short rallies (1-4 shots)
    p1_short_won: int = 0
    p1_short_err: int = 0
    p2_short_won: int = 0
    p2_short_err: int = 0

    # Medium rallies (5-8 shots)
    p1_medium_won: int = 0
    p1_medium_err: int = 0
    p2_medium_won: int = 0
    p2_medium_err: int = 0

    # Long rallies (9+ shots)
    p1_long_won: int = 0
    p1_long_err: int = 0
    p2_long_won: int = 0
    p2_long_err: int = 0

    # Unclassified points
    p1_unclassified_won: int = 0
    p1_unclassified_err: int = 0
    p2_unclassified_won: int = 0
    p2_unclassified_err: int = 0

    @field_validator("match_id", mode="before")
    @classmethod
    def _uppercase_match_id(cls, v: str) -> str:
        return v.upper().strip()

    # Data quality
    points_missing: bool = False

    # Traceability
    source_file: str
    parsed_at: datetime


SCHEMA_HASH = "rally_analysis_v1_2026_03_03"
