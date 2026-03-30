"""Schema for stroke_analysis match-level data."""

from datetime import datetime

from pydantic import BaseModel, field_validator

SHOT_TYPES = [
    "ground_stroke",
    "overhead",
    "passing",
    "volley",
    "approach",
    "drop_shot",
    "lob",
]

# Maps JSON name to our column prefix
SHOT_TYPE_MAP = {
    "Ground Stroke": "ground_stroke",
    "Overhead Shots": "overhead",
    "Passing Shots": "passing",
    "Volley Shots": "volley",
    "Approach Shots": "approach",
    "Drop Shots": "drop_shot",
    "Lob Shots": "lob",
}


class StrokeAnalysisRecord(BaseModel):
    """Match-level stroke analysis — one row per match.

    Combines two views:
    - totalPointsCount[0]: FH/BH split (includes serve/return outcomes)
    - allPoints[0] summed across sets: shot-type split, FH+BH combined (rally shots only)
    """

    # Match context
    tournament_id: str
    year: int
    match_id: str
    is_doubles: bool
    p1_id: str
    p2_id: str

    # From totalPointsCount[0] — FH/BH match totals (includes serve/return)
    p1_fh_winners: int = 0
    p1_fh_forced_errors: int = 0
    p1_fh_unforced_errors: int = 0
    p1_bh_winners: int = 0
    p1_bh_forced_errors: int = 0
    p1_bh_unforced_errors: int = 0
    p2_fh_winners: int = 0
    p2_fh_forced_errors: int = 0
    p2_fh_unforced_errors: int = 0
    p2_bh_winners: int = 0
    p2_bh_forced_errors: int = 0
    p2_bh_unforced_errors: int = 0

    # From allPoints[0] — per shot type, FH+BH combined, summed across sets
    # Ground Stroke
    p1_ground_stroke_winners: int = 0
    p1_ground_stroke_forced_errors: int = 0
    p1_ground_stroke_unforced_errors: int = 0
    p1_ground_stroke_others: int = 0
    p2_ground_stroke_winners: int = 0
    p2_ground_stroke_forced_errors: int = 0
    p2_ground_stroke_unforced_errors: int = 0
    p2_ground_stroke_others: int = 0
    # Overhead
    p1_overhead_winners: int = 0
    p1_overhead_forced_errors: int = 0
    p1_overhead_unforced_errors: int = 0
    p1_overhead_others: int = 0
    p2_overhead_winners: int = 0
    p2_overhead_forced_errors: int = 0
    p2_overhead_unforced_errors: int = 0
    p2_overhead_others: int = 0
    # Passing
    p1_passing_winners: int = 0
    p1_passing_forced_errors: int = 0
    p1_passing_unforced_errors: int = 0
    p1_passing_others: int = 0
    p2_passing_winners: int = 0
    p2_passing_forced_errors: int = 0
    p2_passing_unforced_errors: int = 0
    p2_passing_others: int = 0
    # Volley
    p1_volley_winners: int = 0
    p1_volley_forced_errors: int = 0
    p1_volley_unforced_errors: int = 0
    p1_volley_others: int = 0
    p2_volley_winners: int = 0
    p2_volley_forced_errors: int = 0
    p2_volley_unforced_errors: int = 0
    p2_volley_others: int = 0
    # Approach
    p1_approach_winners: int = 0
    p1_approach_forced_errors: int = 0
    p1_approach_unforced_errors: int = 0
    p1_approach_others: int = 0
    p2_approach_winners: int = 0
    p2_approach_forced_errors: int = 0
    p2_approach_unforced_errors: int = 0
    p2_approach_others: int = 0
    # Drop Shot
    p1_drop_shot_winners: int = 0
    p1_drop_shot_forced_errors: int = 0
    p1_drop_shot_unforced_errors: int = 0
    p1_drop_shot_others: int = 0
    p2_drop_shot_winners: int = 0
    p2_drop_shot_forced_errors: int = 0
    p2_drop_shot_unforced_errors: int = 0
    p2_drop_shot_others: int = 0
    # Lob
    p1_lob_winners: int = 0
    p1_lob_forced_errors: int = 0
    p1_lob_unforced_errors: int = 0
    p1_lob_others: int = 0
    p2_lob_winners: int = 0
    p2_lob_forced_errors: int = 0
    p2_lob_unforced_errors: int = 0
    p2_lob_others: int = 0

    @field_validator("match_id", mode="before")
    @classmethod
    def _uppercase_match_id(cls, v: str) -> str:
        return v.upper().strip()

    # Traceability
    source_file: str
    parsed_at: datetime


SCHEMA_HASH = "stroke_analysis_v1_2026_03_03"
