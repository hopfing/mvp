"""Schema for stats_plus match-level data.

One row per match, built from the feed's own match-total slot (``setStats.set0``).
Per-set splits (``set1``/``set2``/...) are intentionally not staged here; raw is
preserved and can be re-staged at a finer grain later if needed.

The set0 payload is a *list* of stat rows, each identified by a ``name`` string
(not list position — the ``order`` field is non-contiguous). We key off ``name`` and
flatten to one row per match.

Column names for the 16 always-present stats mirror ``MatchStatsRecord`` exactly
(same datapoints, a different ATP source) for backwards compatibility — including the
``svc_``/``ret_``/``pts_`` grouping and the explicit numerator/denominator columns
(``..._won``/``..._played``, ``first_serve_in``/``first_serve_att``,
``bp_saved``/``bp_faced``, ``bp_converted``/``bp_opportunities``). These are required.

Six stats have no ``match_stats`` counterpart and are reported only on a subset of
matches/courts, so they are nullable: net points (won/played), winners, unforced
errors, and the three serve speeds (km/h). An absent stat name yields NULL columns;
a present ``"0/0 (0%)"`` yields ``(0, 0)`` — these are distinct states.

``serve_rating``/``return_rating`` are opaque ATP composite scores, not event counts.
"""

from datetime import datetime

from pydantic import BaseModel, field_validator

from mvp.atptour.mappings import map_player_id
from mvp.common.schema_hash import compute_schema_hash

# set0 stat name -> (numerator column stem, denominator column stem).
# Stems mirror MatchStatsRecord for the 10 shared frac stats; net points is
# stats_plus-only (nullable).
FRAC_STATS: dict[str, tuple[str, str]] = {
    "1st Serve": ("svc_first_serve_in", "svc_first_serve_att"),
    "1st Serve Points Won": ("svc_first_serve_pts_won", "svc_first_serve_pts_played"),
    "2nd Serve Points Won": ("svc_second_serve_pts_won", "svc_second_serve_pts_played"),
    "Break Points Saved": ("svc_bp_saved", "svc_bp_faced"),
    "1st Serve Return Points Won": ("ret_first_serve_pts_won", "ret_first_serve_pts_played"),
    "2nd Serve Return Points Won": ("ret_second_serve_pts_won", "ret_second_serve_pts_played"),
    "Break Points Converted": ("ret_bp_converted", "ret_bp_opportunities"),
    "Service Points Won": ("pts_service_pts_won", "pts_service_pts_played"),
    "Return Points Won": ("pts_return_pts_won", "pts_return_pts_played"),
    "Total Points Won": ("pts_total_pts_won", "pts_total_pts_played"),
    # stats_plus-only (no match_stats counterpart), nullable:
    "Net Points Won": ("pts_net_pts_won", "pts_net_pts_played"),
}

# set0 stat name -> column stem (single int per player).
NUM_STATS: dict[str, str] = {
    "Serve Rating": "svc_serve_rating",
    "Aces": "svc_aces",
    "Double Faults": "svc_double_faults",
    "Service Games Played": "svc_games_played",
    "Return Rating": "ret_return_rating",
    "Return Games Played": "ret_games_played",
    # stats_plus-only (no match_stats counterpart), nullable:
    "Winners": "winners",
    "Unforced Errors": "unforced_errors",
    "Max Speed": "max_serve_speed_kmh",
    "1st Serve Average Speed": "first_serve_avg_speed_kmh",
    "2nd Serve Average Speed": "second_serve_avg_speed_kmh",
}


class StatsPlusRecord(BaseModel):
    """Match-level stats_plus record — one row per match, from set0."""

    # Match context
    tournament_id: str
    year: int
    match_id: str
    is_doubles: bool
    p1_id: str
    p2_id: str
    sets_completed: int | None = None

    # --- P1 service (shared with match_stats, required) ---
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
    # --- P1 return (shared with match_stats, required) ---
    p1_ret_first_serve_pts_won: int
    p1_ret_first_serve_pts_played: int
    p1_ret_second_serve_pts_won: int
    p1_ret_second_serve_pts_played: int
    p1_ret_bp_converted: int
    p1_ret_bp_opportunities: int
    p1_ret_games_played: int
    p1_ret_return_rating: int
    # --- P1 points (shared with match_stats, required) ---
    p1_pts_service_pts_won: int
    p1_pts_service_pts_played: int
    p1_pts_return_pts_won: int
    p1_pts_return_pts_played: int
    p1_pts_total_pts_won: int
    p1_pts_total_pts_played: int
    # --- P1 stats_plus-only (nullable) ---
    p1_pts_net_pts_won: int | None = None
    p1_pts_net_pts_played: int | None = None
    p1_winners: int | None = None
    p1_unforced_errors: int | None = None
    p1_max_serve_speed_kmh: int | None = None
    p1_first_serve_avg_speed_kmh: int | None = None
    p1_second_serve_avg_speed_kmh: int | None = None

    # --- P2 service (shared with match_stats, required) ---
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
    # --- P2 return (shared with match_stats, required) ---
    p2_ret_first_serve_pts_won: int
    p2_ret_first_serve_pts_played: int
    p2_ret_second_serve_pts_won: int
    p2_ret_second_serve_pts_played: int
    p2_ret_bp_converted: int
    p2_ret_bp_opportunities: int
    p2_ret_games_played: int
    p2_ret_return_rating: int
    # --- P2 points (shared with match_stats, required) ---
    p2_pts_service_pts_won: int
    p2_pts_service_pts_played: int
    p2_pts_return_pts_won: int
    p2_pts_return_pts_played: int
    p2_pts_total_pts_won: int
    p2_pts_total_pts_played: int
    # --- P2 stats_plus-only (nullable) ---
    p2_pts_net_pts_won: int | None = None
    p2_pts_net_pts_played: int | None = None
    p2_winners: int | None = None
    p2_unforced_errors: int | None = None
    p2_max_serve_speed_kmh: int | None = None
    p2_first_serve_avg_speed_kmh: int | None = None
    p2_second_serve_avg_speed_kmh: int | None = None

    # Traceability
    source_file: str
    parsed_at: datetime

    _normalize_ids = field_validator("p1_id", "p2_id", mode="before")(map_player_id)

    @field_validator("match_id", mode="before")
    @classmethod
    def _uppercase_match_id(cls, v: str) -> str:
        return v.upper().strip()


SCHEMA_HASH = compute_schema_hash(StatsPlusRecord)
StatsPlusRecord.SCHEMA_HASH = SCHEMA_HASH
