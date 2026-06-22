"""Schema for stats_plus staged data — normalized long format.

One row per ``(match, set_num, stat)``. ``set_num=0`` is the whole-match total;
``set_num=1..N`` are the individual sets (the feed's ``setStats.set0`` = match
total, ``set1..setN`` = the sets — verified on raw payloads). The raw payload is
itself a *list of stat rows per set*, so a long table mirrors it faithfully and
is the same staging shape as ``match_beats.parquet`` (one row per point, match
context denormalized onto every row).

Stat values come in two kinds (``STAT_REGISTRY`` carries the split):
  - ``frac`` (``"N/M (P%)"``) -> ``(num, den)``
  - ``num`` (bare int, incl. opaque composites like Serve/Return Rating and the
    km/h serve speeds) -> ``num`` with ``den = None``.
All value columns are nullable: a missing/unparseable value stages as ``None``
(we null the single bad value rather than drop the whole match), while a present
``"0/0 (0%)"`` stages as ``(0, 0)`` — distinct from absent.

``influence`` is the ATP per-stat "share of match outcome" (e.g. ``"6%"``) parsed
to a float fraction in ``[0, 1]`` (``"6%" -> 0.06``); absent/unparseable -> ``None``.

Raw fields intentionally NOT staged (see spec
``2026-06-22-statsplus-staging-aggregation``):
  - ``player{1,2}Points`` / ``CrucialPoints`` — empty across the corpus sample.
  - ``player*Bar`` / ``order`` — UI render artifacts, redundant with parsed values.
  - ``tm1*`` / ``tm2*`` — doubles slots, empty in singles.

The wide / ``match_stats``-mirrored view is a Phase-2 integration concern,
recoverable by pivoting this table; it is deliberately not produced here.
"""

from datetime import datetime

from pydantic import BaseModel, field_validator

from mvp.atptour.mappings import map_player_id
from mvp.common.schema_hash import compute_schema_hash

# Raw ATP stat ``name`` -> (stat_key, kind). kind in {"frac", "num"}.
# stat_key is a stable snake_case identifier (survives ATP display-string
# renames); the keys mirror the prior match_stats column stems so a Phase-2
# pivot lands on familiar names. Speed keys carry the km/h unit, since the
# generic ``num`` column drops the unit annotation the wide schema had.
STAT_REGISTRY: dict[str, tuple[str, str]] = {
    # --- frac ("N/M (P%)") ---
    "1st Serve": ("svc_first_serve", "frac"),
    "1st Serve Points Won": ("svc_first_serve_pts_won", "frac"),
    "2nd Serve Points Won": ("svc_second_serve_pts_won", "frac"),
    "Break Points Saved": ("svc_bp_saved", "frac"),
    "1st Serve Return Points Won": ("ret_first_serve_pts_won", "frac"),
    "2nd Serve Return Points Won": ("ret_second_serve_pts_won", "frac"),
    "Break Points Converted": ("ret_bp_converted", "frac"),
    "Service Points Won": ("pts_service_pts_won", "frac"),
    "Return Points Won": ("pts_return_pts_won", "frac"),
    "Total Points Won": ("pts_total_pts_won", "frac"),
    # stats_plus-only (no match_stats counterpart):
    "Net Points Won": ("pts_net_pts_won", "frac"),
    # --- num (bare int) ---
    "Serve Rating": ("svc_serve_rating", "num"),
    "Aces": ("svc_aces", "num"),
    "Double Faults": ("svc_double_faults", "num"),
    "Service Games Played": ("svc_games_played", "num"),
    "Return Rating": ("ret_return_rating", "num"),
    "Return Games Played": ("ret_games_played", "num"),
    # stats_plus-only (no match_stats counterpart):
    "Winners": ("winners", "num"),
    "Unforced Errors": ("unforced_errors", "num"),
    "Max Speed": ("max_serve_speed_kmh", "num"),
    "1st Serve Average Speed": ("first_serve_avg_speed_kmh", "num"),
    "2nd Serve Average Speed": ("second_serve_avg_speed_kmh", "num"),
}


class StatsPlusRowRecord(BaseModel):
    """A single staged stats_plus observation: one ``(match, set_num, stat)``.

    Match context is denormalized onto every row (same pattern as
    ``MatchBeatsPointRecord``) for efficient querying without a join.
    """

    # Match context (denormalized onto every row)
    tournament_id: str
    year: int
    match_id: str
    is_doubles: bool
    p1_id: str
    p2_id: str
    sets_completed: int | None = None

    # Grain keys
    set_num: int  # 0 = match total, 1..N = individual sets
    stat_key: str
    stat_name: str  # raw ATP display name (provenance)

    # Values: frac -> num/den; num -> num with den = None. All nullable.
    p1_num: int | None = None
    p1_den: int | None = None
    p2_num: int | None = None
    p2_den: int | None = None
    influence: float | None = None

    # Traceability
    source_file: str
    parsed_at: datetime

    # Player IDs normalized via map_player_id (ADR-002): ATP IDs uppercased,
    # Sportradar IDs mapped. An UNMAPPED SR id raises ValueError, which drops
    # that match's file in the transformer. SR ids aren't expected from the
    # match-centre feed (it emits ATP ids), so this is a guard, not a hot path.
    _normalize_ids = field_validator("p1_id", "p2_id", mode="before")(map_player_id)

    @field_validator("match_id", mode="before")
    @classmethod
    def _uppercase_match_id(cls, v: str) -> str:
        return v.upper().strip()


SCHEMA_HASH = compute_schema_hash(StatsPlusRowRecord)
StatsPlusRowRecord.SCHEMA_HASH = SCHEMA_HASH
