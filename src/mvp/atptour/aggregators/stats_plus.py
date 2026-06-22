"""Stats-plus match-total aggregation into player-match columns.

The staged ``stats_plus.parquet`` is a long table (one row per
``(match, set_num, stat)``). For the ``matches.parquet`` join we surface the
``set_num == 0`` rows (whole-match totals) only — ``matches.parquet`` is one row
per match and has no per-set slot.

We do NOT persist a copy of every stat. Most stats_plus stats already exist in
another source; we keep only what is genuinely new or measurably different,
decided on agreement measured against the existing feeds (probes:
``scripts/mb_vs_sp_agreement.py``, ``scripts/sp_vs_matchstats_agreement.py``):

  - **net points (won/played)** — no other source carries it -> NEW.
  - **winners / unforced errors** — match_beats has them but DISAGREES
    systematically (stats_plus ran +5.6 winners / +1.4 UE per player-match on a
    3.8k-row Tour sample; ATP's own tally vs match_beats' point-result-code
    derivation). Different measurement, so kept as DISTINCT ``sp_`` columns
    alongside the match_beats ones — never coalesced into them.
  - **per-stat influence** (ATP outcome share) for the kept stats — new metadata.
  - everything else (serve/return/points counts, ratings, serve speeds) agrees
    with match_stats / match_beats, which already cover it more broadly -> NOT
    surfaced here (no second copy).

Column names (suffix is after ``pivot_to_player_match`` adds ``player_``/``opp_``)
and the schema fragment in ``tournament_matches.py`` both derive from the lists
below so they cannot drift.
"""

import polars as pl

from mvp.atptour.aggregators.helpers import pivot_to_player_match

_INDEX = ["tournament_id", "year", "match_id", "p1_id", "p2_id"]

# stats_plus stat_key -> output stem. frac kept stats expand to _won/_played.
_FRAC_KEEP: dict[str, str] = {"pts_net_pts_won": "net_points"}
_NUM_KEEP: dict[str, str] = {"winners": "winners", "unforced_errors": "unforced_errors"}

# Player/opp field suffixes and shared per-stat influence fields.
STATSPLUS_PLAYER_FIELDS: list[str] = (
    [f"sp_{stem}_won" for stem in _FRAC_KEEP.values()]
    + [f"sp_{stem}_played" for stem in _FRAC_KEEP.values()]
    + [f"sp_{stem}" for stem in _NUM_KEEP.values()]
)
STATSPLUS_SHARED_FIELDS: list[str] = [
    f"sp_{stem}_influence" for stem in list(_FRAC_KEEP.values()) + list(_NUM_KEEP.values())
]


def stats_plus_to_player_match(raw: pl.DataFrame) -> pl.DataFrame:
    """Pivot long staged stats_plus (set_num=0 totals) to player-match rows.

    Mirrors the empty-state contract of the other match-level loaders: returns
    an empty frame when there is no singles set-total data to surface.
    """
    if raw.is_empty():
        return pl.DataFrame()

    df = raw.filter((~pl.col("is_doubles")) & (pl.col("set_num") == 0))
    if df.is_empty():
        return pl.DataFrame()

    df = df.with_columns(pl.col("match_id").str.to_uppercase())

    # One row per (match, stat) at set_num=0, so .first() picks that stat's
    # value; an absent stat yields an empty filter -> null (fixed column set).
    exprs: list[pl.Expr] = []
    for key, stem in _FRAC_KEEP.items():
        sel = pl.col("stat_key") == key
        exprs.append(pl.col("p1_num").filter(sel).first().alias(f"p1_sp_{stem}_won"))
        exprs.append(pl.col("p1_den").filter(sel).first().alias(f"p1_sp_{stem}_played"))
        exprs.append(pl.col("p2_num").filter(sel).first().alias(f"p2_sp_{stem}_won"))
        exprs.append(pl.col("p2_den").filter(sel).first().alias(f"p2_sp_{stem}_played"))
        exprs.append(pl.col("influence").filter(sel).first().alias(f"sp_{stem}_influence"))
    for key, stem in _NUM_KEEP.items():
        sel = pl.col("stat_key") == key
        exprs.append(pl.col("p1_num").filter(sel).first().alias(f"p1_sp_{stem}"))
        exprs.append(pl.col("p2_num").filter(sel).first().alias(f"p2_sp_{stem}"))
        exprs.append(pl.col("influence").filter(sel).first().alias(f"sp_{stem}_influence"))

    wide = df.group_by(_INDEX, maintain_order=True).agg(exprs)

    player_match = pivot_to_player_match(wide)
    if "opp_id" in player_match.columns:
        player_match = player_match.drop("opp_id")
    return player_match
