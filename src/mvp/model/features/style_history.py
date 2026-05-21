"""Style-conditional history: career counts/rates against opponents of each style type.

For each opponent style label, accumulate the player's prior matches/wins/losses where
the opp carried that label, then derive a win pct. Buckets are denser than h2h (which
is per-pair, often n=0-3); style matchups give career-long sample sizes per archetype.

Two label types:
  - **Universal style labels** (power_server, counterpuncher, etc.) — apply cross-surface.
    Unconditional career aggregation.
  - **Surface-specialist labels** (clay_specialist, hard_specialist) — composite features
    that count prior matches only when (opp was specialist of CURRENT match's surface)
    AND (prior match was on CURRENT match's surface). The feature is non-zero only on
    hard matches (using hard_specialist + prior-on-hard) or clay matches (using
    clay_specialist + prior-on-clay); grass / indoor / carpet matches get 0 since
    no specialist label exists for those surfaces.

Caveat: the underlying `is_<label>` features in style.py use rolling-quantile thresholds
(730d window) computed across the population. This is a time-aware threshold rather than
the dataset-wide quantile previously used (leakage was fixed alongside this module).
"""


import polars as pl

from mvp.model.primitives import cumulative_sum
from mvp.model.registry import feature, register_diff

# Universal style labels — apply cross-surface; no surface conditioning.
UNIVERSAL_LABELS = [
    "power_server",
    "placement_server",
    "counterpuncher",
    "aggressive_baseliner",
    "net_rusher",
    "clutch_player",
]

# Surface-specialist labels — composite features gated by current match's surface.
# (label_suffix, surface_value_in_data)
SURFACE_SPECIALISTS = [
    ("clay_specialist", "Clay"),
    ("hard_specialist", "Hard"),
]


def _build_universal(label: str):
    """Build 4 unconditioned career features for a universal label."""
    opp_label_col = f"opp_is_{label}"

    def matches_vs() -> pl.Expr:
        cond = pl.col(opp_label_col).cast(pl.Int64)
        return cumulative_sum(cond, group_by="player_id")

    def wins_vs() -> pl.Expr:
        cond = pl.col("won").cast(pl.Int64) * pl.col(opp_label_col).cast(pl.Int64)
        return cumulative_sum(cond, group_by="player_id")

    def losses_vs() -> pl.Expr:
        cond = (1 - pl.col("won").cast(pl.Int64)) * pl.col(opp_label_col).cast(pl.Int64)
        return cumulative_sum(cond, group_by="player_id")

    def winpct_vs() -> pl.Expr:
        wins_expr = pl.col("won").cast(pl.Int64) * pl.col(opp_label_col).cast(pl.Int64)
        matches_expr = pl.col(opp_label_col).cast(pl.Int64)
        cum_w = cumulative_sum(wins_expr, group_by="player_id")
        cum_n = cumulative_sum(matches_expr, group_by="player_id")
        return pl.when(cum_n > 0).then(cum_w / cum_n).otherwise(0.5)

    return matches_vs, wins_vs, losses_vs, winpct_vs


def _surface_aligned_cum(value_per_match: pl.Expr) -> pl.Expr:
    """Cumulative count of value_per_match, restricted to prior matches where:

      - opp was a specialist of the CURRENT match's surface, AND
      - the prior match was played on the CURRENT match's surface

    Returns a per-row value that varies based on the current row's surface.
    Non-zero only on hard or clay matches; grass / indoor / carpet returns 0.
    """
    hard_mask = (
        pl.col("opp_is_hard_specialist").cast(pl.Int64)
        * (pl.col("surface") == "Hard").cast(pl.Int64)
    )
    clay_mask = (
        pl.col("opp_is_clay_specialist").cast(pl.Int64)
        * (pl.col("surface") == "Clay").cast(pl.Int64)
    )

    cum_hard = cumulative_sum(value_per_match * hard_mask, group_by="player_id")
    cum_clay = cumulative_sum(value_per_match * clay_mask, group_by="player_id")

    return (
        pl.when(pl.col("surface") == "Hard").then(cum_hard)
        .when(pl.col("surface") == "Clay").then(cum_clay)
        .otherwise(pl.lit(0, dtype=pl.Float64))
    )


def _build_surface_specialist_composite():
    """Build the 4 composite surface-aligned matchup features.

    For each, the value for row R is computed against prior matches where the opp was a
    specialist of R's surface AND the prior match was on R's surface. On grass/indoor/
    carpet matches the value is 0.
    """

    def matches_vs() -> pl.Expr:
        return _surface_aligned_cum(pl.lit(1, dtype=pl.Int64))

    def wins_vs() -> pl.Expr:
        return _surface_aligned_cum(pl.col("won").cast(pl.Int64))

    def losses_vs() -> pl.Expr:
        return _surface_aligned_cum(1 - pl.col("won").cast(pl.Int64))

    def winpct_vs() -> pl.Expr:
        cum_w = _surface_aligned_cum(pl.col("won").cast(pl.Int64))
        cum_n = _surface_aligned_cum(pl.lit(1, dtype=pl.Int64))
        return pl.when(cum_n > 0).then(cum_w / cum_n).otherwise(0.5)

    return matches_vs, wins_vs, losses_vs, winpct_vs


# Register universal-label matchups (24 base features = 6 labels * 4 metrics)
for _label in UNIVERSAL_LABELS:
    _matches, _wins, _losses, _winpct = _build_universal(_label)

    feature(
        name=f"matches_vs_{_label}",
        params=[], mirror=True, impute=0,
        depends_on=[f"is_{_label}"],
        description=f"Career count of prior matches vs opponents flagged is_{_label}",
    )(_matches)

    feature(
        name=f"wins_vs_{_label}",
        params=[], mirror=True, impute=0,
        depends_on=[f"is_{_label}"],
        description=f"Career count of prior wins vs opponents flagged is_{_label}",
    )(_wins)

    feature(
        name=f"losses_vs_{_label}",
        params=[], mirror=True, impute=0,
        depends_on=[f"is_{_label}"],
        description=f"Career count of prior losses vs opponents flagged is_{_label}",
    )(_losses)

    feature(
        name=f"winpct_vs_{_label}",
        params=[], mirror=True, impute=0.5,
        depends_on=[f"is_{_label}"],
        description=f"Career win pct vs opponents flagged is_{_label} (impute 0.5 when no prior)",
    )(_winpct)

    for _stat in ("matches", "wins", "losses", "winpct"):
        register_diff(f"{_stat}_vs_{_label}")


# Register surface-aligned specialist matchups (4 composite base features).
# Each feature is gated by the CURRENT match's surface — non-zero only on hard or clay.
_matches_ssp, _wins_ssp, _losses_ssp, _winpct_ssp = _build_surface_specialist_composite()

feature(
    name="matches_vs_surface_specialists",
    params=[], mirror=True, impute=0,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Career matches vs surface specialists in same-surface conditions: "
        "counts prior matches where opp was a specialist of the CURRENT match's "
        "surface AND the prior match was on that surface. 0 on non-hard/clay matches."
    ),
)(_matches_ssp)

feature(
    name="wins_vs_surface_specialists",
    params=[], mirror=True, impute=0,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Career wins vs surface specialists in same-surface conditions "
        "(see matches_vs_surface_specialists for gating logic)."
    ),
)(_wins_ssp)

feature(
    name="losses_vs_surface_specialists",
    params=[], mirror=True, impute=0,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Career losses vs surface specialists in same-surface conditions "
        "(see matches_vs_surface_specialists for gating logic)."
    ),
)(_losses_ssp)

feature(
    name="winpct_vs_surface_specialists",
    params=[], mirror=True, impute=0.5,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Career win pct vs surface specialists in same-surface conditions; "
        "impute 0.5 when no prior. See matches_vs_surface_specialists for gating."
    ),
)(_winpct_ssp)

for _stat in ("matches", "wins", "losses", "winpct"):
    register_diff(f"{_stat}_vs_surface_specialists")
