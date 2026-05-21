"""Style-conditional history: career counts/rates against opponents of each style type.

For each opponent style label, accumulate the player's prior matches/wins/losses where
the opp carried that label, then derive a win pct. Buckets are denser than h2h (which
is per-pair, often n=0-3); style matchups give career-long sample sizes per archetype.

Two label types:
  - **Universal style labels** (power_server, counterpuncher, etc.) — apply cross-surface.
    Unconditional career aggregation.
  - **Surface-specialist labels** (clay_specialist, hard_specialist) — only engage their
    specialty on the matching surface. Split into on-surface and off-surface variants;
    the off-surface form is a separate signal (potential "anti-feature").

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

# Surface-specialist labels — split into on-surface (engages) and off-surface (anti-signal).
# (label_suffix, surface_value_in_data, surface_name_for_feature)
SURFACE_SPECIALISTS = [
    ("clay_specialist", "Clay", "clay"),
    ("hard_specialist", "Hard", "hard"),
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


def _build_surface_conditioned(label: str, surface_value: str, on_surface: bool):
    """Build 4 surface-conditioned career features.

    on_surface=True  -> only counts matches where surface == surface_value
    on_surface=False -> only counts matches where surface != surface_value
    """
    opp_label_col = f"opp_is_{label}"
    if on_surface:
        surface_mask = (pl.col("surface") == surface_value).cast(pl.Int64)
    else:
        surface_mask = (pl.col("surface") != surface_value).cast(pl.Int64)

    def matches_vs() -> pl.Expr:
        cond = pl.col(opp_label_col).cast(pl.Int64) * surface_mask
        return cumulative_sum(cond, group_by="player_id")

    def wins_vs() -> pl.Expr:
        cond = (
            pl.col("won").cast(pl.Int64)
            * pl.col(opp_label_col).cast(pl.Int64)
            * surface_mask
        )
        return cumulative_sum(cond, group_by="player_id")

    def losses_vs() -> pl.Expr:
        cond = (
            (1 - pl.col("won").cast(pl.Int64))
            * pl.col(opp_label_col).cast(pl.Int64)
            * surface_mask
        )
        return cumulative_sum(cond, group_by="player_id")

    def winpct_vs() -> pl.Expr:
        wins_expr = (
            pl.col("won").cast(pl.Int64)
            * pl.col(opp_label_col).cast(pl.Int64)
            * surface_mask
        )
        matches_expr = pl.col(opp_label_col).cast(pl.Int64) * surface_mask
        cum_w = cumulative_sum(wins_expr, group_by="player_id")
        cum_n = cumulative_sum(matches_expr, group_by="player_id")
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


# Register surface-conditioned matchups (16 base features = 2 specialists * 2 on/off * 4 metrics)
for _label, _surface_value, _surface_name in SURFACE_SPECIALISTS:
    for _on_surface, _suffix in ((True, "on"), (False, "off")):
        _matches, _wins, _losses, _winpct = _build_surface_conditioned(
            _label, _surface_value, _on_surface,
        )
        _base = f"{_label}_{_suffix}_{_surface_name}"
        _qual = f"{'on' if _on_surface else 'off'} {_surface_value}"

        feature(
            name=f"matches_vs_{_base}",
            params=[], mirror=True, impute=0,
            depends_on=[f"is_{_label}"],
            description=f"Career count of prior matches vs is_{_label} opponents ({_qual})",
        )(_matches)

        feature(
            name=f"wins_vs_{_base}",
            params=[], mirror=True, impute=0,
            depends_on=[f"is_{_label}"],
            description=f"Career count of prior wins vs is_{_label} opponents ({_qual})",
        )(_wins)

        feature(
            name=f"losses_vs_{_base}",
            params=[], mirror=True, impute=0,
            depends_on=[f"is_{_label}"],
            description=f"Career count of prior losses vs is_{_label} opponents ({_qual})",
        )(_losses)

        feature(
            name=f"winpct_vs_{_base}",
            params=[], mirror=True, impute=0.5,
            depends_on=[f"is_{_label}"],
            description=f"Career win pct vs is_{_label} opponents ({_qual}); impute 0.5 when no prior",
        )(_winpct)

        for _stat in ("matches", "wins", "losses", "winpct"):
            register_diff(f"{_stat}_vs_{_base}")
