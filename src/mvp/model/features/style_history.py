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

from mvp.model.primitives import cumulative_sum, rolling_sum
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
    """Build 4 unconditioned matchup features for a universal label.

    Each returned function takes an optional `days` window: None for career-
    cumulative, an int for rolling over the last `days` days.
    """
    opp_label_col = f"opp_is_{label}"

    def _accum(expr: pl.Expr, days: int | None) -> pl.Expr:
        if days is None:
            return cumulative_sum(expr, group_by="player_id")
        return rolling_sum(expr, days=days, group_by="player_id")

    def matches_vs(days: int | None = None) -> pl.Expr:
        cond = pl.col(opp_label_col).cast(pl.Int64)
        return _accum(cond, days)

    def wins_vs(days: int | None = None) -> pl.Expr:
        cond = pl.col("won").cast(pl.Int64) * pl.col(opp_label_col).cast(pl.Int64)
        return _accum(cond, days)

    def losses_vs(days: int | None = None) -> pl.Expr:
        cond = (1 - pl.col("won").cast(pl.Int64)) * pl.col(opp_label_col).cast(pl.Int64)
        return _accum(cond, days)

    def winpct_vs(days: int | None = None) -> pl.Expr:
        wins_expr = pl.col("won").cast(pl.Int64) * pl.col(opp_label_col).cast(pl.Int64)
        matches_expr = pl.col(opp_label_col).cast(pl.Int64)
        cum_w = _accum(wins_expr, days)
        cum_n = _accum(matches_expr, days)
        return pl.when(cum_n > 0).then(cum_w / cum_n).otherwise(None)

    return matches_vs, wins_vs, losses_vs, winpct_vs


def _surface_aligned_cum(
    value_per_match: pl.Expr,
    days: int | None = None,
) -> pl.Expr:
    """Cumulative count of value_per_match, restricted to prior matches where:

      - opp was a specialist of the CURRENT match's surface, AND
      - the prior match was played on the CURRENT match's surface

    Returns a per-row value that varies based on the current row's surface.
    Non-zero only on hard or clay matches; grass / indoor / carpet returns 0.

    If `days` is given, accumulation is over the last `days` days only (rolling);
    otherwise career-cumulative.
    """
    hard_mask = (
        pl.col("opp_is_hard_specialist").cast(pl.Int64)
        * (pl.col("surface") == "Hard").cast(pl.Int64)
    )
    clay_mask = (
        pl.col("opp_is_clay_specialist").cast(pl.Int64)
        * (pl.col("surface") == "Clay").cast(pl.Int64)
    )

    if days is None:
        cum_hard = cumulative_sum(value_per_match * hard_mask, group_by="player_id")
        cum_clay = cumulative_sum(value_per_match * clay_mask, group_by="player_id")
    else:
        cum_hard = rolling_sum(value_per_match * hard_mask, days=days, group_by="player_id")
        cum_clay = rolling_sum(value_per_match * clay_mask, days=days, group_by="player_id")

    return (
        pl.when(pl.col("surface") == "Hard").then(cum_hard)
        .when(pl.col("surface") == "Clay").then(cum_clay)
        .otherwise(pl.lit(0, dtype=pl.Float64))
    )


def _build_surface_specialist_composite():
    """Build the 4 composite surface-aligned matchup features.

    For each, the value for row R is computed against prior matches where the opp was a
    specialist of R's surface AND the prior match was on R's surface. On grass/indoor/
    carpet matches the value is 0. `days` selects the rolling window; None = career.
    """

    def matches_vs(days: int | None = None) -> pl.Expr:
        return _surface_aligned_cum(pl.lit(1, dtype=pl.Int64), days=days)

    def wins_vs(days: int | None = None) -> pl.Expr:
        return _surface_aligned_cum(pl.col("won").cast(pl.Int64), days=days)

    def losses_vs(days: int | None = None) -> pl.Expr:
        return _surface_aligned_cum(1 - pl.col("won").cast(pl.Int64), days=days)

    def winpct_vs(days: int | None = None) -> pl.Expr:
        cum_w = _surface_aligned_cum(pl.col("won").cast(pl.Int64), days=days)
        cum_n = _surface_aligned_cum(pl.lit(1, dtype=pl.Int64), days=days)
        return pl.when(cum_n > 0).then(cum_w / cum_n).otherwise(None)

    return matches_vs, wins_vs, losses_vs, winpct_vs


# Register universal-label matchups (24 base features = 6 labels * 4 metrics)
for _label in UNIVERSAL_LABELS:
    _matches, _wins, _losses, _winpct = _build_universal(_label)

    feature(
        name=f"matches_vs_{_label}",
        params=["days"], mirror=True, impute=0,
        depends_on=[f"is_{_label}"],
        description=(
            f"Prior matches vs opponents flagged is_{_label}; "
            "`days` = rolling window, omit for career-cumulative."
        ),
    )(_matches)

    feature(
        name=f"wins_vs_{_label}",
        params=["days"], mirror=True, impute=0,
        depends_on=[f"is_{_label}"],
        description=(
            f"Prior wins vs opponents flagged is_{_label}; "
            "`days` = rolling window, omit for career-cumulative."
        ),
    )(_wins)

    feature(
        name=f"losses_vs_{_label}",
        params=["days"], mirror=True, impute=0,
        depends_on=[f"is_{_label}"],
        description=(
            f"Prior losses vs opponents flagged is_{_label}; "
            "`days` = rolling window, omit for career-cumulative."
        ),
    )(_losses)

    feature(
        name=f"winpct_vs_{_label}",
        params=["days"], mirror=True, impute=None,
        depends_on=[f"is_{_label}"],
        description=(
            f"Win pct vs opponents flagged is_{_label} (null when no prior); "
            "`days` = rolling window, omit for career-cumulative."
        ),
    )(_winpct)

    for _stat in ("matches", "wins", "losses", "winpct"):
        register_diff(f"{_stat}_vs_{_label}")


# Register surface-aligned specialist matchups (4 composite base features).
# Each feature is gated by the CURRENT match's surface — non-zero only on hard or clay.
_matches_ssp, _wins_ssp, _losses_ssp, _winpct_ssp = _build_surface_specialist_composite()

feature(
    name="matches_vs_surface_specialists",
    params=["days"], mirror=True, impute=0,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Matches vs surface specialists in same-surface conditions: counts prior "
        "matches where opp was a specialist of the CURRENT match's surface AND "
        "the prior match was on that surface. 0 on non-hard/clay matches. "
        "`days` = rolling window in days; omit for career-cumulative."
    ),
)(_matches_ssp)

feature(
    name="wins_vs_surface_specialists",
    params=["days"], mirror=True, impute=0,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Wins vs surface specialists in same-surface conditions; `days` = "
        "rolling window, omit for career. See matches_vs_surface_specialists "
        "for gating logic."
    ),
)(_wins_ssp)

feature(
    name="losses_vs_surface_specialists",
    params=["days"], mirror=True, impute=0,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Losses vs surface specialists in same-surface conditions; `days` = "
        "rolling window, omit for career. See matches_vs_surface_specialists "
        "for gating logic."
    ),
)(_losses_ssp)

feature(
    name="winpct_vs_surface_specialists",
    params=["days"], mirror=True, impute=None,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Win pct vs surface specialists in same-surface conditions; null "
        "when no prior. `days` = rolling window, omit for career. See "
        "matches_vs_surface_specialists for gating."
    ),
)(_winpct_ssp)

for _stat in ("matches", "wins", "losses", "winpct"):
    register_diff(f"{_stat}_vs_surface_specialists")


# =============================================================================
# Vs-current-opponent's-type composites (serve / rally / net axes)
# =============================================================================
# Collapse the per-label universal composites into ONE always-relevant column per
# axis. For each row, accumulate the player's record against opponents that fell in
# the CURRENT opponent's bucket on that axis, then select that bucket. Buckets are
# mutually exclusive (tertile/binary labels), so exactly one branch fires; the value
# is null when the current opponent's type is unknown (label absent). No impute —
# unknown stays null rather than being merged into a fabricated neutral/0.5.


def _opp(label: str) -> pl.Expr:
    return pl.col(f"opp_is_{label}")


# (axis_name, [(bucket_name, current-opp condition), ...], depends_on labels)
_OPP_TYPE_AXES = [
    (
        "serve_type",
        [
            ("power", _opp("power_server") == 1),
            ("placement", _opp("placement_server") == 1),
            ("neutral", (_opp("power_server") == 0) & (_opp("placement_server") == 0)),
        ],
        ["is_power_server", "is_placement_server"],
    ),
    (
        "rally_type",
        [
            ("aggressive", _opp("aggressive_baseliner") == 1),
            ("counterpuncher", _opp("counterpuncher") == 1),
            ("neutral", (_opp("aggressive_baseliner") == 0) & (_opp("counterpuncher") == 0)),
        ],
        ["is_aggressive_baseliner", "is_counterpuncher"],
    ),
    (
        "net_type",
        [
            ("rusher", _opp("net_rusher") == 1),
            ("non_rusher", _opp("net_rusher") == 0),
        ],
        ["is_net_rusher"],
    ),
]


def _build_vs_opp_type(buckets, group="player_id"):
    """Build matches/wins/losses/winpct features gated to the current opp's bucket.

    For each bucket, accumulate the per-stat value over prior matches whose opp fell
    in that bucket, then select the accumulator for the bucket the CURRENT opp falls
    in. Null when the current opp's bucket is undefined (its type label is absent).

    `group` controls the accumulation scope: "player_id" = cross-surface career;
    ["player_id", "surface"] = restricted to prior matches on the current surface.
    """

    def _accum(expr: pl.Expr, days: int | None) -> pl.Expr:
        if days is None:
            return cumulative_sum(expr, group_by=group)
        return rolling_sum(expr, days=days, group_by=group)

    def _mask(cond: pl.Expr) -> pl.Expr:
        # Nullable labels: an unknown-style prior opp counts as 0 toward every bucket
        # (we only accumulate matches whose opp bucket is known).
        return cond.cast(pl.Int64).fill_null(0)

    def _select(branches: list[tuple[pl.Expr, pl.Expr]]) -> pl.Expr:
        acc = pl.when(branches[0][0]).then(branches[0][1])
        for cond, val in branches[1:]:
            acc = acc.when(cond).then(val)
        return acc.otherwise(None)

    def matches_vs(days: int | None = None) -> pl.Expr:
        return _select([(c, _accum(_mask(c), days)) for _, c in buckets])

    def wins_vs(days: int | None = None) -> pl.Expr:
        won = pl.col("won").cast(pl.Int64)
        return _select([(c, _accum(won * _mask(c), days)) for _, c in buckets])

    def losses_vs(days: int | None = None) -> pl.Expr:
        lost = 1 - pl.col("won").cast(pl.Int64)
        return _select([(c, _accum(lost * _mask(c), days)) for _, c in buckets])

    def winpct_vs(days: int | None = None) -> pl.Expr:
        won = pl.col("won").cast(pl.Int64)
        branches = []
        for _, c in buckets:
            m = _mask(c)
            cum_w = _accum(won * m, days)
            cum_n = _accum(m, days)
            branches.append((c, pl.when(cum_n > 0).then(cum_w / cum_n).otherwise(None)))
        return _select(branches)

    return matches_vs, wins_vs, losses_vs, winpct_vs


for _axis, _buckets, _deps in _OPP_TYPE_AXES:
    _m_ot, _w_ot, _l_ot, _wp_ot = _build_vs_opp_type(_buckets)

    feature(
        name=f"matches_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Prior matches vs opponents sharing the current opponent's {_axis} "
            "bucket; null when the current opp's type is unknown. `days` = rolling "
            "window, omit for career-cumulative."
        ),
    )(_m_ot)

    feature(
        name=f"wins_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Prior wins vs opponents sharing the current opponent's {_axis} bucket; "
            "null when type unknown. `days` = rolling window, omit for career."
        ),
    )(_w_ot)

    feature(
        name=f"losses_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Prior losses vs opponents sharing the current opponent's {_axis} bucket; "
            "null when type unknown. `days` = rolling window, omit for career."
        ),
    )(_l_ot)

    feature(
        name=f"winpct_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Win pct vs opponents sharing the current opponent's {_axis} bucket; "
            "null when no prior or type unknown. `days` = rolling window, omit for career."
        ),
    )(_wp_ot)

    for _stat in ("matches", "wins", "losses", "winpct"):
        register_diff(f"{_stat}_vs_opp_{_axis}")


# Surface-conditioned variants: same as above but accumulation restricted to prior
# matches on the CURRENT surface (group by player + surface). Works on all surfaces
# (style types, unlike specialist labels, exist on every surface).
for _axis, _buckets, _deps in _OPP_TYPE_AXES:
    _m_os, _w_os, _l_os, _wp_os = _build_vs_opp_type(_buckets, group=["player_id", "surface"])

    feature(
        name=f"surface_matches_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Prior same-surface matches vs opponents sharing the current opp's "
            f"{_axis} bucket; null when type unknown. `days` = rolling, omit for career."
        ),
    )(_m_os)

    feature(
        name=f"surface_wins_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Prior same-surface wins vs opponents sharing the current opp's {_axis} "
            "bucket; null when type unknown. `days` = rolling, omit for career."
        ),
    )(_w_os)

    feature(
        name=f"surface_losses_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Prior same-surface losses vs opponents sharing the current opp's {_axis} "
            "bucket; null when type unknown. `days` = rolling, omit for career."
        ),
    )(_l_os)

    feature(
        name=f"surface_winpct_vs_opp_{_axis}",
        params=["days"], mirror=True, impute=None, depends_on=_deps,
        description=(
            f"Same-surface win pct vs opponents sharing the current opp's {_axis} "
            "bucket; null when no prior or type unknown. `days` = rolling, omit for career."
        ),
    )(_wp_os)

    for _stat in ("matches", "wins", "losses", "winpct"):
        register_diff(f"surface_{_stat}_vs_opp_{_axis}")


# =============================================================================
# Surface rate stats vs surface specialists (+ elo-weighted quality vs spec)
# =============================================================================
# A per-surface rate accumulated only over prior same-surface matches vs specialists
# of that surface (same gating as the surface_specialists count composite). Fires
# only on hard/clay (no specialist label elsewhere); null on other surfaces, when
# no prior, or when the denominator is empty. No impute.


def _surf_spec_ratio(num: pl.Expr, den: pl.Expr, days: int | None = None) -> pl.Expr:
    hard = (
        pl.col("opp_is_hard_specialist").cast(pl.Int64).fill_null(0)
        * (pl.col("surface") == "Hard").cast(pl.Int64)
    )
    clay = (
        pl.col("opp_is_clay_specialist").cast(pl.Int64).fill_null(0)
        * (pl.col("surface") == "Clay").cast(pl.Int64)
    )

    def acc(e: pl.Expr) -> pl.Expr:
        if days is None:
            return cumulative_sum(e, group_by="player_id")
        return rolling_sum(e, days=days, group_by="player_id")

    nh, dh = acc(num * hard), acc(den * hard)
    nc, dc = acc(num * clay), acc(den * clay)
    return (
        pl.when((pl.col("surface") == "Hard") & (dh > 0)).then(nh / dh)
        .when((pl.col("surface") == "Clay") & (dc > 0)).then(nc / dc)
        .otherwise(None)
    )


def _make_surf_spec_fn(num: pl.Expr, den: pl.Expr):
    def fn(days: int | None = None) -> pl.Expr:
        return _surf_spec_ratio(num, den, days)
    return fn


_holds = pl.col("svc_games_played") - (pl.col("svc_bp_faced") - pl.col("svc_bp_saved"))

# (feature_name, numerator, denominator) — mirrors the 12 surface_* micro-stats
_SURF_SPEC_RATIOS = [
    ("surface_first_serve_win_pct_vs_surf_spec",
     pl.col("svc_first_serve_pts_won"), pl.col("svc_first_serve_pts_played")),
    ("surface_second_serve_win_pct_vs_surf_spec",
     pl.col("svc_second_serve_pts_won"), pl.col("svc_second_serve_pts_played")),
    ("surface_ace_pct_vs_surf_spec",
     pl.col("svc_aces"), pl.col("svc_first_serve_att")),
    ("surface_df_pct_vs_surf_spec",
     pl.col("svc_double_faults"), pl.col("svc_first_serve_att")),
    ("surface_first_serve_in_pct_vs_surf_spec",
     pl.col("svc_first_serve_in"), pl.col("svc_first_serve_att")),
    ("surface_bp_save_pct_vs_surf_spec",
     pl.col("svc_bp_saved"), pl.col("svc_bp_faced")),
    ("surface_hold_pct_vs_surf_spec",
     _holds, pl.col("svc_games_played")),
    ("surface_ret_first_serve_win_pct_vs_surf_spec",
     pl.col("ret_first_serve_pts_won"), pl.col("ret_first_serve_pts_played")),
    ("surface_ret_second_serve_win_pct_vs_surf_spec",
     pl.col("ret_second_serve_pts_won"), pl.col("ret_second_serve_pts_played")),
    ("surface_ret_bp_convert_pct_vs_surf_spec",
     pl.col("ret_bp_converted"), pl.col("ret_bp_opportunities")),
    ("surface_pts_service_won_pct_vs_surf_spec",
     pl.col("pts_service_pts_won"), pl.col("pts_service_pts_played")),
    ("surface_pts_return_won_pct_vs_surf_spec",
     pl.col("pts_return_pts_won"), pl.col("pts_return_pts_played")),
]

for _nm, _num, _den in _SURF_SPEC_RATIOS:
    feature(
        name=_nm, params=["days"], mirror=True, impute=None,
        depends_on=["is_hard_specialist", "is_clay_specialist"],
        description=(
            "Surface rate accumulated over prior same-surface matches vs specialists "
            "of that surface (hard/clay only; null otherwise / no prior). `days` = "
            "rolling window, omit for career."
        ),
    )(_make_surf_spec_fn(_num, _den))
    register_diff(_nm)

# Elo-weighted win rate vs surface specialists (quality x surf-spec).
feature(
    name="quality_win_rate_vs_surf_spec", params=["days"], mirror=True, impute=None,
    depends_on=["is_hard_specialist", "is_clay_specialist"],
    description=(
        "Elo-weighted win rate vs specialists of the current surface, over prior "
        "same-surface matches (hard/clay only; null otherwise / no prior). `days` = "
        "rolling window, omit for career."
    ),
)(_make_surf_spec_fn(pl.col("won").cast(pl.Float64) * pl.col("opp_elo"), pl.col("opp_elo")))
register_diff("quality_win_rate_vs_surf_spec")
