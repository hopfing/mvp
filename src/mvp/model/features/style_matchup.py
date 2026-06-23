"""Style-matchup lookup (spec 2026-06-22-style-radar-lookup §5) — Form B.

Form B = the per-axis MARGINAL of the style matchup: for each radar axis k, the
no-intercept OLS slope of A's rating-residual on the opponent's axis-k value,
over A's past matches:

    style_resid_vs_k = Σ_O (opp_radar_k@t_O · resid_AO) / Σ_O (opp_radar_k@t_O)²

reads as "A's residual per unit of the opponent's axis-k." The OLS-slope form
(denominator = Σ of squares, never degenerate) replaces a signed-kernel-weighted
mean, whose denominator cancels to ~0 (review F1 blocker).

  resid_AO = won_AO − E_rating[t_O], E_rating = the Elo-implied win prob from the
    surface-Elo diff (rating-only, never the production model — D-RESID; surface
    Elo is the v1 D-RATING choice). resid = A's over/under-performance vs rating.
  opp_radar_k@t_O = the OPPONENT's radar axis k as of the historical match — the
    `opp_style_radar_{k}` column already on A's row, stamped at t_O, not
    forward-filled (review F3).

Leakage-safe: the rolling/cumulative sums use closed="left" / shift(1), so the
current match (its `won` is the target) is excluded — only A's strictly-prior
matches feed the slope. Form A (the joint kNN) is a separate, later build.
"""

import polars as pl

from mvp.model.primitives import cumulative_sum, rolling_sum
from mvp.model.registry import feature

_GRP = "player_id"
_AXES = ["serve", "net", "aggression", "error", "rally"]

# Ridge on the OLS-slope denominator: Σ(z·r)/(Σz²+λ). The raw slope is unbiased
# but explodes when Σz² is tiny (a player with little data on that axis), so it
# is shrunk toward 0 by λ — i.e. shrinkage by the effective sample Σz² (which is
# the conf the design carries anyway). λ in Σz² units; tunable.
_RIDGE = 1.0


def _residual() -> pl.Expr:
    """A's rating-residual: actual − Elo-implied (surface) win prob. Per match."""
    e = 1.0 / (1.0 + 10.0 ** (-pl.col("player_elo_surface_diff") / 400.0))
    return pl.col("won").cast(pl.Float64) - e


def _register_resid_vs(axis: str) -> None:
    @feature(name=f"style_resid_vs_{axis}", params=["days"], mirror=True, impute=None,
             depends_on=[f"style_radar_{axis}", "elo_surface_diff"],
             description=f"A's rating-residual OLS slope vs opponent's {axis} radar axis")
    def _f(days: int | None = None, _ax: str = axis) -> pl.Expr:
        z = pl.col(f"opp_style_radar_{_ax}")
        resid = _residual()
        num_expr, den_expr = z * resid, z * z
        if days is None:
            num = cumulative_sum(num_expr, group_by=_GRP, fill_with=None)
            den = cumulative_sum(den_expr, group_by=_GRP, fill_with=None)
        else:
            num = rolling_sum(num_expr, days=days, group_by=_GRP, fill_with=None)
            den = rolling_sum(den_expr, days=days, group_by=_GRP, fill_with=None)
        # Ridge-regularized slope: shrinks toward 0 when Σz² (the effective
        # sample) is small. Null only when there is no prior history at all.
        return pl.when(den.is_not_null()).then(num / (den + _RIDGE)).otherwise(None)


for _a in _AXES:
    _register_resid_vs(_a)
