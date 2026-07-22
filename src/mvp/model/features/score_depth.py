"""Score-depth features: how a player is winning (rolling window)."""


import polars as pl

from mvp.model.features._score_helpers import (
    blowout_sets as _blowout_sets,
)
from mvp.model.features._score_helpers import (
    is_straight_set_win as _is_straight_set_win,
)
from mvp.model.features._score_helpers import (
    tight_sets as _tight_sets,
)
from mvp.model.features._score_helpers import (
    total_games_lost as _total_games_lost,
)
from mvp.model.features._score_helpers import (
    total_games_won as _total_games_won,
)
from mvp.model.primitives import (
    cumulative_mean,
    cumulative_sum,
    ratio_feature,
    rolling_mean,
    rolling_sum,
    surface_ratio_feature,
)
from mvp.model.registry import feature, register_diff, register_sum


def _per_set(numerator: pl.Expr) -> pl.Expr:
    """Per-completed-set rate of ``numerator``.

    Null when ``sets_played`` is 0 (e.g. a first-set retirement: games were
    played but no set completed). Without the guard the bare divide yields
    ``inf`` — which, unlike a null, survives imputation and poisons every
    rolling/cumulative-window row that includes the match. Null matches these
    features' ``impute=None`` policy (it reaches the model as missing).
    """
    sets = pl.col("sets_played").cast(pl.Float64)
    return pl.when(sets > 0).then(numerator.cast(pl.Float64) / sets).otherwise(None)


def _games_won_per_set() -> pl.Expr:
    """Games won per set in this match."""
    return _per_set(_total_games_won())


def _games_lost_per_set() -> pl.Expr:
    """Games lost per set in this match."""
    return _per_set(_total_games_lost())


def _games_margin_per_set() -> pl.Expr:
    """(Games won - games lost) per set in this match."""
    return _per_set(_total_games_won() - _total_games_lost())


def _games_margin_per_set_won() -> pl.Expr:
    """Per-set games margin, but only on matches the player won (else null)."""
    return pl.when(pl.col("won").cast(pl.Int64) == 1).then(_games_margin_per_set()).otherwise(None)


def _games_margin_per_set_lost() -> pl.Expr:
    """Per-set games margin, but only on matches the player lost (else null)."""
    return pl.when(pl.col("won").cast(pl.Int64) == 0).then(_games_margin_per_set()).otherwise(None)


def _games_per_set() -> pl.Expr:
    """Total games per set in this match."""
    return _per_set(_total_games_won() + _total_games_lost())


# --- Base features ---


@feature(
    name="sets_per_match",
    params=["days"],
    description="Avg sets played per match in window",
    mirror=True,
    impute=None,
)
def sets_per_match(days: int | None = None) -> pl.Expr:
    expr = pl.col("sets_played").cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="straight_sets_win_pct",
    params=["days"],
    description="Fraction of wins in straight sets",
    mirror=True,
    impute=None,
)
def straight_sets_win_pct(days: int | None = None) -> pl.Expr:
    wins = pl.col("won").cast(pl.Int64)
    ss_wins = _is_straight_set_win()
    return ratio_feature(ss_wins, wins, days)


@feature(
    name="games_won_per_set",
    params=["days"],
    description="Avg games won per set in window",
    mirror=True,
    impute=None,
)
def games_won_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_won_per_set(), group_by="player_id")
    return rolling_mean(_games_won_per_set(), days=days, group_by="player_id")


@feature(
    name="games_lost_per_set",
    params=["days"],
    description="Avg games lost per set in window",
    mirror=True,
    impute=None,
)
def games_lost_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_lost_per_set(), group_by="player_id")
    return rolling_mean(_games_lost_per_set(), days=days, group_by="player_id")


@feature(
    name="games_margin_per_set",
    params=["days"],
    description="Avg (games won - lost) per set in window",
    mirror=True,
    impute=None,
)
def games_margin_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_margin_per_set(), group_by="player_id")
    return rolling_mean(_games_margin_per_set(), days=days, group_by="player_id")


@feature(
    name="games_margin_per_set_won",
    params=["days"],
    description="Avg (games won - lost) per set over the player's WON matches in window",
    mirror=True,
    impute=None,
)
def games_margin_per_set_won(days: int | None = None) -> pl.Expr:
    """Dominance in wins: how decisively the player wins, conditioned on winning.

    Conditioning on the outcome keeps this from washing out for ~50% players the
    way the pooled (win+loss) ``games_margin_per_set`` does — a balanced record
    no longer cancels positive-margin wins against negative-margin losses.
    """
    if days is None:
        return cumulative_mean(_games_margin_per_set_won(), group_by="player_id")
    return rolling_mean(_games_margin_per_set_won(), days=days, group_by="player_id")


@feature(
    name="games_margin_per_set_lost",
    params=["days"],
    description="Avg (games won - lost) per set over the player's LOST matches in window",
    mirror=True,
    impute=None,
)
def games_margin_per_set_lost(days: int | None = None) -> pl.Expr:
    """Competitiveness in losses: how close vs. blown-out the player's losses are
    (negative; nearer 0 = more competitive). The half of the dominance picture no
    existing feature captures, and the most discriminating for middle-pack players.
    """
    if days is None:
        return cumulative_mean(_games_margin_per_set_lost(), group_by="player_id")
    return rolling_mean(_games_margin_per_set_lost(), days=days, group_by="player_id")


@feature(
    name="games_per_set",
    params=["days"],
    description="Avg total games per set in window (tightness)",
    mirror=True,
    impute=None,
)
def games_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_per_set(), group_by="player_id")
    return rolling_mean(_games_per_set(), days=days, group_by="player_id")


@feature(
    name="total_games_won",
    params=["days"],
    description="Avg total games won per match in window",
    mirror=True,
    impute=None,
)
def total_games_won(days: int | None = None) -> pl.Expr:
    expr = _total_games_won().cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="total_games_lost",
    params=["days"],
    description="Avg total games conceded per match in window",
    mirror=True,
    impute=None,
)
def total_games_lost(days: int | None = None) -> pl.Expr:
    expr = _total_games_lost().cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="total_games",
    params=["days"],
    description="Avg total games per match in window (match length tendency)",
    mirror=True,
    impute=None,
)
def total_games(days: int | None = None) -> pl.Expr:
    expr = (_total_games_won() + _total_games_lost()).cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="recent_games_load",
    params=["days"],
    description="Total games played (won + lost) in window (physical load proxy)",
    mirror=True,
    impute=0,
)
def recent_games_load(days: int | None = None) -> pl.Expr:
    """Total games played in the window — better fatigue proxy than match count.

    A player who played three 5-set matches has more load than one
    who played three straight-set wins.
    """
    expr = (_total_games_won() + _total_games_lost()).cast(pl.Float64)
    if days is None:
        return cumulative_sum(expr, group_by="player_id")
    return rolling_sum(expr, days=days, group_by="player_id")


@feature(
    name="tight_set_pct",
    params=["days"],
    description="Fraction of sets at 7-5 or 7-6 (tight set tendency)",
    mirror=True,
    impute=None,
)
def tight_set_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(_tight_sets(), pl.col("sets_played").cast(pl.Int64), days)


@feature(
    name="blowout_set_pct",
    params=["days"],
    description="Fraction of sets at 6-0 or 6-1 (blowout tendency)",
    mirror=True,
    impute=None,
)
def blowout_set_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(_blowout_sets(), pl.col("sets_played").cast(pl.Int64), days)


# --- Derived diff features ---

for _base in [
    "sets_per_match", "straight_sets_win_pct", "games_won_per_set",
    "games_lost_per_set", "games_margin_per_set", "games_margin_per_set_won",
    "games_margin_per_set_lost", "games_per_set",
    "total_games_won", "total_games_lost", "total_games",
    "recent_games_load", "tight_set_pct", "blowout_set_pct",
]:
    register_diff(_base)

for _base in ["games_per_set", "sets_per_match", "total_games",
              "tight_set_pct", "blowout_set_pct"]:
    register_sum(_base)


# --- Surface-conditioned variants (A1: score shape) ---
#
# Same per-set / per-match means as the base features, grouped by
# (player_id, surface). All are mean-based (no ``ratio_feature``), so there is
# no EB shrinkage ``k`` — and therefore none of the pooled-shrinkage-target
# concern that affects the surface_* ratio stats. Scoreline data is near-fully
# covered, so the only cost of the split is sample-thinning.
#
# The three k-less ratios in this module (straight_sets_win_pct, tight_set_pct,
# blowout_set_pct) are intentionally NOT surfaced here — they need shrinkage
# added first (deferred to A2). ``recent_games_load`` is also omitted: cumulative
# games-played is a cross-surface fatigue proxy, not a surface-specific shape.

_SURFACE_GROUP = ["player_id", "surface"]

# (name, description, per-match expr thunk)
_SURFACE_MEAN_SPECS = [
    ("surface_sets_per_match", "Avg sets per match on current surface in window",
     lambda: pl.col("sets_played").cast(pl.Float64)),
    ("surface_games_won_per_set", "Avg games won per set on current surface in window",
     _games_won_per_set),
    ("surface_games_lost_per_set", "Avg games lost per set on current surface in window",
     _games_lost_per_set),
    ("surface_games_margin_per_set", "Avg (games won - lost) per set on current surface in window",
     _games_margin_per_set),
    ("surface_games_margin_per_set_won", "Avg per-set games margin over WON matches on current surface",
     _games_margin_per_set_won),
    ("surface_games_margin_per_set_lost", "Avg per-set games margin over LOST matches on current surface",
     _games_margin_per_set_lost),
    ("surface_games_per_set", "Avg total games per set on current surface in window (tightness)",
     _games_per_set),
    ("surface_total_games_won", "Avg total games won per match on current surface in window",
     lambda: _total_games_won().cast(pl.Float64)),
    ("surface_total_games_lost", "Avg total games conceded per match on current surface in window",
     lambda: _total_games_lost().cast(pl.Float64)),
    ("surface_total_games", "Avg total games per match on current surface in window (length tendency)",
     lambda: (_total_games_won() + _total_games_lost()).cast(pl.Float64)),
]

for _name, _desc, _expr_fn in _SURFACE_MEAN_SPECS:
    @feature(name=_name, params=["days"], description=_desc, mirror=True, impute=None)
    def _surface_mean(days: int | None = None, _expr_fn=_expr_fn) -> pl.Expr:
        if days is None:
            return cumulative_mean(_expr_fn(), group_by=_SURFACE_GROUP)
        return rolling_mean(_expr_fn(), days=days, group_by=_SURFACE_GROUP)

    register_diff(_name)

# Sum contrasts for the length/tightness shape, mirroring the base module's choice.
for _base in ["surface_sets_per_match", "surface_games_per_set", "surface_total_games"]:
    register_sum(_base)


# --- Surface-conditioned blowout tendency (A2) ---
#
# Unlike the A1 means above, this is an EB-shrunk RATE, so it needs a shrinkage k.
# scripts/_eb_shrinkage_k.py gives k~8 sets — real between-player variance — so it
# earns a surface split. The other two k-less score ratios were dropped on the same
# derivation: straight_sets_win_pct came back k~1350 and tight_set_pct a degenerate
# k (near-zero between-player signal), so surface-splitting them would be ~constant.
@feature(
    name="surface_blowout_set_pct",
    params=["days"],
    description="Fraction of sets at 6-0/6-1 on current surface (blowout tendency)",
    mirror=True,
    impute=None,
)
def surface_blowout_set_pct(days: int | None = None) -> pl.Expr:
    return surface_ratio_feature(
        _blowout_sets(), pl.col("sets_played").cast(pl.Int64), days, k=8.0,
    )


register_diff("surface_blowout_set_pct")
register_sum("surface_blowout_set_pct")
