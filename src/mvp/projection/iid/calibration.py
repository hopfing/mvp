"""Fit the gap shrink of a serve model on a match frame.

`gap_shrink` compresses the between-player serve-probability gap toward the
pair mean. The composed serve model sets that gap from fixed composition
weights rather than from a fit, so a chain scored at one fixed shrink rewards
dispersion from any source instead of ordering alone (spec: #107).

Three entry points, usable on any serve model that exposes `gap_shrink` and the
two predict methods:

- `realized_serve_shares` -- each player's realised serve-point share per
  match, so the proxy and any diagnostic compute it the same way.
- `score_at_shrink` -- one chain evaluation with the model held at a given
  shrink.
- `fit_gap_shrink` -- the shrink itself, by grid search on the caller's chain
  metric or by the cheap regression proxy, plus a `ShrinkFit` record of what
  it did.

Nothing here refits the model. The shrink is set, the existing match
distribution and chain scoring run, and the model's previous shrink is put
back -- which works only because the two-level serve model applies the shrink
as a per-player offset inside its state functions, where the dynamic programme
sees it.
"""

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import polars as pl

from mvp.projection.iid.metric_registry import is_minimize, score_chain
from mvp.projection.iid.stateful_chain import match_distribution_from_state_fn

# 0.6 to 2.0 inclusive, in steps of 0.1.
DEFAULT_GRID: tuple[float, ...] = tuple(round(0.6 + 0.1 * i, 1) for i in range(15))

# An extension never proposes a shrink below this: the shrink multiplies the
# between-player gap, so a point at or below zero inverts the favourite.
GRID_FLOOR = 0.1

# Empirical, and #107 records it as open, which is why it is a parameter
# rather than baked into the proxy.
DEFAULT_PROXY_K = 0.715

# The chain evaluation reads these off `matches`. A missing one is a caller
# error worth naming rather than an opaque polars ColumnNotFound.
_REQUIRED_MATCH_COLUMNS: tuple[str, ...] = (
    "best_of", "_target_games_a", "_target_games_b", "won",
)

# Below this many matches with a realised share for both players, the proxy's
# regression is fitting noise and reports a slope anyway.
_MIN_PROXY_ROWS = 10


@dataclass(frozen=True)
class ChainObjective:
    """What a chain evaluation scores: the metric and the lines it needs.

    Travels as one value because the three are never independently meaningful
    -- the lines exist only to serve the metric, and two of the chain metrics
    raise without them.
    """

    metric: str
    total_lines: tuple[float, ...]
    spread_lines: tuple[float, ...]


@dataclass(frozen=True)
class ShrinkFit:
    """What the fit did, for the FS history and for diagnostics to print."""

    method: Literal["grid", "proxy"]
    shrink: float
    # Every shrink evaluated, ascending, extension included. Empty for the
    # proxy, which evaluates no chain.
    evaluated: tuple[float, ...]
    scores: tuple[float, ...]  # aligned with `evaluated`; empty for the proxy
    slope: float | None  # proxy only
    edge: bool  # the chosen point is still an end of everything evaluated
    extended: bool  # an extension was evaluated


def realized_serve_shares(
    points: pl.DataFrame, matches: pl.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    """Each side's realised serve-point share per match, row-aligned to `matches`.

    Pools both serve numbers: the share is the mean of `point_won_by_server`
    over every non-null point that player served in that match. A player with
    no points in `points` gets NaN, so callers must mask before regressing.
    """
    scored = points.filter(pl.col("point_won_by_server").is_not_null())
    per_server = scored.group_by(["match_uid", "server_id"]).agg(
        pl.col("point_won_by_server").cast(pl.Float64).mean().alias("share")
    )
    keys = matches.select(["match_uid", "player_id", "opp_id"])
    share_a = keys.join(
        per_server,
        left_on=["match_uid", "player_id"],
        right_on=["match_uid", "server_id"],
        how="left",
    )["share"].to_numpy()
    share_b = keys.join(
        per_server,
        left_on=["match_uid", "opp_id"],
        right_on=["match_uid", "server_id"],
        how="left",
    )["share"].to_numpy()
    return share_a, share_b


def score_at_shrink(
    model: Any,
    matches: pl.DataFrame,
    objective: ChainObjective,
    shrink: float,
) -> float:
    """Score `matches` through the chain with `model` held at `shrink`.

    One chain evaluation: the state-function predict, the neutral predict, the
    match distribution, then the objective's metric. This is the serve FS chain
    scorer's own call sequence, and it is public so that scorer can call it
    (#109) instead of keeping a second copy that can drift from this one.

    The model's `gap_shrink` is restored before returning, on the exception
    path as well, so a caller's model is never left holding a probe value.
    """
    previous = model.gap_shrink
    try:
        model.gap_shrink = shrink
        p_a_fn, p_b_fn = model.predict_state_fn(matches)
        p_a, p_b = model.predict(matches)
        dist = match_distribution_from_state_fn(
            p_a_fn, p_b_fn, p_a, p_b,
            matches["best_of"].to_numpy().astype(np.int64),
        )
        return float(
            score_chain(
                objective.metric,
                dist,
                matches["_target_games_a"].to_numpy().astype(np.float64),
                matches["_target_games_b"].to_numpy().astype(np.float64),
                total_lines=list(objective.total_lines),
                spread_lines=list(objective.spread_lines),
                y_won=matches["won"].to_numpy().astype(np.int64),
            )
        )
    finally:
        model.gap_shrink = previous


def fit_gap_shrink(
    model: Any,
    matches: pl.DataFrame,
    points: pl.DataFrame | None,
    *,
    method: Literal["grid", "proxy"],
    objective: ChainObjective,
    grid: tuple[float, ...] = DEFAULT_GRID,
    proxy_k: float = DEFAULT_PROXY_K,
) -> ShrinkFit:
    """Fit `model`'s gap shrink on `matches` and hand back a record of the fit.

    `model` is any serve model carrying `gap_shrink`, `predict_state_fn` and
    `predict`; it is never refitted. `objective` carries the caller's chain
    metric, which supplies both the goal and its direction, so the fitter has
    no metric setting of its own.

    `points` is read by the proxy only; the grid accepts None for it.

    The model's `gap_shrink` is restored before returning, including on the way
    out of an exception.
    """
    if method not in ("grid", "proxy"):
        raise ValueError(
            f"Unknown gap-shrink method: {method!r}. Valid: 'grid', 'proxy'"
        )
    if method == "proxy" and points is None:
        raise ValueError("method='proxy' requires a points frame")
    for column in _REQUIRED_MATCH_COLUMNS:
        if column not in matches.columns:
            raise KeyError(f"fit_gap_shrink needs column {column!r} on `matches`")

    previous = model.gap_shrink
    try:
        if method == "proxy":
            return _fit_proxy(model, matches, points, proxy_k=proxy_k)
        return _fit_grid(model, matches, objective=objective, grid=grid)
    finally:
        model.gap_shrink = previous


def _fit_grid(
    model: Any,
    matches: pl.DataFrame,
    *,
    objective: ChainObjective,
    grid: tuple[float, ...],
) -> ShrinkFit:
    # Each point's distribution is built and dropped inside its own evaluation,
    # so the loop holds one at a time rather than one per grid point.
    evaluated = list(grid)
    scores = [score_at_shrink(model, matches, objective, g) for g in evaluated]
    best = _best_index(scores, objective.metric)

    # An optimum on an end means the grid may simply be too narrow, so widen it
    # once in that direction. Once, not until it stops: a runaway walk would
    # spend a dynamic-programming pass per step chasing a scale the data does
    # not support, and `edge` is what tells the caller the answer is truncated.
    extension = _extension_points(
        grid, at_start=best == 0, at_end=best == len(grid) - 1,
    )
    for shrink in extension:
        evaluated.append(shrink)
        scores.append(score_at_shrink(model, matches, objective, shrink))
    if extension:
        best = _best_index(scores, objective.metric)

    order = sorted(range(len(evaluated)), key=lambda i: evaluated[i])
    ascending = [evaluated[i] for i in order]
    return ShrinkFit(
        method="grid",
        shrink=float(evaluated[best]),
        evaluated=tuple(ascending),
        scores=tuple(scores[i] for i in order),
        slope=None,
        edge=evaluated[best] in (ascending[0], ascending[-1]),
        extended=bool(extension),
    )


def _extension_points(
    grid: tuple[float, ...], *, at_start: bool, at_end: bool,
) -> list[float]:
    """The new points to evaluate when the optimum lands on an end of `grid`.

    Extends by the grid's own span, in the grid's own step, in the one
    direction the optimum sits at. Empty when the optimum is interior, when the
    grid is too short to have a span, or when a downward extension falls
    entirely below the floor.
    """
    if not (at_start or at_end) or len(grid) < 2:
        return []
    step = grid[1] - grid[0]
    span = grid[-1] - grid[0]
    n_points = int(round(span / step))
    if n_points < 1:
        return []
    if at_start:
        candidates = [round(grid[0] - step * i, 10) for i in range(1, n_points + 1)]
        return [g for g in candidates if g >= GRID_FLOOR]
    return [round(grid[-1] + step * i, 10) for i in range(1, n_points + 1)]


def _best_index(scores: list[float], metric: str) -> int:
    arr = np.asarray(scores, dtype=np.float64)
    return int(np.argmin(arr) if is_minimize(metric) else np.argmax(arr))


def _fit_proxy(
    model: Any,
    matches: pl.DataFrame,
    points: pl.DataFrame,
    *,
    proxy_k: float,
) -> ShrinkFit:
    """The shrink as a constant times a regression slope, with no chain pass.

    Regresses the realised serve-share gap on the model's composed neutral gap
    over the same frame: the slope says how much of the gap the model draws is
    actually realised.
    """
    # Measured against the model's UNSHRUNK gap, whatever shrink the model
    # arrived carrying, so the answer does not depend on the caller's starting
    # point. `fit_gap_shrink` restores that value.
    model.gap_shrink = 1.0
    q_a, q_b = model.predict(matches)
    share_a, share_b = realized_serve_shares(points, matches)

    ok = np.isfinite(share_a) & np.isfinite(share_b)
    n_ok = int(ok.sum())
    if n_ok < _MIN_PROXY_ROWS:
        raise ValueError(
            f"gap-shrink proxy needs at least {_MIN_PROXY_ROWS} matches with a "
            f"realised serve share for both players; got {n_ok}"
        )

    slope = float(np.polyfit((q_a - q_b)[ok], (share_a - share_b)[ok], 1)[0])
    return ShrinkFit(
        method="proxy",
        shrink=proxy_k * slope,
        evaluated=(),
        scores=(),
        slope=slope,
        edge=False,
        extended=False,
    )
