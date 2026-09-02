"""Chain shape scalars: the projection distribution's moments, shared schema.

Lives in `mvp.common` (a leaf package) because both sides of the artifact
boundary need it and they must never import each other at module scope: the
projection writers (`mvp.projection.iid`) produce the columns, and the
winner-side `chain_shape` feature transform (`mvp.model.features.prior`)
consumes them — importing the projection package from the features package
at import time closes a cycle through the engine.

The level scalar (`p_match_win_a`) is what the winner-side prior consumes;
these are the moments that collapse discards — how the match is structured,
not who wins. Split by mirror semantics: symmetric columns read the same from
both players' perspectives; antisymmetric columns negate on the mirrored
orientation.
"""

from __future__ import annotations

from typing import Any

import numpy as np

SHAPE_SYMMETRIC = [
    "chain_egames", "chain_gstd", "chain_spread_std", "chain_hold_sum",
    "chain_p_straight", "chain_p_decider", "chain_p_4set",
    "chain_serve_level",
]
SHAPE_ANTISYMMETRIC = [
    "chain_hold_asym", "chain_serve_gap", "chain_tb_edge", "chain_espread",
]
SHAPE_COLUMNS = SHAPE_SYMMETRIC + SHAPE_ANTISYMMETRIC


def shape_scalars(out: Any) -> dict[str, np.ndarray]:
    """The twelve shape scalars for a ProjectionOutput, row-aligned to it.

    Everything here is a reduction of fields the projector already computed —
    nothing is re-derived from serve rates, so these stay consistent with
    `p_match_win_a` by construction. Set-outcome keys are looked up
    defensively: `match_distribution` only inserts keys for a best_of present
    in the batch, so a bo3-only frame has no (3, x) keys and they contribute
    zero rather than KeyError.
    """
    dist = out.distribution
    n = len(dist.p_match_win_a)

    games = np.arange(dist.total_games_pmf.shape[1], dtype=np.float64)
    gvar = (dist.total_games_pmf * games**2).sum(axis=1) - dist.expected_total_games**2
    spreads = (
        np.arange(dist.spread_pmf.shape[1], dtype=np.float64) - dist.spread_offset
    )
    svar = (dist.spread_pmf * spreads**2).sum(axis=1) - dist.expected_spread**2

    def _outcomes(*keys: tuple[int, int]) -> np.ndarray:
        acc = np.zeros(n, dtype=np.float64)
        for k in keys:
            vec = dist.set_outcome_probs.get(k)
            if vec is not None:
                acc = acc + vec
        return acc

    return {
        "chain_egames": dist.expected_total_games,
        "chain_gstd": np.sqrt(np.clip(gvar, 0.0, None)),
        "chain_spread_std": np.sqrt(np.clip(svar, 0.0, None)),
        "chain_hold_sum": out.h_a + out.h_b,
        "chain_p_straight": _outcomes((2, 0), (0, 2), (3, 0), (0, 3)),
        "chain_p_decider": _outcomes((2, 1), (1, 2), (3, 2), (2, 3)),
        "chain_p_4set": _outcomes((3, 1), (1, 3)),
        "chain_serve_level": out.p_a_serve_win + out.p_b_serve_win,
        "chain_hold_asym": out.h_a - out.h_b,
        "chain_serve_gap": out.p_a_serve_win - out.p_b_serve_win,
        "chain_tb_edge": out.t_ab - 0.5,
        "chain_espread": dist.expected_spread,
    }
