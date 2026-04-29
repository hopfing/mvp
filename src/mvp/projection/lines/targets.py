"""Per-market binary label derivation from match-grain game counts.

Each function maps realized (games_a, games_b) outcomes to a `{line: labels}`
dict where each label vector holds `1{outcome > line}` per match. Thresholding
matches `MatchDistribution.p_over_total` / `p_a_spread_cover` semantics:
strictly greater than the line.
"""

from typing import Final, Literal

import numpy as np


Target = Literal["total", "spread", "player_games"]
TARGET_NAMES: Final[tuple[Target, ...]] = ("total", "spread", "player_games")


def total_labels(
    y_a: np.ndarray, y_b: np.ndarray, lines: list[float],
) -> dict[float, np.ndarray]:
    """`1{(y_a + y_b) > line}` per line, per match."""
    total = y_a.astype(np.int64) + y_b.astype(np.int64)
    return {float(L): (total > L).astype(np.int64) for L in lines}


def spread_labels(
    y_a: np.ndarray, y_b: np.ndarray, lines: list[float],
) -> dict[float, np.ndarray]:
    """`1{(y_a - y_b) > line}` per line, per match.

    Lines may be positive or negative; the threshold is signed and matches
    `MatchDistribution.p_a_spread_cover`.
    """
    spread = y_a.astype(np.int64) - y_b.astype(np.int64)
    return {float(L): (spread > L).astype(np.int64) for L in lines}


def player_games_labels(
    y_player: np.ndarray, lines: list[float],
) -> dict[float, np.ndarray]:
    """`1{y_player > line}` per line, per match.

    The caller controls perspective: pass `y_a` for player-A markets, `y_b`
    for player-B markets, or use the doubled-data pattern (concatenate both
    perspectives with mirrored features) to fit a single symmetric model.
    """
    yp = y_player.astype(np.int64)
    return {float(L): (yp > L).astype(np.int64) for L in lines}


def derive_labels(
    target: Target,
    y_a: np.ndarray,
    y_b: np.ndarray,
    lines: list[float],
) -> dict[float, np.ndarray]:
    """Dispatch to the per-target label function."""
    if target == "total":
        return total_labels(y_a, y_b, lines)
    if target == "spread":
        return spread_labels(y_a, y_b, lines)
    if target == "player_games":
        return player_games_labels(y_a, lines)
    raise ValueError(f"Unknown target: {target!r}; expected one of {TARGET_NAMES}")
