"""Serve point win probability estimators for the IID tennis projector.

A `ServeWinProbEstimator` takes a polars DataFrame of matches (one row per
match, with both players' features) and returns a per-match estimate of each
player's serve point win probability for the matchup. These probabilities are
the fundamental input to the IID chain in `mvp.projection.iid.chain` — from
them, hold-per-game and tiebreak-game-win probabilities follow analytically.
"""

from abc import ABC, abstractmethod
from typing import Any, Final, Literal

import numpy as np
import polars as pl

from mvp.projection.models import get_regression_model


# Default clip range for serve point win prob. ATP serve point win rates
# essentially never fall outside [0.30, 0.90]; clipping protects the
# downstream lookup tables and signals upstream feature bugs if hit often.
SERVE_PROB_MIN: Final[float] = 0.30
SERVE_PROB_MAX: Final[float] = 0.90

# League-mean fallback for missing values. Loosely calibrated to ATP tour /
# challenger pooled serve point win rate.
LEAGUE_MEAN_SERVE_PROB: Final[float] = 0.62


class ServeWinProbEstimator(ABC):
    """Predicts each player's serve point win probability per matchup."""

    @abstractmethod
    def fit(self, df: pl.DataFrame) -> None:
        """Fit any internal parameters from training data. May be a no-op."""

    @abstractmethod
    def predict(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Predict (p_a, p_b) per row. Returns two 1-D float64 arrays."""

    @property
    @abstractmethod
    def required_columns(self) -> list[str]:
        """Columns the estimator needs in the input DataFrame."""


class IdentityServeModel(ServeWinProbEstimator):
    """Pass-through baseline: rolling pts_service_won_pct per side, no learning.

    The chain alone does the work. Outputs are clipped to [clip_min, clip_max]
    and missing values are filled with the league-mean serve rate.

    Args:
        window: Window size in days (e.g. 90 → uses
            `player_pts_service_won_pct_90d`). `None` uses the all-time mean
            feature `player_pts_service_won_pct`.
        clip_min: Lower bound applied before returning. Defaults to SERVE_PROB_MIN.
        clip_max: Upper bound applied before returning. Defaults to SERVE_PROB_MAX.
    """

    def __init__(
        self,
        window: int | None = 90,
        clip_min: float = SERVE_PROB_MIN,
        clip_max: float = SERVE_PROB_MAX,
    ) -> None:
        self.window = window
        self.clip_min = clip_min
        self.clip_max = clip_max
        suffix = f"_{window}d" if window is not None else ""
        self._player_col = f"player_pts_service_won_pct{suffix}"
        self._opp_col = f"opp_pts_service_won_pct{suffix}"

    @property
    def required_columns(self) -> list[str]:
        return [self._player_col, self._opp_col]

    def fit(self, df: pl.DataFrame) -> None:
        return None

    def predict(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        p_a = df[self._player_col].to_numpy().astype(np.float64)
        p_b = df[self._opp_col].to_numpy().astype(np.float64)
        p_a = np.where(np.isnan(p_a), LEAGUE_MEAN_SERVE_PROB, p_a)
        p_b = np.where(np.isnan(p_b), LEAGUE_MEAN_SERVE_PROB, p_b)
        p_a = np.clip(p_a, self.clip_min, self.clip_max)
        p_b = np.clip(p_b, self.clip_min, self.clip_max)
        return p_a, p_b


class MatchupServeModel(ServeWinProbEstimator):
    """Predicts each player's serve point win prob for THIS matchup.

    The IID chain expects per-matchup serve win probabilities, not historical
    averages. This model fits a regression that maps (player rolling stats,
    opponent rolling stats, ...) to the per-match serve win pct that the
    player ACTUALLY achieved. At prediction time it is called twice per match,
    once from player A's perspective and once from player B's perspective via
    the `player_*` ↔ `opp_*` column swap, producing (p_a, p_b) directly.

    Training uses both perspectives of each match (each match contributes two
    training rows: player A's features → A's actual serve rate, and the
    swapped features → B's actual serve rate). This doubles the training set
    and makes the model symmetric by construction.

    The feature_columns list specifies the features as the player-perspective
    columns (typically `player_*`-prefixed). For each entry the model derives
    the swapped column name by replacing `player_` with `opp_` (or vice versa);
    columns without a prefix are treated as match-level and used unchanged.
    All swapped columns must exist in the input DataFrame, which means the
    config's `features.include` should request both `player_*` and `opp_*`
    versions of every mirrored feature.

    Args:
        feature_columns: Player-perspective feature column names. The opp
            versions are derived by prefix swap and must also exist in the
            input DataFrame.
        match_level_columns: Optional list of match-level columns (e.g.
            `best_of`, surface dummies) that are not perspective-dependent
            and are used as-is for both perspectives.
        regressor_type: "ridge" or "linear" — uses
            `mvp.projection.models.get_regression_model`.
        regressor_params: Sklearn kwargs. Defaults to ridge with alpha=1.0.
        clip_min, clip_max: Final clip applied to predicted serve rates.
    """

    def __init__(
        self,
        feature_columns: list[str],
        match_level_columns: list[str] | None = None,
        regressor_type: Literal["ridge", "linear"] = "ridge",
        regressor_params: dict[str, Any] | None = None,
        clip_min: float = SERVE_PROB_MIN,
        clip_max: float = SERVE_PROB_MAX,
    ) -> None:
        if not feature_columns:
            raise ValueError("feature_columns must be non-empty")
        self.feature_columns = list(feature_columns)
        self.match_level_columns = list(match_level_columns or [])
        self.regressor_type = regressor_type
        self.regressor_params = dict(regressor_params or {})
        self.clip_min = clip_min
        self.clip_max = clip_max
        self._model = None
        self._mean: np.ndarray | None = None
        self._std: np.ndarray | None = None

    @staticmethod
    def _swap_perspective(col: str) -> str:
        if col.startswith("player_"):
            return "opp_" + col[len("player_"):]
        if col.startswith("opp_"):
            return "player_" + col[len("opp_"):]
        return col  # match-level — no swap

    @property
    def required_columns(self) -> list[str]:
        cols: set[str] = set(self.match_level_columns)
        for c in self.feature_columns:
            cols.add(c)
            swapped = self._swap_perspective(c)
            cols.add(swapped)
        # Raw target columns for both perspectives. The parquet stores the
        # row's player perspective UNPREFIXED and the opp perspective with
        # an opp_ prefix.
        cols.update(
            [
                "pts_service_pts_won",
                "pts_service_pts_played",
                "opp_pts_service_pts_won",
                "opp_pts_service_pts_played",
            ]
        )
        return sorted(cols)

    def _build_X(self, df: pl.DataFrame, *, swap: bool) -> np.ndarray:
        cols = []
        for c in self.feature_columns:
            effective = self._swap_perspective(c) if swap else c
            cols.append(df[effective].to_numpy().astype(np.float64))
        for c in self.match_level_columns:
            cols.append(df[c].to_numpy().astype(np.float64))
        return np.column_stack(cols)

    def _actual_serve_rate(self, df: pl.DataFrame, *, swap: bool) -> np.ndarray:
        # Player perspective: parquet stores it unprefixed.
        # Opp perspective: parquet stores it with opp_ prefix.
        prefix = "opp_" if swap else ""
        won = df[f"{prefix}pts_service_pts_won"].to_numpy().astype(np.float64)
        played = df[f"{prefix}pts_service_pts_played"].to_numpy().astype(np.float64)
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.where(played > 0, won / played, np.nan)

    def fit(self, df: pl.DataFrame) -> None:
        # Build training data from BOTH perspectives — each match contributes
        # 2 rows, one per player.
        X_player = self._build_X(df, swap=False)
        y_player = self._actual_serve_rate(df, swap=False)
        X_opp = self._build_X(df, swap=True)
        y_opp = self._actual_serve_rate(df, swap=True)

        X_full = np.vstack([X_player, X_opp])
        y_full = np.concatenate([y_player, y_opp])

        valid = np.isfinite(y_full) & np.isfinite(X_full).all(axis=1)
        X_valid = X_full[valid]
        y_valid = y_full[valid]

        if len(X_valid) == 0:
            raise ValueError(
                "MatchupServeModel: no valid training rows after dropping NaNs"
            )

        self._mean = X_valid.mean(axis=0)
        self._std = X_valid.std(axis=0)
        self._std = np.where(self._std == 0, 1.0, self._std)
        X_scaled = (X_valid - self._mean) / self._std

        self._model = get_regression_model(self.regressor_type, dict(self.regressor_params))
        self._model.fit(X_scaled, y_valid)

    def predict(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        if self._model is None or self._mean is None or self._std is None:
            raise RuntimeError("MatchupServeModel.predict called before fit")

        X_a = self._build_X(df, swap=False)
        X_b = self._build_X(df, swap=True)

        # Impute missing feature values with the train mean (post-standardization → 0).
        X_a = np.where(np.isnan(X_a), self._mean, X_a)
        X_b = np.where(np.isnan(X_b), self._mean, X_b)

        X_a_scaled = (X_a - self._mean) / self._std
        X_b_scaled = (X_b - self._mean) / self._std

        p_a = self._model.predict(X_a_scaled)
        p_b = self._model.predict(X_b_scaled)

        p_a = np.clip(p_a, self.clip_min, self.clip_max)
        p_b = np.clip(p_b, self.clip_min, self.clip_max)
        return p_a, p_b
