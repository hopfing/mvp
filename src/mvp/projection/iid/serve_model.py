"""Serve point win probability estimators for the IID tennis projector.

A `ServeWinProbEstimator` takes a polars DataFrame of matches (one row per
match, with both players' features) and returns a per-match estimate of each
player's serve point win probability for the matchup. These probabilities are
the fundamental input to the IID chain in `mvp.projection.iid.chain` — from
them, hold-per-game and tiebreak-game-win probabilities follow analytically.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, Literal

import numpy as np
import polars as pl

from mvp.projection.models import get_regression_model


ServeStateFn = Callable[["ScoreState"], np.ndarray]  # type: ignore[name-defined]


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

    @property
    def is_state_aware(self) -> bool:
        """Whether this estimator's per-point probability varies by ScoreState.

        When False, the projector uses the scalar `chain.match_distribution`
        path (faster). When True, the projector uses
        `stateful_chain.match_distribution_from_state_fn`, which invokes the
        state-fn at every game-state / set-state in the DP.
        """
        return False

    def predict_state_fn(
        self, df: pl.DataFrame,
    ) -> tuple[ServeStateFn, ServeStateFn]:
        """Return (p_a_fn, p_b_fn) callables for the stateful chain.

        Default behavior for scalar models: freeze the scalar `predict()`
        output into state-independent constant functions. Score-state-aware
        models override this to return genuinely state-dependent callables.
        """
        p_a, p_b = self.predict(df)
        p_a_arr = np.asarray(p_a, dtype=np.float64)
        p_b_arr = np.asarray(p_b, dtype=np.float64)

        def p_a_fn(_state: Any) -> np.ndarray:
            return p_a_arr

        def p_b_fn(_state: Any) -> np.ndarray:
            return p_b_arr

        return p_a_fn, p_b_fn


class ScoreStateChainServeModel(ServeWinProbEstimator):
    """Score-state-dependent serve model wired into the stateful IID chain.

    At `fit(df)` time: extracts the training match_uids, loads the point-grain
    rows from `match_beats_points.parquet`, joins server-perspective
    match-level features computed via FeatureEngine, adds derived point
    features, and trains an internal point-grain classifier (logistic or
    xgboost).

    At `predict_state_fn(df)` time: builds per-match feature matrices in both
    server perspectives (A serving, B serving) once, and returns callables
    that combine those match features with per-state point features at each
    evaluation (game state, set state, match state).

    Point-level features are routed by grain:
      - STATE_DERIVABLE: computed from ScoreState at each call (varies by state)
      - match-constant (surface flags, etc.): pulled from the DataFrame once
    """

    # Point-level feature names whose value varies by ScoreState.
    _STATE_DERIVABLE: Final[frozenset[str]] = frozenset(
        {
            "is_break_point", "is_set_point", "is_match_point", "is_tiebreak",
            "is_server_set_point", "is_returner_set_point",
            "is_server_match_point", "is_returner_match_point",
            "set_score_asymmetry", "sets_won_asymmetry",
            "set_score_server_games", "set_score_returner_games",
            "sets_won_server", "sets_won_returner",
            "game_score_numeric_server", "game_score_numeric_returner",
            "game_score_diff",
            "serve", "is_second_serve",
        }
    )

    _GAME_SCORE_NUMERIC: Final[dict[str, int]] = {
        "0": 0, "15": 15, "30": 30, "40": 40, "D": 45, "AD": 50,
    }

    def __init__(
        self,
        model_type: Literal["logistic", "xgboost"],
        match_level_features: list[str],
        point_level_features: list[str],
        params: dict[str, Any] | None = None,
        *,
        points_path: Path | str | None = None,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        engine: Any = None,
        clip_min: float = SERVE_PROB_MIN,
        clip_max: float = SERVE_PROB_MAX,
    ) -> None:
        if not match_level_features and not point_level_features:
            raise ValueError(
                "ScoreStateChainServeModel requires non-empty match_level_features "
                "and/or point_level_features"
            )
        self.model_type = model_type
        self.match_level_features = list(match_level_features)
        self.point_level_features = list(point_level_features)
        self.params = dict(params or {})
        self.clip_min = clip_min
        self.clip_max = clip_max
        # Paths default to the standard data locations; tests can override.
        self._points_path = Path(points_path) if points_path is not None else None
        self._matches_path = Path(matches_path) if matches_path is not None else None
        self._cache_dir = Path(cache_dir) if cache_dir is not None else None
        # Optional pre-built FeatureEngine. Passing one skips building a fresh
        # engine per fit() — the fresh build re-hashes matches.parquet and
        # invalidates the cache if the file is touched mid-run (e.g., by a
        # live pipeline). Callers doing many fits in one process should share.
        self._engine = engine

        self._model: "ScoreStateServeModel | None" = None
        # server_/returner_ column names built from match_level_features specs.
        self._match_feature_cols: list[str] = []
        # Parallel to _match_feature_cols: True for diff-style features
        # (registry mirror=False) whose swapped-perspective value is the
        # negation of the server-side column, rather than a separate opp_
        # column read.
        self._match_feature_is_diff: list[bool] = []

        # Cached per-df state — populated by predict_state_fn().
        self._X_match_A: np.ndarray | None = None   # server=A perspective
        self._X_match_B: np.ndarray | None = None   # server=B perspective
        self._point_constants: dict[str, np.ndarray] = {}

    @property
    def is_state_aware(self) -> bool:
        return True

    @property
    def required_columns(self) -> list[str]:
        # self._match_feature_cols uses server_/returner_; at inference the
        # DataFrame has player_/opp_ — translate both. Diff-style features
        # have no opp_ column; the swap-side value is the negation of the
        # player_ column, so we only require the player_ side.
        cols: set[str] = set()
        for name, is_diff in zip(
            self._match_feature_cols,
            self._match_feature_is_diff or [False] * len(self._match_feature_cols),
        ):
            if name.startswith("server_"):
                cols.add("player_" + name[len("server_"):])
                if not is_diff:
                    cols.add("opp_" + name[len("server_"):])
            elif name.startswith("returner_"):
                cols.add("player_" + name[len("returner_"):])
                if not is_diff:
                    cols.add("opp_" + name[len("returner_"):])
            else:
                cols.add(name)
        for name in self.point_level_features:
            if name not in self._STATE_DERIVABLE:
                cols.add(name)
        cols.add("best_of")
        return sorted(cols)

    def _resolve_match_feature_cols(self) -> list[str]:
        """Map config feature specs to the server_/returner_ column names used
        at inference. `player_*` specs become `server_*`; `opp_*` become
        `returner_*`; unprefixed specs are passed through as match-level.

        Also populates `self._match_feature_is_diff`: True for registered
        diff-style features (mirror=False) — these have only a `player_`
        column in the raw frame, and the swap-side value is the negation
        of the server-side value (handled in `_match_feature_values`).
        """
        from mvp.model.engine import build_column_name, parse_feature_spec
        from mvp.model.registry import get_registry

        registry = get_registry()
        cols: list[str] = []
        is_diff_flags: list[bool] = []
        for spec in self.match_level_features:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            col = build_column_name(full_name, params)
            if col.startswith("player_"):
                col = "server_" + col[len("player_"):]
            elif col.startswith("opp_"):
                col = "returner_" + col[len("opp_"):]
            is_diff = False
            if prefix is not None:
                try:
                    is_diff = not registry.get(base_name).mirror
                except KeyError:
                    is_diff = False
            cols.append(col)
            is_diff_flags.append(is_diff)
        self._match_feature_is_diff = is_diff_flags
        return cols

    def fit(self, df: pl.DataFrame) -> None:
        """Train the point-grain classifier on matches present in `df`.

        `df` is the IID runner's train split (one row per match_uid). Points
        are loaded and filtered to these match_uids; match-level features are
        (re)computed via a cached FeatureEngine call.
        """
        from mvp.common.base_job import get_data_root, get_local_data_root
        from mvp.model.engine import FeatureEngine
        from mvp.projection.iid.score_state_features import (
            DERIVED_POINT_FEATURES,
            add_derived_point_features,
        )
        from mvp.projection.iid.score_state_model import build_score_state_model

        if "match_uid" not in df.columns:
            raise ValueError("ScoreStateChainServeModel.fit: df missing match_uid column")
        train_uids = df["match_uid"].unique().to_list()
        if not train_uids:
            raise ValueError("ScoreStateChainServeModel.fit: empty training df")

        points_path = self._points_path or (
            get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
        )
        matches_path = self._matches_path or (
            get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        )
        cache_dir = self._cache_dir or (
            get_local_data_root() / "features" / "cache"
        )

        points = pl.read_parquet(points_path).filter(
            pl.col("match_uid").is_in(train_uids)
        )
        if len(points) == 0:
            raise ValueError("no points rows matched the training match_uids")

        self._match_feature_cols = self._resolve_match_feature_cols()

        if self.match_level_features:
            engine = self._engine if self._engine is not None else FeatureEngine(
                matches_path=matches_path, cache_dir=cache_dir,
            )
            matches_features = engine.compute(
                feature_specs=self.match_level_features,
                extra_columns=["player_id", "opp_id", "match_uid"],
            )
            matches_features = matches_features.rename(
                {"player_id": "server_id", "opp_id": "returner_id"}
            )
            # Drop any non-key column that already exists in points to avoid
            # `_right` collisions (same pattern as ServeDiscoverySelector
            # ._build_base_matrix). points carries match-grain fields like
            # best_of / surface / round that engine.compute can surface via
            # source-column pruning.
            keys = {"match_uid", "server_id", "returner_id"}
            overlap = (set(points.columns) & set(matches_features.columns)) - keys
            if overlap:
                matches_features = matches_features.drop(list(overlap))
            joined = points.join(
                matches_features,
                on=["match_uid", "server_id", "returner_id"],
                how="inner",
            )
            renames: dict[str, str] = {}
            for c in joined.columns:
                if c.startswith("player_") and c != "player_id":
                    renames[c] = "server_" + c[len("player_"):]
                elif c.startswith("opp_") and c != "opp_id":
                    renames[c] = "returner_" + c[len("opp_"):]
            if renames:
                joined = joined.rename(renames)
        else:
            joined = points

        derived = [n for n in self.point_level_features if n in DERIVED_POINT_FEATURES]
        if derived:
            joined = add_derived_point_features(joined, derived)

        joined = joined.filter(pl.col("point_won_by_server").is_not_null())
        if len(joined) == 0:
            raise ValueError("no valid training points after target filter")

        feature_cols = self._match_feature_cols + self.point_level_features
        X = joined.select(feature_cols).to_numpy()
        y = joined["point_won_by_server"].cast(pl.Int64).to_numpy()

        self._model = build_score_state_model(
            type_=self.model_type,
            feature_names=feature_cols,
            params=self.params,
            match_feature_names=self._match_feature_cols,
            point_feature_names=self.point_level_features,
        )
        self._model.fit(X, y)

    def _match_feature_values(self, df: pl.DataFrame, *, swap: bool) -> np.ndarray:
        """Build the match-level feature matrix in server-perspective.

        `swap=False` → player A is server; `swap=True` → player B is server
        (columns read via the player_/opp_ swap). Diff-style features
        (mirror=False) have no opp_ counterpart; the swap-side value is the
        negation of the player_ column.
        """
        cols: list[np.ndarray] = []
        is_diff_flags = self._match_feature_is_diff or [False] * len(self._match_feature_cols)
        for name, is_diff in zip(self._match_feature_cols, is_diff_flags):
            sign = 1.0
            if name.startswith("server_"):
                if is_diff:
                    col = "player_" + name[len("server_"):]
                    if swap:
                        sign = -1.0
                else:
                    src_prefix = "opp_" if swap else "player_"
                    col = src_prefix + name[len("server_"):]
            elif name.startswith("returner_"):
                if is_diff:
                    # Only the player_ diff column exists in the df.
                    # returner = non-server: equals -player_diff when A serves,
                    # +player_diff when B serves (swap=True).
                    col = "player_" + name[len("returner_"):]
                    sign = 1.0 if swap else -1.0
                else:
                    src_prefix = "player_" if swap else "opp_"
                    col = src_prefix + name[len("returner_"):]
            else:
                col = name
            arr = df[col].to_numpy().astype(np.float64)
            if sign != 1.0:
                arr = arr * sign
            cols.append(arr)
        if not cols:
            return np.zeros((len(df), 0), dtype=np.float64)
        return np.column_stack(cols)

    def _point_constant_values(self, df: pl.DataFrame) -> dict[str, np.ndarray]:
        """Broadcast-constant point features (surface flags, etc.) from df."""
        out: dict[str, np.ndarray] = {}
        for name in self.point_level_features:
            if name in self._STATE_DERIVABLE:
                continue
            if name not in df.columns:
                raise KeyError(
                    f"ScoreStateChainServeModel: required match-constant point "
                    f"feature '{name}' not in df"
                )
            out[name] = df[name].to_numpy().astype(np.float64)
        return out

    def _state_derivable_values(self, state: Any) -> dict[str, float]:
        """Per-call values for STATE_DERIVABLE features, given a ScoreState."""
        values: dict[str, float] = {
            "is_break_point": float(state.is_break_point()),
            "is_set_point": float(state.is_set_point()),
            "is_match_point": float(state.is_match_point()),
            "is_tiebreak": float(state.is_tiebreak),
            "is_server_set_point": float(state.is_server_set_point()),
            "is_returner_set_point": float(state.is_returner_set_point()),
            "is_server_match_point": float(state.is_server_match_point()),
            "is_returner_match_point": float(state.is_returner_match_point()),
            "set_score_asymmetry": float(state.set_score_asymmetry()),
            "sets_won_asymmetry": float(state.sets_won_asymmetry()),
            "set_score_server_games": float(state.set_score_server_games),
            "set_score_returner_games": float(state.set_score_returner_games),
            "sets_won_server": float(state.sets_won_server),
            "sets_won_returner": float(state.sets_won_returner),
            "serve": float(state.serve_num),
            "is_second_serve": float(state.serve_num == 2),
        }
        gs_s = self._GAME_SCORE_NUMERIC.get(state.game_score_server, 0)
        gs_r = self._GAME_SCORE_NUMERIC.get(state.game_score_returner, 0)
        values["game_score_numeric_server"] = float(gs_s)
        values["game_score_numeric_returner"] = float(gs_r)
        values["game_score_diff"] = float(gs_s - gs_r)
        return values

    def predict(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Scalar prediction evaluated at a neutral opening state.

        Used by the projector as the `p_a_avg / p_b_avg` input to the stateful
        chain's tiebreak approximation.
        """
        from mvp.projection.iid.score_state import ScoreState  # local import

        p_a_fn, p_b_fn = self.predict_state_fn(df)
        neutral = ScoreState(
            serve_num=1,
            game_score_server="0", game_score_returner="0",
            is_tiebreak=False,
            set_score_server_games=0, set_score_returner_games=0,
            sets_won_server=0, sets_won_returner=0,
            best_of=3,
        )
        return p_a_fn(neutral), p_b_fn(neutral)

    def predict_state_fn(
        self, df: pl.DataFrame,
    ) -> tuple[ServeStateFn, ServeStateFn]:
        """Build state-aware callables. Match-level features are cached once."""
        if self._model is None:
            raise RuntimeError(
                "ScoreStateChainServeModel.predict_state_fn called before fit"
            )
        self._X_match_A = self._match_feature_values(df, swap=False)
        self._X_match_B = self._match_feature_values(df, swap=True)
        self._point_constants = self._point_constant_values(df)
        n = len(df)

        def _X_for(X_match: np.ndarray, state: Any) -> np.ndarray:
            state_vals = self._state_derivable_values(state)
            point_cols: list[np.ndarray] = []
            for name in self.point_level_features:
                if name in self._STATE_DERIVABLE:
                    point_cols.append(np.full(n, state_vals[name], dtype=np.float64))
                else:
                    point_cols.append(self._point_constants[name])
            if point_cols:
                X_point = np.column_stack(point_cols)
                return np.hstack([X_match, X_point])
            return X_match

        def p_a_fn(state: Any) -> np.ndarray:
            X = _X_for(self._X_match_A, state)  # type: ignore[arg-type]
            p = self._model.predict_proba(X)  # type: ignore[union-attr]
            return np.clip(p, self.clip_min, self.clip_max)

        def p_b_fn(state: Any) -> np.ndarray:
            X = _X_for(self._X_match_B, state)  # type: ignore[arg-type]
            p = self._model.predict_proba(X)  # type: ignore[union-attr]
            return np.clip(p, self.clip_min, self.clip_max)

        return p_a_fn, p_b_fn


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
