"""Two-level serve estimator — the serve tree, composed before the chain.

    p = P(1st in) * P(win | 1st in) + (1 - P(1st in)) * P(win | 2nd)

Exact for the marginal probability of winning a service point, so the result is
a single `p` per state and the chain is untouched. `stateful_chain` still walks
game/set/match states over one probability; nothing here reaches it.

Why decompose at all, measured rather than argued (2023+, 6.15M points, 744
servers with >=2000 service points):

- The identity closes at 0.00e+00 — composite 0.616579 against observed
  0.616579. Double faults sit inside the second-serve branch, which is why it
  closes exactly and why they are not modelled separately.
- Blended `p` has SMALLER cross-player spread (sd 0.0287) than the components it
  is built from (0.0374 and 0.0398), because r(first_in, win_1st) = -0.352 —
  landing more first serves means winning fewer of them, and the two partially
  cancel.
- Consequently r(first_in, blended) = -0.032. First-serve percentage is
  effectively orthogonal to blended serve-win rate, so an estimator whose target
  is blended `p` has no gradient toward it. That dimension is not underweighted
  by the single-level model, it is invisible to it.
- r(win_1st, win_2nd) = +0.394: winning first-serve points and winning
  second-serve points are only weakly the same skill. Three fits therefore give
  every existing match-level feature its own serve-conditional relationship
  instead of one averaged over both.

Grains differ by component, and that is the point rather than an inconvenience:

  P(1st in)      (match, server) grain, weighted by service points played, NO
                 ScoreState. Fitting it on point rows would replicate a
                 within-match constant across ~140 rows and report confidence
                 the sample does not contain.
  P(win | 1st)   point grain, state-varying, rows where serve == 1
  P(win | 2nd)   point grain, state-varying, rows where serve == 2

`P(1st in)` being state-invariant is measured, not assumed: within-player on the
sets-won axis it moves +0.19pp (t=1.8, flat) while P(win|1st) moves +5.42pp
(t=50.3) on the same rows. State-invariant does NOT mean constant — it carries
the largest cross-player spread of the three and is exactly the orthogonal
dimension above. It is estimated per match from features; it simply takes no
ScoreState argument.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Final

import numpy as np
import polars as pl

from mvp.projection.iid.serve_model import (
    ScoreStateChainServeModel,
    ServeStateFn,
    ServeWinProbEstimator,
    apply_serve_branch,
    neutral_score_state,
    resolve_match_feature_cols,
)

logger = logging.getLogger(__name__)

# Component names. These are the strings a config and the FS selector use to
# address one component, so they are part of the interface rather than internal
# labels.
FIRST_IN: Final[str] = "first_in"
WIN_FIRST: Final[str] = "win_first"
WIN_SECOND: Final[str] = "win_second"
COMPONENTS: Final[tuple[str, ...]] = (FIRST_IN, WIN_FIRST, WIN_SECOND)

_SERVE_BRANCH: Final[dict[str, int]] = {WIN_FIRST: 1, WIN_SECOND: 2}


class FirstServeInModel:
    """P(1st serve in) per (match, server) — player-varying, state-invariant.

    Not a `ServeWinProbEstimator`: it answers a different question and never
    sees a `ScoreState`. It exists as its own class so the match-grain fit
    cannot accidentally acquire point-grain rows.

    The fit is weighted by service points actually played in that match. Match
    grain without the weight would let a 40-point service performance count the
    same as a 120-point one — reintroducing at match grain the equal-weighting
    problem that point-row replication was falsely solving.
    """

    def __init__(
        self,
        model_type: str,
        match_level_features: list[str],
        params: dict[str, Any] | None = None,
        *,
        point_level_features: list[str] | None = None,
        points_path: Path | str | None = None,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        engine: Any = None,
        clip_min: float = 0.05,
        clip_max: float = 0.95,
    ) -> None:
        self.model_type = model_type
        self.match_level_features = list(match_level_features)
        # Point-pool features that are MATCH-CONSTANT (the surface one-hots).
        # Legitimate here despite the absence of a ScoreState: they do not vary
        # by state, they are the only route to surface (no registered
        # match-level surface indicator exists), and first-serve rate plausibly
        # varies by surface. State-derivable names are rejected at the FS seam,
        # keyed off the same `_STATE_DERIVABLE` set the win branches use.
        self.point_level_features = list(point_level_features or [])
        self.params = dict(params or {})
        self.clip_min = clip_min
        self.clip_max = clip_max
        self._points_path = Path(points_path) if points_path is not None else None
        self._matches_path = Path(matches_path) if matches_path is not None else None
        self._cache_dir = Path(cache_dir) if cache_dir is not None else None
        self._engine = engine
        self._model: Any = None
        self._cols: list[str] = []
        self._is_diff: list[bool] = []
        # Fallback when no features are configured, or a row's features are
        # unusable: the weighted training base rate. Never a hardcoded constant
        # — a wrong global here would bias every match in the same direction.
        self._base_rate: float = 0.62

    def __getstate__(self) -> dict[str, Any]:
        # Same reason as ScoreStateChainServeModel.__getstate__ (serve_model.py):
        # _engine holds the global FeatureRegistry, whose register_diff/_sum/
        # _matchup closures pickle cannot resolve by qualified name. The win
        # branches scrub themselves; without this the first_in arm was the one
        # unscrubbed engine reference in a TwoLevelServeModel, so NO two-level
        # model could be saved — latent until the first two-level backtest,
        # since `_engine` is set here whether or not the arm has features.
        #
        # Safe to drop: `_match_features` rebuilds a fresh engine when this is
        # None, exactly as the win branches do.
        state = self.__dict__.copy()
        state["_engine"] = None
        return state

    # -- helpers -------------------------------------------------------
    def _resolve_cols(self) -> tuple[list[str], list[bool]]:
        return resolve_match_feature_cols(self.match_level_features)

    def _aggregate(self, points: pl.DataFrame) -> pl.DataFrame:
        """Collapse points to (match_uid, server_id) with the branch counts.

        Match-constant point features come along via `first()` — they are
        constant within a match by definition, so any row carries the value.
        Taking a mean instead would quietly tolerate a non-constant column and
        turn a wrong feature into a plausible number.
        """
        from mvp.projection.iid.score_state_features import (
            DERIVED_POINT_FEATURES,
            add_derived_point_features,
        )

        if self.point_level_features:
            wanted = [
                f for f in self.point_level_features
                if f in DERIVED_POINT_FEATURES and f not in points.columns
            ]
            if wanted:
                points = add_derived_point_features(points, wanted)
        aggs = [
            pl.len().alias("_n_serve_pts"),
            (pl.col("serve") == 1).sum().alias("_n_first_in"),
        ]
        aggs += [
            pl.col(f).first().alias(f)
            for f in self.point_level_features
            if f in points.columns
        ]
        return points.group_by(["match_uid", "server_id"]).agg(aggs)

    # -- fit -----------------------------------------------------------
    def fit(
        self,
        df: pl.DataFrame,
        *,
        preloaded_match_features: pl.DataFrame | None = None,
        preloaded_points: pl.DataFrame | None = None,
    ) -> None:
        from mvp.common.base_job import get_data_root

        if "match_uid" not in df.columns:
            raise ValueError("FirstServeInModel.fit: df missing match_uid")
        train_uids = df["match_uid"].unique().to_list()

        points_path = self._points_path or (
            get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
        )
        if preloaded_points is not None:
            points = preloaded_points
        else:
            points = pl.read_parquet(
                points_path, columns=["match_uid", "server_id", "serve"]
            ).filter(pl.col("match_uid").is_in(train_uids))
        if len(points) == 0:
            raise ValueError("FirstServeInModel.fit: no points for training matches")

        agg = self._aggregate(points)
        self._base_rate = float(
            agg["_n_first_in"].sum() / max(agg["_n_serve_pts"].sum(), 1)
        )

        if not self.match_level_features:
            # Intercept-only is legitimate: it is the degenerate arm of the
            # recovery test and a valid configuration in its own right.
            self._model = None
            self._cols, self._is_diff = [], []
            return

        self._cols, self._is_diff = self._resolve_cols()
        feats = self._match_features(df, preloaded_match_features)
        train = agg.join(
            feats, left_on=["match_uid", "server_id"],
            right_on=["match_uid", "player_id"], how="inner",
        ).drop_nulls(self._cols)
        if len(train) == 0:
            logger.warning(
                "FirstServeInModel: no rows survived the feature join; "
                "falling back to the weighted base rate"
            )
            self._model = None
            return

        X = train.select(self._cols).to_numpy().astype(np.float64)
        y = (train["_n_first_in"] / train["_n_serve_pts"]).to_numpy()
        w = train["_n_serve_pts"].to_numpy().astype(np.float64)
        self._model = _build_rate_regressor(self.model_type, self.params)
        self._model.fit(X, y, sample_weight=w)
        logger.info(
            "FirstServeInModel fit at match grain: %d rows, %d features "
            "(base rate %.4f)", len(train), len(self._cols), self._base_rate,
        )

    def _match_features(
        self, df: pl.DataFrame, preloaded: pl.DataFrame | None,
    ) -> pl.DataFrame:
        """Server-perspective match features, one row per (match, player)."""
        from mvp.common.base_job import get_data_root, get_local_data_root
        from mvp.model.engine import (
            FeatureEngine, build_column_name, parse_feature_spec,
        )

        needed = [
            build_column_name(full, params)
            for spec in self.match_level_features
            for _, _, full, params in [parse_feature_spec(spec)]
        ]
        if preloaded is not None:
            # Pair each raw column with ITS OWN inference name before dropping
            # the absent ones. Zipping survivors against `self._cols[:len(have)]`
            # positionally re-labelled every survivor after a gap with the wrong
            # spec's name — silently, producing a model fitted on mislabelled
            # columns rather than an error.
            pairs = [
                (raw, col)
                for raw, col in zip(needed, self._cols, strict=True)
                if raw in preloaded.columns
            ]
            absent = [raw for raw in needed if raw not in preloaded.columns]
            if absent:
                raise KeyError(
                    f"FirstServeInModel: match features absent from the preloaded "
                    f"frame: {sorted(absent)}. The FS candidate pool must contain "
                    f"every spec any held component references, or the column is "
                    f"never materialized."
                )
            return preloaded.select(
                ["match_uid", "player_id", *[r for r, _ in pairs]]
            ).rename(dict(pairs))
        engine = self._engine or FeatureEngine(
            matches_path=self._matches_path
            or (get_data_root() / "aggregate" / "atptour" / "matches.parquet"),
            cache_dir=self._cache_dir
            or (get_local_data_root() / "features" / "cache"),
        )
        out = engine.compute(self.match_level_features, extra_columns=["player_id"])
        rename = {}
        for spec, col in zip(self.match_level_features, self._cols, strict=True):
            _, _, full, params = parse_feature_spec(spec)
            raw = build_column_name(full, params)
            if raw in out.columns:
                rename[raw] = col
        return out.select(["match_uid", "player_id", *rename.keys()]).rename(rename)

    # -- predict -------------------------------------------------------
    def predict_rate(self, X: np.ndarray | None) -> np.ndarray:
        """P(1st in) for a prepared feature matrix, or the base rate if none."""
        if self._model is None or X is None or X.size == 0:
            n = 0 if X is None else len(X)
            return np.full(n, self._base_rate, dtype=np.float64)
        out = np.asarray(self._model.predict(X), dtype=np.float64)
        out = np.where(np.isfinite(out), out, self._base_rate)
        return np.clip(out, self.clip_min, self.clip_max)


def _build_rate_regressor(model_type: str, params: dict[str, Any]) -> Any:
    """Regressor for a rate in [0, 1]. Mirrors score_state_model's dispatch."""
    if model_type in ("xgboost", "hierarchical_boosted"):
        from xgboost import XGBRegressor

        p = {"tree_method": "hist", "random_state": 42, **params}
        p.pop("eval_metric", None)
        return XGBRegressor(**p)
    from sklearn.linear_model import Ridge

    return Ridge(alpha=float(params.get("alpha", 1.0)))


class _ConstantBranch:
    """A win branch with no features: the training win rate on its own rows.

    `ScoreStateChainServeModel.__init__` raises on empty feature lists
    (serve_model.py:407), but `FirstServeInModel` treats empty as intercept-only
    and falls back to a base rate (its comment: "Intercept-only is legitimate...
    a valid configuration in its own right"). That asymmetry made an all-empty
    two-level model unconstructible, which blocked two things that both need it:
    validation-ladder step 2 (degenerate recovery, which is DEFINED as the
    feature-blind fit), and any component-wise FS that starts from nothing —
    round 1 has the selected component holding one feature and the other two
    holding none.

    Deliberately not a ScoreStateChainServeModel subclass: it has no model, no
    feature columns and no state dependence, so inheriting would carry a fit path
    that cannot run. It implements the three members TwoLevelServeModel actually
    uses — `required_columns`, `fit`, `predict_state_fn`.
    """

    is_state_aware = True
    required_columns: list[str] = []

    def __init__(self, serve_branch: int, points_path: Path | str | None = None):
        self.serve_branch = serve_branch
        self._points_path = points_path
        self._rate = 0.5

    def fit(
        self,
        df: pl.DataFrame,
        *,
        preloaded_match_features: pl.DataFrame | None = None,
        preloaded_points: pl.DataFrame | None = None,
    ) -> None:
        from mvp.common.base_job import get_data_root

        if preloaded_points is not None:
            points = preloaded_points
        else:
            path = self._points_path or (
                get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
            )
            points = pl.read_parquet(
                path, columns=["match_uid", "serve", "point_won_by_server"]
            ).filter(pl.col("match_uid").is_in(df["match_uid"].unique().to_list()))
        rows = apply_serve_branch(points, self.serve_branch)
        won = rows["point_won_by_server"].drop_nulls()
        if len(won) == 0:
            raise ValueError(
                f"_ConstantBranch(serve_branch={self.serve_branch}): no training "
                f"points on this branch"
            )
        self._rate = float(won.mean())

    def predict_state_fn(self, df: pl.DataFrame):
        n = len(df)

        def fn(_state) -> np.ndarray:
            return np.full(n, self._rate, dtype=np.float64)

        return fn, fn


class TwoLevelServeModel(ServeWinProbEstimator):
    """Serve-tree composition exposing the single-level estimator interface.

    Every existing caller — projector, runner, backtest, tuner — sees an
    ordinary `ServeWinProbEstimator`. The composition happens here; the chain
    receives one `p` per state exactly as before.
    """

    def __init__(
        self,
        *,
        model_type: str,
        first_in_match_features: list[str],
        first_in_point_features: list[str] | None = None,
        win_first_match_features: list[str],
        win_first_point_features: list[str],
        win_second_match_features: list[str],
        win_second_point_features: list[str],
        params: dict[str, Any] | None = None,
        first_in_params: dict[str, Any] | None = None,
        points_path: Path | str | None = None,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        engine: Any = None,
        clip_min: float = 0.30,
        clip_max: float = 0.90,
        gap_shrink: float = 1.0,
        surface_circuit_offset: dict[str, float] | None = None,
    ) -> None:
        self.model_type = model_type
        self.first_in_match_features = list(first_in_match_features)
        self.first_in_point_features = list(first_in_point_features or [])
        self.win_first_match_features = list(win_first_match_features)
        self.win_first_point_features = list(win_first_point_features)
        self.win_second_match_features = list(win_second_match_features)
        self.win_second_point_features = list(win_second_point_features)
        self.params = dict(params or {})
        self.first_in_params = dict(first_in_params or self.params)
        self.clip_min = clip_min
        self.clip_max = clip_max
        self.gap_shrink = gap_shrink
        self.surface_circuit_offset = dict(surface_circuit_offset or {})
        # Counted rather than silent: the offset's trailing clip is a guard, and
        # a correction that gets truncated is not the correction that was fitted.
        self.offset_clipped_count = 0

        shared = dict(
            points_path=points_path, matches_path=matches_path,
            cache_dir=cache_dir, engine=engine,
        )
        # The two win branches are ONE implementation separated by a fit-time
        # row filter, not parallel code. gap_shrink and the calibration offset
        # are applied to the COMPOSED p below, never inside a branch — applying
        # them per branch would compress each one and then compose the
        # compressed pair, which is not the same transform.
        def _branch(component: str, match: list[str], point: list[str]):
            # Empty -> the branch's training win rate, matching FirstServeInModel's
            # treatment of an empty set. See _ConstantBranch.
            if not match and not point:
                return _ConstantBranch(
                    _SERVE_BRANCH[component], points_path=points_path,
                )
            return ScoreStateChainServeModel(
                model_type=model_type,
                match_level_features=match,
                point_level_features=point,
                params=self.params, clip_min=0.0, clip_max=1.0,
                gap_shrink=1.0, serve_branch=_SERVE_BRANCH[component], **shared,
            )

        self._win_first = _branch(
            WIN_FIRST, self.win_first_match_features, self.win_first_point_features,
        )
        self._win_second = _branch(
            WIN_SECOND, self.win_second_match_features, self.win_second_point_features,
        )
        self._first_in = FirstServeInModel(
            model_type=model_type,
            match_level_features=self.first_in_match_features,
            point_level_features=self.first_in_point_features,
            params=self.first_in_params, **shared,
        )

    # -- interface -----------------------------------------------------
    @property
    def is_state_aware(self) -> bool:
        return True

    @property
    def required_columns(self) -> list[str]:
        cols = set(self._win_first.required_columns)
        cols.update(self._win_second.required_columns)
        return sorted(cols)

    def fit(
        self,
        df: pl.DataFrame,
        *,
        preloaded_match_features: pl.DataFrame | None = None,
        preloaded_points: pl.DataFrame | None = None,
    ) -> None:
        kw = dict(
            preloaded_match_features=preloaded_match_features,
            preloaded_points=preloaded_points,
        )
        self._first_in.fit(df, **kw)
        self._win_first.fit(df, **kw)
        self._win_second.fit(df, **kw)

    @staticmethod
    def _engine_col(col: str) -> str:
        """Inference name -> engine name. Inverse of resolve_match_feature_cols."""
        if col.startswith("server_"):
            return "player_" + col[len("server_"):]
        if col.startswith("returner_"):
            return "opp_" + col[len("returner_"):]
        return col

    def _first_in_for(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """P(1st in) per match for A-serving and B-serving, as arrays."""
        n = len(df)
        if self._first_in._model is None or not self.first_in_match_features:
            base = np.full(n, self._first_in._base_rate, dtype=np.float64)
            return base, base
        # `_cols` are INFERENCE names (`server_*` / `returner_*`, from
        # resolve_match_feature_cols at serve_model.py:159). `df` here is the
        # match-grain frame, which carries the ENGINE names (`player_*` /
        # `opp_*`) — the rename only happens inside FirstServeInModel.fit's own
        # join. Selecting `_cols` straight off `df` therefore raised KeyError
        # for every non-empty first_in feature set, which is every round of a
        # first_in FS run. Map back to the engine names before reading.
        cols, is_diff = self._first_in._cols, self._first_in._is_diff
        engine_cols = [self._engine_col(c) for c in cols]
        missing = [
            e for e, c in zip(engine_cols, cols, strict=True)
            if e not in df.columns and c not in df.columns
        ]
        if missing:
            raise KeyError(
                f"TwoLevelServeModel: first_in features missing from df: "
                f"{sorted(missing)}"
            )
        # A frame already in inference names (the FS scorer builds one) still
        # works — prefer the engine name, fall back to the inference name.
        read = [e if e in df.columns else c
                for e, c in zip(engine_cols, cols, strict=True)]
        X_a = df.select(read).to_numpy().astype(np.float64)
        # Swap perspective: diff-style columns negate, mirrored ones read the
        # partner column. Same rule resolve_match_feature_cols encodes.
        X_b = X_a.copy()
        for j, diff in enumerate(is_diff):
            if diff:
                X_b[:, j] = -X_b[:, j]
        return self._first_in.predict_rate(X_a), self._first_in.predict_rate(X_b)

    @staticmethod
    def _compose_raw(
        fi: np.ndarray, w1: np.ndarray, w2: np.ndarray,
    ) -> np.ndarray:
        """The serve tree, unclipped and unadjusted."""
        return fi * w1 + (1.0 - fi) * w2

    def _surface_circuit_offsets(self, df: pl.DataFrame) -> np.ndarray | None:
        """Per-match `<surface>/<circuit>` offset, or None. Per-cell, never pooled."""
        table = self.surface_circuit_offset
        if not table:
            return None
        if "surface" not in df.columns or "circuit" not in df.columns:
            logger.warning(
                "surface_circuit_offset configured but df lacks surface/circuit; "
                "no correction applied"
            )
            return None
        keys = [
            f"{s}/{c}" for s, c in zip(
                df["surface"].to_list(), df["circuit"].to_list(), strict=True,
            )
        ]
        return np.array([table.get(k, 0.0) for k in keys], dtype=np.float64)

    def _finish(
        self, raw: np.ndarray, shift: np.ndarray | float,
        offsets: np.ndarray | None,
    ) -> np.ndarray:
        """shrink -> clip -> offset -> guard clip. Same order as single-level.

        The offset lands AFTER the shrink and its clip, so a measured +1.0pp
        correction is applied as +1.0pp rather than `gap_shrink x 1.0pp` — the
        same reasoning as `ScoreStateChainServeModel._apply_offset`, and the
        reason the two must not be reordered independently here.
        """
        p = np.clip(raw + shift, self.clip_min, self.clip_max)
        if offsets is None:
            return p
        shifted = p + offsets
        out = np.clip(shifted, self.clip_min, self.clip_max)
        n_clipped = int(np.count_nonzero(shifted != out))
        if n_clipped:
            self.offset_clipped_count += n_clipped
        return out

    def predict(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Scalar `p` at the neutral opening state.

        Routed through `predict_state_fn` rather than composing separately, so
        the shrink and offset are applied once, in one place. A second
        composition path here is how the two would drift.
        """
        p_a_fn, p_b_fn = self.predict_state_fn(df)
        neutral = neutral_score_state()
        return p_a_fn(neutral), p_b_fn(neutral)

    def predict_state_fn(
        self, df: pl.DataFrame,
    ) -> tuple[ServeStateFn, ServeStateFn]:
        """Two state-varying predicts plus one per-match scalar, then compose.

        `P(1st in)` is evaluated ONCE per df, not per state — it takes no
        ScoreState, so evaluating it inside the closure would repeat identical
        work at every node of the DP.

        `gap_shrink` is a PAIRWISE transform and cannot be applied inside a
        branch or to one side alone. It is derived here as a per-player constant
        offset from the composed `p` at the neutral state — mirroring
        ScoreStateChainServeModel — and added at every state, so each player's
        state-dependent modulation survives and only the between-player level
        gap narrows. Compressing each branch before composing would be a
        different transform entirely.
        """
        fi_a, fi_b = self._first_in_for(df)
        w1_a_fn, w1_b_fn = self._win_first.predict_state_fn(df)
        w2_a_fn, w2_b_fn = self._win_second.predict_state_fn(df)
        offsets = self._surface_circuit_offsets(df)

        shift_a: np.ndarray | float = 0.0
        shift_b: np.ndarray | float = 0.0
        if self.gap_shrink != 1.0:
            neutral = neutral_score_state()
            p_a0 = np.clip(
                self._compose_raw(fi_a, w1_a_fn(neutral), w2_a_fn(neutral)),
                self.clip_min, self.clip_max,
            )
            p_b0 = np.clip(
                self._compose_raw(fi_b, w1_b_fn(neutral), w2_b_fn(neutral)),
                self.clip_min, self.clip_max,
            )
            m0 = 0.5 * (p_a0 + p_b0)
            shift_a = (self.gap_shrink - 1.0) * (p_a0 - m0)
            shift_b = (self.gap_shrink - 1.0) * (p_b0 - m0)

        def p_a_fn(state: Any) -> np.ndarray:
            return self._finish(
                self._compose_raw(fi_a, w1_a_fn(state), w2_a_fn(state)),
                shift_a, offsets,
            )

        def p_b_fn(state: Any) -> np.ndarray:
            return self._finish(
                self._compose_raw(fi_b, w1_b_fn(state), w2_b_fn(state)),
                shift_b, offsets,
            )

        return p_a_fn, p_b_fn
