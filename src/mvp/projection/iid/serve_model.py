"""Serve point win probability estimators for the IID tennis projector.

A `ServeWinProbEstimator` takes a polars DataFrame of matches (one row per
match, with both players' features) and returns a per-match estimate of each
player's serve point win probability for the matchup. These probabilities are
the fundamental input to the IID chain in `mvp.projection.iid.chain` — from
them, hold-per-game and tiebreak-game-win probabilities follow analytically.
"""

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, Literal

import numpy as np
import polars as pl

from mvp.projection.models import get_regression_model

logger = logging.getLogger(__name__)


ServeStateFn = Callable[["ScoreState"], np.ndarray]  # type: ignore[name-defined]


# Default clip range for serve point win prob. ATP serve point win rates
# essentially never fall outside [0.30, 0.90]; clipping protects the
# downstream lookup tables and signals upstream feature bugs if hit often.
SERVE_PROB_MIN: Final[float] = 0.30
SERVE_PROB_MAX: Final[float] = 0.90

# League-mean fallback for missing values. Loosely calibrated to ATP tour /
# challenger pooled serve point win rate.
LEAGUE_MEAN_SERVE_PROB: Final[float] = 0.62


def _nan_if_none(value: int | None) -> float:
    """NaN for an unreadable score, rather than a plausible-looking number.

    `ScoreState.game_points_*` returns None only when the score label is in
    neither the regular-game nor the tiebreak vocabulary. The old code defaulted
    that case to 0 — indistinguishable from love, which is a real state — so an
    unparsed score entered the model as a confident claim about the score. NaN
    carries the ignorance forward: XGBoost routes it as missing, and the
    logistic path substitutes the training mean at predict time.
    """
    return float("nan") if value is None else float(value)


def neutral_score_state() -> "ScoreState":  # type: ignore[name-defined]
    """The opening state: 0-0, first serve, no sets played, not a tiebreak.

    The reference point for every scalar `p` this module produces — the
    tiebreak approximation's `p_a_avg / p_b_avg`, the serve diagnostics, and
    the gap-shrink offsets. Defined once because three call sites previously
    built their own copy, and a silent divergence between them would move the
    shrink offsets relative to the probabilities they correct.

    `ScoreState` is imported inside the function: this module is imported by
    `score_state` transitively, and a top-level import closes the cycle.
    """
    from mvp.projection.iid.score_state import ScoreState

    return ScoreState(
        serve_num=1,
        game_score_server="0", game_score_returner="0",
        is_tiebreak=False,
        set_score_server_games=0, set_score_returner_games=0,
        sets_won_server=0, sets_won_returner=0,
        best_of=3,
    )


def build_serve_model(cfg: Any, engine: Any = None) -> "ServeWinProbEstimator":
    """Construct the serve estimator described by a `ServeModelConfig`.

    Single implementation shared by the projection runner, the backtest and the
    tuner. It previously existed as a private copy in each of runner.py and
    backtest.py, and the copies drifted: only the backtest passed `gap_shrink`,
    so a config with `gap_shrink != 1.0` was scored by a *different model*
    depending on which entrypoint you ran. Any new config field has to reach
    every caller at once, which means one builder.
    """
    if cfg.type == "identity":
        return IdentityServeModel(
            window=cfg.window, clip_min=cfg.clip_min, clip_max=cfg.clip_max,
        )
    if cfg.type == "matchup":
        if not cfg.feature_columns:
            raise ValueError(
                "serve_model.feature_columns must be non-empty for type=matchup"
            )
        return MatchupServeModel(
            feature_columns=cfg.feature_columns,
            match_level_columns=cfg.match_level_columns,
            regressor_type=cfg.regressor.type,
            regressor_params=dict(cfg.regressor.params),
            clip_min=cfg.clip_min,
            clip_max=cfg.clip_max,
        )
    if cfg.type == "score_state":
        if not cfg.match_level_features and not cfg.point_level_features:
            raise ValueError(
                "serve_model.match_level_features and/or point_level_features "
                "must be non-empty for type=score_state"
            )
        return ScoreStateChainServeModel(
            model_type=cfg.model_type,
            match_level_features=cfg.match_level_features,
            point_level_features=cfg.point_level_features,
            params=dict(cfg.params),
            engine=engine,
            clip_min=cfg.clip_min,
            clip_max=cfg.clip_max,
            gap_shrink=cfg.gap_shrink,
            surface_circuit_offset=dict(cfg.surface_circuit_offset),
            posterior_draws=cfg.posterior_draws,
            posterior_seed=cfg.posterior_seed,
        )
    if cfg.type == "two_level":
        from mvp.projection.iid.two_level_serve_model import TwoLevelServeModel

        # No non-empty requirement. An all-empty two-level model is the
        # feature-blind fit validation-ladder step 2 is defined as, and it is the
        # round-0 state of any component-wise FS that starts from nothing. Each
        # component degrades to its own training base rate — first_in via
        # FirstServeInModel's intercept-only path, the win branches via
        # _ConstantBranch — so the composition stays well defined throughout.
        return TwoLevelServeModel(
            model_type=cfg.model_type,
            first_in_match_features=cfg.first_in_match_features,
            first_in_point_features=cfg.first_in_point_features,
            win_first_match_features=cfg.win_first_match_features,
            win_first_point_features=cfg.win_first_point_features,
            win_second_match_features=cfg.win_second_match_features,
            win_second_point_features=cfg.win_second_point_features,
            params=dict(cfg.params),
            first_in_params=dict(cfg.first_in_params) or dict(cfg.params),
            engine=engine,
            clip_min=cfg.clip_min,
            clip_max=cfg.clip_max,
            gap_shrink=cfg.gap_shrink,
            surface_circuit_offset=dict(cfg.surface_circuit_offset),
        )
    raise ValueError(f"Unknown serve model type: {cfg.type}")


def perspective_column(
    name: str, *, is_diff: bool, swap: bool,
) -> tuple[str, str, float]:
    """(engine column, inference-name fallback, sign) for one match feature.

    ONE definition of the server-perspective rule. `swap=False` → A serves,
    `swap=True` → B serves. Mirrored features (`is_diff=False`) read the
    PARTNER's column on the swap side; diff features (`is_diff=True`) have only
    a `player_` column in the frame, so the swap side is its negation.

    The fallback exists because two frame conventions reach these readers: the
    match-grain engine frame carries `player_`/`opp_` names, while the serve FS
    scorer builds one already in inference names (`server_`/`returner_`). In
    the latter the partner of `server_x` is `returner_x`, which is why the
    fallback is perspective-dependent rather than a plain rename.
    """
    for pref, own, other in (
        ("server_", ("player_", "server_"), ("opp_", "returner_")),
        ("returner_", ("opp_", "returner_"), ("player_", "server_")),
    ):
        if not name.startswith(pref):
            continue
        base = name[len(pref):]
        if is_diff:
            # Only the player_ diff column exists. A server_ diff negates when
            # B serves; a returner_ diff is the same quantity seen from the
            # other side, so its signs are the mirror image.
            sign = (-1.0 if swap else 1.0) if pref == "server_" else (
                1.0 if swap else -1.0
            )
            return "player_" + base, "server_" + base, sign
        src_pref, fb_pref = other if swap else own
        return src_pref + base, fb_pref + base, 1.0
    return name, name, 1.0


def match_feature_matrix(
    df: pl.DataFrame,
    cols: list[str],
    is_diff: list[bool] | None,
    *,
    swap: bool,
    owner: str = "match_feature_matrix",
) -> np.ndarray:
    """Match-level design matrix in server perspective, via `perspective_column`.

    Both `ScoreStateChainServeModel._match_feature_values` and
    `TwoLevelServeModel._first_in_for` build their swap side here. A second copy
    is how `_first_in_for` came to apply only the negation half of the rule,
    leaving every mirrored column holding A's values on B's side -- silently,
    because a mirrored-only feature set makes the two sides identical rather
    than wrong-looking.
    """
    flags = is_diff or [False] * len(cols)
    resolved: list[tuple[str, float]] = []
    missing: list[str] = []
    for name, diff in zip(cols, flags, strict=True):
        engine, fallback, sign = perspective_column(name, is_diff=diff, swap=swap)
        if engine in df.columns:
            resolved.append((engine, sign))
        elif fallback in df.columns:
            resolved.append((fallback, sign))
        else:
            missing.append(engine)
    if missing:
        raise KeyError(
            f"{owner}: match features missing from df (swap={swap}): "
            f"{sorted(missing)}"
        )
    if not resolved:
        return np.zeros((len(df), 0), dtype=np.float64)
    out = []
    for col, sign in resolved:
        arr = df[col].to_numpy().astype(np.float64)
        out.append(arr * sign if sign != 1.0 else arr)
    return np.column_stack(out)


def resolve_match_feature_cols(
    match_level_features: list[str],
) -> tuple[list[str], list[bool]]:
    """Map match-level feature specs to inference column names + swap mechanism.

    `player_*` specs become `server_*`; `opp_*` become `returner_*`; unprefixed
    specs pass through as match-level.

    Returns `(cols, is_diff_flags)`. `is_diff` is True for diff-style features
    (registry `mirror=False`) — these have only a `player_` column in the raw
    frame, so the swap (B-serves) perspective is the NEGATION of the server-side
    value. `is_diff=False` means the swap side is read from a separate `opp_`
    column, which the caller must therefore materialize.

    Module-level so the FS selector (which has to load those `opp_` columns) and
    the model (which reads them) share one classification and can't drift.
    """
    from mvp.model.engine import build_column_name, parse_feature_spec
    from mvp.model.registry import get_registry

    registry = get_registry()
    cols: list[str] = []
    is_diff_flags: list[bool] = []
    for spec in match_level_features:
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
                # base_name isn't a directly-registered feature — it's a
                # transform-output column. A single transform (e.g.
                # style_matchup, mirror=False) can emit BOTH a mirror pair
                # (player_/opp_vs_opp_style_resid) AND a player-only diff
                # (player_vs_opp_style_resid_diff), so the transform's mirror
                # flag can't classify the column. Decide per-column by whether
                # the opp_ counterpart output exists: present -> mirror pair
                # (swap reads opp_); absent -> anti-symmetric diff (swap
                # negates the player_ value).
                is_diff = registry.transform_for_output("opp_" + base_name) is None
        cols.append(col)
        is_diff_flags.append(is_diff)
    return cols, is_diff_flags


def swap_side_opp_specs(match_level_features: list[str]) -> list[str]:
    """`opp_`-prefixed specs the swap (B-serves) perspective reads as columns.

    Mirror features — plain per-player features and `register_matchup` outputs
    (`mirror=True`), plus transform-emitted mirror pairs — are read from `opp_X`
    at the swap side, never negated: for a cross-domain matchup the swap value is
    `opp_svc_elo - player_ret_elo`, which is not `-(player_svc_elo - opp_ret_elo)`.
    Callers materializing a match-grain frame for `predict_state_fn` must request
    these alongside the configured specs.

    Diff/sum features (`mirror=False`) are absent from the result — their swap
    side negates the `player_` column, so no `opp_` read is involved.
    """
    from mvp.model.engine import parse_feature_spec

    _cols, is_diff_flags = resolve_match_feature_cols(match_level_features)
    specs: list[str] = []
    for spec, is_diff in zip(match_level_features, is_diff_flags):
        prefix, base_name, _full_name, params = parse_feature_spec(spec)
        if is_diff or prefix != "player":
            continue
        opp_spec = f"opp_{base_name}"
        if params:
            param_str = ",".join(f"{k}={v}" for k, v in params.items())
            opp_spec = f"opp_{base_name}({param_str})"
        if opp_spec not in specs:
            specs.append(opp_spec)
    return specs


def apply_serve_branch(
    points: pl.DataFrame, serve_branch: int | None
) -> pl.DataFrame:
    """Restrict point rows to one branch of the serve tree. `None` keeps all.

    Module level because two unrelated estimators need it: the score-state model
    and `_ConstantBranch`, which has no model at all and shares no base class
    with it. Duplicating six lines of filter across those two is how the two
    branches quietly stop meaning the same thing.

    The missing-`serve` case raises rather than passing the frame through: a
    branch model silently trained on BOTH branches is a wrong model that scores
    plausibly and raises nothing.
    """
    if serve_branch is None:
        return points
    if "serve" not in points.columns:
        raise ValueError(
            "serve_branch set but the point frame has no `serve` column; "
            "the branch filter cannot be applied silently"
        )
    return points.filter(pl.col("serve") == serve_branch)


def _require_preloaded_cols(
    needed: list[str], available: set[str], serve_branch: int | None,
) -> None:
    """Every configured match feature must be in the preloaded frame.

    The selection below is `[c for c in needed if c in available]`, so a spec the
    caller forgot to preload is dropped and the branch trains on FEWER features
    than it was configured with — no error, no log, just a worse model whose
    metrics look ordinary. That is the failure this guards: it would make every
    tuned result quietly wrong rather than obviously broken.

    `FirstServeInModel._match_features` already raises for the same reason; the
    win branches did not, which is the asymmetry this closes.
    """
    absent = [c for c in needed if c not in available]
    if not absent:
        return
    where = "" if serve_branch is None else f" [serve=={serve_branch}]"
    raise KeyError(
        f"ScoreStateChainServeModel{where}: match features absent from the "
        f"preloaded frame: {sorted(absent)}. The caller must preload every spec "
        f"this branch reads — for a two_level model that is the UNION of the "
        f"three components' match features."
    )


def swap_side_partner_specs(match_level_features: list[str]) -> list[str]:
    """Every counterpart column a mirror feature is read from, either side.

    `swap_side_opp_specs` above answers a narrower question — "which `opp_`
    columns must exist" — and skips anything not `player_`-prefixed. That is
    complete only while every selected spec is `player_`-prefixed. It is not:
    the shortlist's composite-side expansion puts `opp_`-prefixed specs in the
    candidate pool, and FS selects them (`opp_surface_matches(days=30)`).

    For a mirror feature the model reads BOTH sides regardless of which one was
    selected. `_match_feature_values` (serve_model.py:855-885): a `returner_`
    column reads `opp_` when A serves and `player_` when B serves, and a
    `server_` column reads the reverse. So `opp_surface_matches(days=30)`
    requires `player_surface_matches(days=30)` exactly as `player_glicko_rd`
    requires `opp_glicko_rd`.

    Diffs are still excluded — their swap side negates the `player_` column in
    place, so no second column is involved.

    Why this is not merely tidiness: a config emitted without the partner still
    RUNS today, because the engine computes every feature player-side first and
    derives `opp_` by mirroring (Phase 2 / Phase 4), so the player-side column
    is a precondition of the opp-side one and lands in the frame regardless.
    The config is then relying on that ordering rather than declaring what it
    reads — and an opp-only feature, or a change to the mirroring pass, turns
    that into a silent wrong-column read instead of a missing-column error.
    """
    from mvp.model.engine import parse_feature_spec

    _cols, is_diff_flags = resolve_match_feature_cols(match_level_features)
    specs: list[str] = []
    for spec, is_diff in zip(match_level_features, is_diff_flags, strict=True):
        prefix, base_name, _full_name, params = parse_feature_spec(spec)
        if is_diff:
            continue
        if prefix == "player":
            partner = f"opp_{base_name}"
        elif prefix == "opp":
            partner = f"player_{base_name}"
        else:
            # Unprefixed match-level (e.g. a `_sum`): one column serves both
            # perspectives, so there is no partner to request.
            continue
        if params:
            param_str = ",".join(f"{k}={v}" for k, v in params.items())
            partner = f"{partner}({param_str})"
        if partner not in specs:
            specs.append(partner)
    return specs


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

    # ------------------------------------------------------------------
    # Distributional output
    #
    # An estimator that knows how uncertain it is exposes that by emitting
    # `n_draws` samples of `p` per match instead of one value. The projector
    # runs the chain once per draw and averages the resulting outcome
    # distributions, so what reaches pricing is the mixture over the posterior
    # rather than the distribution at a single representative `p`.
    #
    # Draws are addressed BY INDEX rather than returned as one (n_draws, N)
    # block, for two reasons: the projector only ever holds one draw's chain
    # output at a time (a 200-draw run would otherwise carry 200 copies of the
    # (N, 131) spread pmf), and an estimator that seeds its sampling on the
    # draw index is reproducible — the same config re-run gives the same
    # posterior, which a backtest depends on.
    #
    # The defaults below make a point estimator a one-draw distribution, so
    # every existing estimator satisfies this interface unchanged and the
    # projector needs no special case.
    # ------------------------------------------------------------------

    @property
    def n_draws(self) -> int:
        """Posterior draws emitted per match. 1 means a point estimate."""
        return 1

    def predict_draw(
        self, df: pl.DataFrame, draw: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Predict (p_a, p_b) for posterior draw `draw` in [0, n_draws)."""
        return self.predict(df)

    def predict_state_fn_draw(
        self, df: pl.DataFrame, draw: int,
    ) -> tuple[ServeStateFn, ServeStateFn]:
        """State-aware callables for posterior draw `draw` in [0, n_draws)."""
        return self.predict_state_fn(df)

    def predict_state_fn_and_neutral(
        self, df: pl.DataFrame, draw: int,
    ) -> tuple[ServeStateFn, ServeStateFn, np.ndarray, np.ndarray]:
        """State callables AND the neutral-state scalars, for one draw.

        The projector needs both: the callables drive the stateful DP, and the
        scalars are the `p_a_avg / p_b_avg` the tiebreak approximation uses.
        Requesting them together rather than as two calls matters for a
        state-aware model, which rebuilds its entire per-match feature matrix
        on every call — doing that twice per draw doubles the dominant cost of
        a Monte-Carlo run, and the second build reassigns instance state that
        the first call's closures are still reading. Same numbers either way,
        but only because both calls pass the same `df` and `draw`; that is a
        coincidence worth not depending on.
        """
        p_a_fn, p_b_fn = self.predict_state_fn_draw(df, draw)
        p_a, p_b = self.predict_draw(df, draw)
        return p_a_fn, p_b_fn, p_a, p_b


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
            "game_points_server", "game_points_returner",
            "game_points_diff",
            "tiebreak_point_diff", "tiebreak_points_played",
            "serve", "is_second_serve",
            "set_num", "game_num",
        }
    )

    # Source columns for known match-constant derivations. Used by
    # required_columns + _point_constant_values: if a configured point feature
    # is a derivation, the caller must supply its source column; the model
    # materializes the derived column on demand.
    _POINT_FEATURE_SOURCES: Final[dict[str, tuple[str, ...]]] = {
        "is_surface_hard": ("surface",),
        "is_surface_clay": ("surface",),
        "is_surface_grass": ("surface",),
    }

    def __init__(
        self,
        model_type: Literal[
            "logistic", "xgboost", "bayesian_logistic", "hierarchical_boosted",
        ],
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
        gap_shrink: float = 1.0,
        surface_circuit_offset: dict[str, float] | None = None,
        posterior_draws: int = 200,
        posterior_seed: int = 0,
        serve_branch: int | None = None,
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
        self.gap_shrink = gap_shrink
        self.surface_circuit_offset = dict(surface_circuit_offset or {})
        # How often the post-offset guard actually truncated a correction. The
        # offsets are ~1-2pp against a p range measured well inside the clip, so
        # this should stay at zero; a non-zero count means the correction is
        # being silently reduced for extreme players and the number reported is
        # not the number applied.
        self.offset_clipped_count = 0
        # Only consumed by distributional inner models; a point classifier
        # ignores them and reports n_draws == 1.
        self.posterior_draws = posterior_draws
        self.posterior_seed = posterior_seed
        # Restrict FITTING to one branch of the serve tree (1 = first serve in,
        # 2 = second serve). None trains on every point, which is the
        # single-level behaviour. Used by TwoLevelServeModel to get its two
        # win-rate branches from one implementation rather than two.
        #
        # The filter is applied AFTER the preloaded-vs-read selection, never at
        # the read. ServeDiscoverySelector passes `preloaded_points`, so a
        # filter attached to the parquet read alone would leave FS silently
        # training both branches on all rows — a wrong model that raises
        # nothing and scores plausibly.
        if serve_branch is not None and serve_branch not in (1, 2):
            raise ValueError(
                f"serve_branch must be 1, 2 or None; got {serve_branch!r}"
            )
        self.serve_branch = serve_branch
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

        # Wall-time of the last fit(), by phase (seconds). Diagnostic only;
        # the serve FS aggregates these onto its [diag] line (plan
        # 2026-09-03-serve-fs-wall-time, Phase 0).
        self.fit_timings: dict[str, float] = {}

    def __getstate__(self) -> dict[str, Any]:
        # _engine holds the global FeatureRegistry, which contains closure
        # funcs from register_diff/_sum/_matchup that pickle can't resolve by
        # qualified name. fit() and score_test_points() rebuild a fresh engine
        # when _engine is None, and predict_state_fn doesn't need one — so
        # dropping it on save is safe.
        state = self.__dict__.copy()
        state["_engine"] = None
        return state

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
            if name in self._STATE_DERIVABLE:
                continue
            # For derivations (e.g. surface one-hots) require the source
            # column. _point_constant_values materializes the derived column.
            sources = self._POINT_FEATURE_SOURCES.get(name)
            if sources:
                cols.update(sources)
            else:
                cols.add(name)
        cols.add("best_of")
        if self.surface_circuit_offset:
            # Required, not optional: a configured correction that silently
            # does nothing because the frame lacks the columns would leave the
            # config named for a correction it never applied.
            cols.update(("surface", "circuit"))
        return sorted(cols)

    def _resolve_match_feature_cols(self) -> list[str]:
        """Resolve config specs to inference column names (see
        `resolve_match_feature_cols`), populating `self._match_feature_is_diff`
        with the per-feature swap mechanism used by `_match_feature_values`.
        """
        cols, is_diff_flags = resolve_match_feature_cols(self.match_level_features)
        self._match_feature_is_diff = is_diff_flags
        return cols

    def _apply_serve_branch(self, points: pl.DataFrame) -> pl.DataFrame:
        """Restrict rows to this model's branch of the serve tree.

        Deliberately called at both point-loading sites rather than passed as an
        argument to the read: `fit` and `score_test_points` each take a
        `preloaded_points` frame from the FS selector, and a filter applied only
        to the parquet read would be skipped exactly when FS is driving.
        """
        return apply_serve_branch(points, self.serve_branch)

    def fit(
        self,
        df: pl.DataFrame,
        *,
        preloaded_match_features: "pl.DataFrame | None" = None,
        preloaded_points: "pl.DataFrame | None" = None,
    ) -> None:
        """Train the point-grain classifier on matches present in `df`.

        `df` is the IID runner's train split (one row per match_uid). Points
        are loaded and filtered to these match_uids; match-level features are
        (re)computed via a cached FeatureEngine call.

        `preloaded_match_features` and `preloaded_points` are optional pre-filtered
        frames passed by ServeDiscoverySelector to avoid repeated full parquet reads
        during the FS loop. When provided they must already be filtered to the
        training match_uids.
        """
        from mvp.common.base_job import get_data_root, get_local_data_root
        from mvp.model.engine import FeatureEngine, build_column_name, parse_feature_spec
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

        t0 = time.perf_counter()
        if preloaded_points is not None:
            points = preloaded_points
        else:
            points = pl.read_parquet(points_path).filter(
                pl.col("match_uid").is_in(train_uids)
            )
        points = self._apply_serve_branch(points)
        if len(points) == 0:
            raise ValueError("no points rows matched the training match_uids")
        load_s = time.perf_counter() - t0
        logger.info(
            "Loaded %d points for %d train matches%s (%.1fs)",
            len(points), len(train_uids),
            "" if self.serve_branch is None else f" [serve=={self.serve_branch}]",
            load_s,
        )

        self._match_feature_cols = self._resolve_match_feature_cols()

        join_s = 0.0
        derive_s = 0.0
        if self.match_level_features:
            t_join = time.perf_counter()
            if preloaded_match_features is not None:
                needed = [
                    build_column_name(full_name, params)
                    for spec in self.match_level_features
                    for _, _, full_name, params in [parse_feature_spec(spec)]
                ]
                available = set(preloaded_match_features.columns)
                _require_preloaded_cols(needed, available, self.serve_branch)
                sel = ["match_uid", "player_id", "opp_id"] + [c for c in needed if c in available]
                matches_features = preloaded_match_features.select(sel).rename(
                    {"player_id": "server_id", "opp_id": "returner_id"}
                )
            else:
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
            join_s = time.perf_counter() - t_join
            logger.info(
                "Joined %d match-level features (%.1fs)",
                len(self.match_level_features), join_s,
            )
        else:
            joined = points

        derived = [n for n in self.point_level_features if n in DERIVED_POINT_FEATURES]
        if derived:
            t_derive = time.perf_counter()
            joined = add_derived_point_features(joined, derived)
            derive_s = time.perf_counter() - t_derive
            logger.info(
                "Derived %d point features (%.1fs)", len(derived), derive_s,
            )

        t_matrix = time.perf_counter()
        joined = joined.filter(pl.col("point_won_by_server").is_not_null())
        if len(joined) == 0:
            raise ValueError("no valid training points after target filter")

        feature_cols = self._match_feature_cols + self.point_level_features
        X = joined.select(feature_cols).to_numpy()
        y = joined["point_won_by_server"].cast(pl.Int64).to_numpy()
        # Server identity per point, for models carrying a per-player
        # parameter. Fit sees training matches only and `build_test_set` is
        # date-disjoint, so an effect never sees the match it will predict.
        groups = (
            joined["server_id"].to_numpy()
            if "server_id" in joined.columns
            else None
        )
        matrix_s = time.perf_counter() - t_matrix

        logger.info(
            "Fitting score-state %s model (%d samples, %d features)",
            self.model_type, X.shape[0], X.shape[1],
        )
        t_fit = time.perf_counter()
        self._model = build_score_state_model(
            type_=self.model_type,
            feature_names=feature_cols,
            params=self.params,
            match_feature_names=self._match_feature_cols,
            point_feature_names=self.point_level_features,
            n_draws=self.posterior_draws,
            seed=self.posterior_seed,
        )
        self._model.fit(X, y, groups=groups)
        fit_s = time.perf_counter() - t_fit
        logger.info("Score-state fit complete in %.1fs", fit_s)
        self.fit_timings = {
            "load": load_s, "join": join_s, "derive": derive_s,
            "matrix": matrix_s, "fit": fit_s,
        }

    def score_test_points(
        self,
        df: pl.DataFrame,
        *,
        preloaded_match_features: "pl.DataFrame | None" = None,
        preloaded_points: "pl.DataFrame | None" = None,
    ) -> dict[str, float]:
        """Point-grain classification metrics on held-out match points.

        Materializes the same point feature matrix used at fit time for the
        match_uids in `df`, runs the fitted model's `predict_proba`, and scores
        against `point_won_by_server`. Returns metrics with `point_` prefixes
        so they coexist with the chain's match-grain classification metrics.
        """
        if self._model is None:
            raise RuntimeError("score_test_points called before fit")
        joined = self.build_test_point_frame(
            df,
            preloaded_match_features=preloaded_match_features,
            preloaded_points=preloaded_points,
        )
        if joined is None or len(joined) == 0:
            return {}
        raw = self.score_test_frame(joined)
        return {f"point_{k}": v for k, v in raw.items()}

    def score_test_frame(self, joined: "pl.DataFrame") -> dict[str, float]:
        """Classification metrics for a frame `build_test_point_frame` built.

        The metric half of `score_test_points`, extracted so the X/y build and
        the `predict_proba` call have one home: the per-branch FS scorer and
        the runner's per-branch emit both need these numbers UNPREFIXED, and
        reaching into `_model` / `_match_feature_cols` from three places is how
        the three come to disagree. `score_test_points` is this plus its
        `point_` re-key, unchanged for its callers.

        `full_range=True` unconditionally: these rows are one per point from
        the SERVER's perspective (`apply_serve_branch` filters on `serve`), so
        unlike the classification frame there is no mirrored partner row and
        the default `p >= 0.50` mask would discard data rather than
        de-duplicate it. It discards asymmetrically — win_first sits near
        0.6901 and survives almost intact, win_second at 0.4968 loses about
        half its rows. All three callers of this method are branch-grain.
        """
        from mvp.model.metrics import compute_metrics

        if self._model is None:
            raise RuntimeError("score_test_frame called before fit")
        feature_cols = self._match_feature_cols + self.point_level_features
        X = joined.select(feature_cols).to_numpy()
        y = joined["point_won_by_server"].cast(pl.Int64).to_numpy()
        return compute_metrics(y, self._model.predict_proba(X), full_range=True)

    def build_test_point_frame(
        self,
        df: pl.DataFrame,
        *,
        preloaded_match_features: "pl.DataFrame | None" = None,
        preloaded_points: "pl.DataFrame | None" = None,
    ) -> "pl.DataFrame | None":
        """Held-out points joined to their match features, ready to select from.

        The frame half of `score_test_points`, extracted so a caller that needs
        the ROWS rather than a metric dict does not have to rebuild this join.
        The two-level step-3 comparison is that caller: it needs `win_first` and
        `win_second` evaluated at EVERY test point so the composite can be formed,
        but each branch model filters to its own half of the serve tree, so no
        per-branch call can produce it. Reconstructing the join in a script
        instead would be an approximation of the engine rather than the engine.

        Returns `None` when there is nothing to score, matching the empty cases
        `score_test_points` already returned `{}` for.

        `serve_branch` still applies — build the frame from an instance whose
        branch is `None` to get both halves.
        """
        from mvp.common.base_job import get_data_root, get_local_data_root
        from mvp.model.engine import FeatureEngine, build_column_name, parse_feature_spec
        from mvp.projection.iid.score_state_features import (
            DERIVED_POINT_FEATURES,
            add_derived_point_features,
        )

        if "match_uid" not in df.columns:
            raise ValueError("score_test_points: df missing match_uid column")
        test_uids = df["match_uid"].unique().to_list()
        if not test_uids:
            return None

        points_path = self._points_path or (
            get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
        )
        matches_path = self._matches_path or (
            get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        )
        cache_dir = self._cache_dir or (
            get_local_data_root() / "features" / "cache"
        )

        if preloaded_points is not None:
            points = preloaded_points
        else:
            points = pl.read_parquet(points_path).filter(
                pl.col("match_uid").is_in(test_uids)
            )
        points = self._apply_serve_branch(points)
        if len(points) == 0:
            return None

        if self.match_level_features:
            if preloaded_match_features is not None:
                needed = [
                    build_column_name(full_name, params)
                    for spec in self.match_level_features
                    for _, _, full_name, params in [parse_feature_spec(spec)]
                ]
                available = set(preloaded_match_features.columns)
                _require_preloaded_cols(needed, available, self.serve_branch)
                sel = ["match_uid", "player_id", "opp_id"] + [c for c in needed if c in available]
                matches_features = preloaded_match_features.select(sel).rename(
                    {"player_id": "server_id", "opp_id": "returner_id"}
                )
            else:
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
            return None
        return joined

    def _match_feature_values(self, df: pl.DataFrame, *, swap: bool) -> np.ndarray:
        """Build the match-level feature matrix in server-perspective.

        `swap=False` → player A is server; `swap=True` → player B is server
        (columns read via the player_/opp_ swap). Diff-style features
        (mirror=False) have no opp_ counterpart; the swap-side value is the
        negation of the player_ column.
        """
        return match_feature_matrix(
            df, self._match_feature_cols, self._match_feature_is_diff,
            swap=swap, owner="ScoreStateChainServeModel",
        )

    def _point_constant_values(self, df: pl.DataFrame) -> dict[str, np.ndarray]:
        """Broadcast-constant point features (surface flags, etc.) from df."""
        from mvp.projection.iid.score_state_features import (
            DERIVED_POINT_FEATURES,
            add_derived_point_features,
        )

        needed = [
            name for name in self.point_level_features
            if name not in self._STATE_DERIVABLE
        ]
        to_derive = [
            name for name in needed
            if name not in df.columns and name in DERIVED_POINT_FEATURES
        ]
        if to_derive:
            missing_sources = {
                src
                for name in to_derive
                for src in self._POINT_FEATURE_SOURCES.get(name, ())
                if src not in df.columns
            }
            if missing_sources:
                raise KeyError(
                    f"ScoreStateChainServeModel: cannot derive point features "
                    f"{to_derive} — df missing source column(s) {sorted(missing_sources)}"
                )
            df = add_derived_point_features(df, to_derive)

        out: dict[str, np.ndarray] = {}
        for name in needed:
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
            "set_num": float(state.sets_won_server + state.sets_won_returner + 1),
            "game_num": float(
                state.set_score_server_games + state.set_score_returner_games + 1
            ),
        }
        # Delegated to ScoreState so training and inference read the score
        # through one mapping. The previous inline table defaulted an
        # unrecognized label to 0, which silently turned every tiebreak score
        # into love at inference while training was nulling the same points —
        # the two sides disagreed about the same state.
        gs_s = state.game_points_server()
        gs_r = state.game_points_returner()
        values["game_points_server"] = _nan_if_none(gs_s)
        values["game_points_returner"] = _nan_if_none(gs_r)
        values["game_points_diff"] = _nan_if_none(state.game_points_diff())
        values["tiebreak_point_diff"] = float(state.tiebreak_point_diff())
        values["tiebreak_points_played"] = float(state.tiebreak_points_played())
        return values

    def predict(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Scalar prediction evaluated at a neutral opening state.

        Used by the projector as the `p_a_avg / p_b_avg` input to the stateful
        chain's tiebreak approximation.
        """
        p_a_fn, p_b_fn = self.predict_state_fn(df)
        neutral = neutral_score_state()
        return p_a_fn(neutral), p_b_fn(neutral)

    @property
    def n_draws(self) -> int:
        """Posterior draws, taken from the inner point-grain classifier.

        A classifier with no posterior (plain logistic, XGBoost) reports 1 and
        this model behaves exactly as it always has.
        """
        return int(getattr(self._model, "n_draws", 1)) if self._model else 1

    # Attributes added after artifacts were first pickled, with the value an
    # older artifact should take. joblib restores `__dict__` directly and never
    # calls `__init__`, so without this a cached `serve_model.joblib` written
    # before a field existed raises AttributeError the moment anything reads it.
    #
    # The defaults are the honest reconstruction, not a patch: a model trained
    # before the offset mechanism existed IS a no-offset model, and one trained
    # before posterior draws IS a point estimator. The dangerous case — a config
    # that ADDS a correction being served by a stale artifact that cannot apply
    # it — is caught upstream, because adding the block changes the config text
    # and `projection_run._load_artifact` retrains on any text mismatch.
    _POST_PICKLE_DEFAULTS: Final[dict[str, Any]] = {
        "surface_circuit_offset": {},
        "offset_clipped_count": 0,
        "posterior_draws": 200,
        "posterior_seed": 0,
        # None = train on every point, which is what every artifact written
        # before the two-level work did.
        "serve_branch": None,
    }

    def __setstate__(self, state: dict[str, Any]) -> None:
        for name, default in self._POST_PICKLE_DEFAULTS.items():
            state.setdefault(
                name, dict(default) if isinstance(default, dict) else default
            )
        self.__dict__.update(state)

    def _surface_circuit_offsets(self, df: pl.DataFrame) -> np.ndarray | None:
        """Per-match offset from the `<surface>/<circuit>` table, or None.

        Fitted per cell, never pooled: a pooled fit averages a clay/challenger
        correction with a hard/tour one whose bias runs the OPPOSITE sign, so
        it would apply the wrong-signed correction to both. The table is
        per-cell by construction and this lookup keeps it that way — a cell
        absent from the table gets 0.0 rather than a pooled fallback.
        """
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
        offsets = np.array([table.get(k, 0.0) for k in keys], dtype=np.float64)
        unmatched = sorted({k for k in keys if k not in table})
        if unmatched:
            logger.info(
                "surface_circuit_offset: no entry for %s — those rows uncorrected",
                unmatched,
            )
        return offsets

    def _apply_offset(
        self, p: np.ndarray, offsets: np.ndarray | None,
    ) -> np.ndarray:
        """Add the calibration offset, re-clipping only if it leaves the range.

        Ordering is deliberate. The offset lands AFTER gap_shrink and after the
        clip that follows it, so a measured +1.0pp gap is corrected by +1.0pp
        rather than by `gap_shrink x 1.0pp`. Applied earlier it would be
        compressed by a knob that is set in exactly one config, which would make
        the same table mean different things in different configs.

        The trailing clip is a GUARD, not part of the correction: with offsets
        of ~1-2pp and a measured p range well inside [clip_min, clip_max] it
        should never bind, and `offset_clipped_count` records it if it does —
        because a silently truncated correction is not the correction that was
        measured.
        """
        if offsets is None:
            return p
        shifted = p + offsets
        out = np.clip(shifted, self.clip_min, self.clip_max)
        n_clipped = int(np.count_nonzero(shifted != out))
        if n_clipped:
            self.offset_clipped_count += n_clipped
        return out

    def _resolve_draw(self, draw: int | None) -> int | None:
        """Map a requested draw index onto the inner model's capability.

        The projector always loops draws, so a point classifier is asked for
        draw 0. For a model with a single draw, draw 0 IS the point estimate —
        resolving it to `None` keeps that path on `predict_proba` and bit-
        identical to the pre-posterior behaviour. Asking a point model for a
        LATER draw is a real error and still raises.
        """
        if draw is None:
            return None
        n = self.n_draws
        if not 0 <= draw < n:
            raise ValueError(f"draw {draw} out of range [0, {n})")
        return None if n == 1 else draw

    def _proba(
        self, X: np.ndarray, draw: int | None, groups: np.ndarray | None = None,
    ) -> np.ndarray:
        """Inner-model probability, at a posterior draw when one is requested.

        `groups` is the server id per row, which a model with a per-player
        parameter needs at predict time as well as at fit time. Models
        without one accept and ignore it.
        """
        assert self._model is not None
        if draw is None:
            return self._model.predict_proba(X, groups)
        predict_draw = getattr(self._model, "predict_proba_draw", None)
        if predict_draw is None:
            raise TypeError(
                f"{type(self._model).__name__} emits a point estimate and cannot "
                f"serve posterior draw {draw}; use a distributional score-state "
                "model (e.g. bayesian_logistic, hierarchical_boosted)"
            )
        return predict_draw(X, draw, groups)

    def predict_draw(
        self, df: pl.DataFrame, draw: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Neutral-state (p_a, p_b) under posterior draw `draw`."""
        p_a_fn, p_b_fn = self._build_state_fns(df, draw=draw)
        neutral = neutral_score_state()
        return p_a_fn(neutral), p_b_fn(neutral)

    def predict_state_fn_and_neutral(
        self, df: pl.DataFrame, draw: int,
    ) -> tuple[ServeStateFn, ServeStateFn, np.ndarray, np.ndarray]:
        """One feature-matrix build serving both the DP and the tiebreak input."""
        p_a_fn, p_b_fn = self._build_state_fns(df, draw=draw)
        neutral = neutral_score_state()
        return p_a_fn, p_b_fn, p_a_fn(neutral), p_b_fn(neutral)

    def predict_state_fn_draw(
        self, df: pl.DataFrame, draw: int,
    ) -> tuple[ServeStateFn, ServeStateFn]:
        """State-aware callables under posterior draw `draw`."""
        return self._build_state_fns(df, draw=draw)

    def clip_mass(self, df: pl.DataFrame) -> dict[str, float]:
        """How much posterior mass the `[clip_min, clip_max]` bound truncates.

        Settles whether the clip is a live distortion or a vestigial safety
        rail. For a point estimate the bound is rarely reached, so clipping is
        harmless. For a posterior each draw is clipped individually, so mass
        beyond the bound is truncated — and asymmetrically whenever the mean
        sits off-centre between the bounds, which shifts the mixture mean on
        top of the Jensen shift the chain already applies.

        Evaluated at the neutral opening state, one prediction per (match,
        draw, perspective), so the fractions are over that population rather
        than over DP states (which would weight by how often the chain happens
        to visit a state).

        Returns fractions at each bound plus the observed range. If
        `frac_clipped` is negligible the clip is not distorting the posterior
        and the draw-vs-clip ordering is a note rather than a decision.
        """
        lows: list[np.ndarray] = []
        for draw in range(self.n_draws):
            p_a, p_b = self.predict_draw(df, draw)
            lows.append(np.concatenate([p_a, p_b]))
        allp = np.concatenate(lows)
        n = float(len(allp))
        if n == 0:
            return {}
        at_min = float(np.sum(allp <= self.clip_min))
        at_max = float(np.sum(allp >= self.clip_max))
        return {
            "n_predictions": n,
            "n_draws": float(self.n_draws),
            "clip_min": float(self.clip_min),
            "clip_max": float(self.clip_max),
            "frac_at_min": at_min / n,
            "frac_at_max": at_max / n,
            "frac_clipped": (at_min + at_max) / n,
            "p_min": float(np.min(allp)),
            "p_max": float(np.max(allp)),
            "p_std": float(np.std(allp)),
        }

    def predict_state_fn(
        self, df: pl.DataFrame,
    ) -> tuple[ServeStateFn, ServeStateFn]:
        """Build state-aware callables from the point estimate."""
        return self._build_state_fns(df, draw=None)

    def _build_state_fns(
        self, df: pl.DataFrame, *, draw: int | None,
    ) -> tuple[ServeStateFn, ServeStateFn]:
        """Build state-aware callables. Match-level features are cached once.

        The chain DP visits thousands of distinct ScoreStates per fold, but the
        model only reads state-derivable features named in `point_level_features`.
        Distinct ScoreStates with identical model-relevant values produce the
        same predict_proba output, so we cache per (perspective, key) where
        key is the tuple of state-derivable values the model actually uses.
        (The cache is local to this call and keyed only on state, so it does
        NOT amortize across draws — every draw re-evaluates every state.)

        `draw` decides where the posterior sits relative to the two
        transformations already on this boundary. The order is
        DRAW -> clip -> shrink -> clip: each draw is passed through the same
        post-processing a point estimate gets, rather than the post-processing
        being applied once to the posterior mean. That is the honest choice —
        what production emits is a clipped, shrunk probability, so a posterior
        over production's output must be a posterior over clipped, shrunk
        values. It also means the clip TRUNCATES the posterior rather than
        merely bounding its centre, asymmetrically once the mean sits off-centre
        between the bounds. `clip_mass` below measures whether that is
        happening; if the posteriors come out narrow relative to
        [clip_min, clip_max] the question is moot.
        """
        if self._model is None:
            raise RuntimeError(
                "ScoreStateChainServeModel.predict_state_fn called before fit"
            )
        draw = self._resolve_draw(draw)
        # Locals, not instance state: the returned closures own these arrays.
        # Assigning them onto `self` made every predict_state_fn call overwrite
        # the previous call's arrays (unsafe for a fitted model shared across
        # callers) and kept the last df's matrices alive on the instance.
        X_match_A = self._match_feature_values(df, swap=False)
        X_match_B = self._match_feature_values(df, swap=True)
        point_constants = self._point_constant_values(df)
        n = len(df)

        # Server identity per perspective: in perspective A the row's own
        # player is serving, in perspective B the opponent is. A model with a
        # per-player effect needs this to look the right effect up; without
        # it every row would silently fall back to the population level.
        groups_a = (
            df["player_id"].to_numpy() if "player_id" in df.columns else None
        )
        groups_b = df["opp_id"].to_numpy() if "opp_id" in df.columns else None

        # Per-match calibration offset. Surface and circuit are properties of
        # the MATCH, not of a player, so both perspectives share one array.
        sc_offset = self._surface_circuit_offsets(df)

        # Subset of point_level_features whose value depends on ScoreState —
        # the cache key is the tuple of these values for the current state.
        state_key_features = [
            name for name in self.point_level_features
            if name in self._STATE_DERIVABLE
        ]

        def _state_key(state: Any) -> tuple:
            if not state_key_features:
                return ()
            vals = self._state_derivable_values(state)
            return tuple(vals[name] for name in state_key_features)

        def _X_for(X_match: np.ndarray, state: Any) -> np.ndarray:
            state_vals = self._state_derivable_values(state)
            point_cols: list[np.ndarray] = []
            for name in self.point_level_features:
                if name in self._STATE_DERIVABLE:
                    point_cols.append(np.full(n, state_vals[name], dtype=np.float64))
                else:
                    point_cols.append(point_constants[name])
            if point_cols:
                X_point = np.column_stack(point_cols)
                return np.hstack([X_match, X_point])
            return X_match

        # Gap-shrink: compress the favorite-underdog serve gap toward the pair
        # mean by `gap_shrink` (1.0 = no-op), via a per-player constant offset
        # derived at the neutral opening state. Preserves each player's
        # state-dependent modulation; only narrows the between-player level gap.
        if self.gap_shrink != 1.0:
            _neutral = neutral_score_state()
            p_a0 = np.clip(
                self._proba(_X_for(X_match_A, _neutral), draw, groups_a),
                self.clip_min, self.clip_max,
            )
            p_b0 = np.clip(
                self._proba(_X_for(X_match_B, _neutral), draw, groups_b),
                self.clip_min, self.clip_max,
            )
            m0 = 0.5 * (p_a0 + p_b0)
            shift_a = (self.gap_shrink - 1.0) * (p_a0 - m0)
            shift_b = (self.gap_shrink - 1.0) * (p_b0 - m0)
        else:
            shift_a = 0.0
            shift_b = 0.0

        cache_a: dict[tuple, np.ndarray] = {}
        cache_b: dict[tuple, np.ndarray] = {}

        def p_a_fn(state: Any) -> np.ndarray:
            key = _state_key(state)
            cached = cache_a.get(key)
            if cached is not None:
                return cached
            X = _X_for(X_match_A, state)
            p = self._proba(X, draw, groups_a) + shift_a
            p = np.clip(p, self.clip_min, self.clip_max)
            p = self._apply_offset(p, sc_offset)
            cache_a[key] = p
            return p

        def p_b_fn(state: Any) -> np.ndarray:
            key = _state_key(state)
            cached = cache_b.get(key)
            if cached is not None:
                return cached
            X = _X_for(X_match_B, state)
            p = self._proba(X, draw, groups_b) + shift_b
            p = np.clip(p, self.clip_min, self.clip_max)
            p = self._apply_offset(p, sc_offset)
            cache_b[key] = p
            return p

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
