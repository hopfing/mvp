"""Configuration schema for the IID projection runner."""

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, BeforeValidator, model_validator

from mvp.model.config import DataConfig, FeaturesConfig, ValidationConfig
from mvp.projection.iid.metric_registry import validate_metric_name

MetricName = Annotated[str, BeforeValidator(validate_metric_name)]


class MatchupServeRegressorConfig(BaseModel):
    """Underlying regressor for the MatchupServeModel."""

    type: Literal["ridge", "linear"] = "ridge"
    params: dict[str, Any] = {}


class ServeModelConfig(BaseModel):
    """Serve win prob estimator configuration."""

    type: Literal["identity", "matchup", "score_state", "two_level", "bayes"] = "identity"
    window: int | None = 90
    clip_min: float = 0.30
    clip_max: float = 0.90
    # type == "bayes" only: the filter's matchup logit is over-dispersed
    # against outcomes (slope 0.78, intercept +0.10 on 2016-2021 serve
    # points); the stem uses intercept + slope * logit for the mean and
    # for each posterior draw. Defaults leave the logit untouched.
    calib_intercept: float = 0.0
    calib_slope: float = 1.0
    # Compress the favorite-underdog serve gap toward the pair mean (1.0 = off).
    # Preserves serve levels; only narrows the between-player differential.
    gap_shrink: float = 1.0
    # Per-(surface, circuit) calibration offset added to `p`, keyed
    # "<surface>/<circuit>" e.g. {"Hard/tour": 0.011, "Clay/chal": -0.006}.
    # A cell absent from the table gets no correction.
    #
    # Applied AFTER gap_shrink and after the clip that follows it, so a
    # measured +1.0pp gap is corrected by +1.0pp rather than by
    # gap_shrink x 1.0pp — otherwise the same table would deliver its full
    # value where gap_shrink is off (the default) and a compressed value where
    # it is on, making the correction's magnitude depend on an unrelated knob.
    surface_circuit_offset: dict[str, float] = {}
    # Used only when type == "matchup"
    feature_columns: list[str] = []
    match_level_columns: list[str] = []
    regressor: MatchupServeRegressorConfig = MatchupServeRegressorConfig()
    # Used only when type == "score_state"
    model_type: Literal[
        "logistic", "xgboost", "bayesian_logistic", "hierarchical_boosted",
    ] = "logistic"
    # Posterior draws for distributional model types. `bayesian_logistic`
    # emits this many samples of `p` per match; the projector runs the chain
    # once per draw and averages, so cost is linear in this number. Ignored by
    # the point model types, which always report a single draw.
    posterior_draws: int = 200
    posterior_seed: int = 0
    match_level_features: list[str] = []
    point_level_features: list[str] = []
    params: dict[str, Any] = {}

    # Used only when type == "two_level". Three components of the serve tree,
    # each carrying its OWN feature set so each can be an independent FS
    # target. Deliberately not folded into match_level_features/
    # point_level_features: sharing one list across the components would
    # hardwire the assumption the decomposition exists to test, and
    # `first_in`'s variation is measured orthogonal to blended p
    # (r = -0.032), so it is the component most likely to want a different set.
    #
    # `first_in` is fit at (match, server) grain and is state-invariant by
    # measurement (+0.19pp on the sets-won axis at t=1.8, against +5.42pp at
    # t=50.3 for win_first on the same rows). It therefore takes no
    # STATE-DERIVABLE point features — there is no ScoreState to evaluate them
    # at — but it does take the MATCH-CONSTANT ones. That distinction is not a
    # nicety: the surface one-hots live only in the point pool (no registered
    # match-level surface indicator exists), they are constant within a match,
    # and first-serve rate plausibly varies by surface. Excluding the whole
    # point list would silently starve the component of its one route to them.
    first_in_match_features: list[str] = []
    first_in_point_features: list[str] = []
    first_in_params: dict[str, Any] = {}
    win_first_match_features: list[str] = []
    win_first_point_features: list[str] = []
    win_second_match_features: list[str] = []
    win_second_point_features: list[str] = []


def _as_metric_list(v: Any) -> Any:
    """Accept a bare metric name as shorthand for a one-element objective list."""
    return [v] if isinstance(v, str) else v


class IIDMetricsConfig(BaseModel):
    """Metric configuration for the IID projector.

    `total_lines` / `spread_lines` / the include flags are REPORTING: they select
    which metrics get computed. `objective` is the optimization target for
    `mvp tune`, mirroring the classification `metrics.objective` — declared in the
    config rather than passed per-invocation so the tuned objective can't diverge
    from the config that drives the run, and so it's recorded in the fingerprint.
    """

    include_classification: bool = True
    include_regression: bool = True
    total_lines: list[float] = [18.5, 19.5, 20.5, 21.5, 22.5, 23.5, 24.5, 25.5]
    spread_lines: list[float] = [-5.5, -4.5, -3.5, -2.5, -1.5, 1.5, 2.5, 3.5, 4.5, 5.5]
    # No default: an unset objective is a hard error at tune time, never a silent
    # fallback (the previous `mae` default optimized a point-estimate metric while
    # the feature set had been selected on a distributional one). A multi-element
    # list = multi-objective (Pareto) tuning.
    objective: Annotated[list[MetricName] | None, BeforeValidator(_as_metric_list)] = None

    @model_validator(mode="after")
    def _validate_objective(self) -> "IIDMetricsConfig":
        if self.objective is None:
            return self
        if not self.objective:
            raise ValueError("metrics.objective must be non-empty when set")
        if len(self.objective) != len(set(self.objective)):
            raise ValueError(
                f"metrics.objective has duplicate metrics: {self.objective}"
            )
        # The tune reads its trial value out of the projection runner's metrics
        # dict, which holds COMPOSED metrics only. A non-chain name there either
        # KeyErrors one trial in (point/branch names the runner never emits) or
        # -- worse -- silently tunes something else: `log_loss` exists in that
        # dict at MATCH-WIN grain, so a config that selected features on the
        # point-grain log loss would tune the composed one. Refuse at load.
        from mvp.projection.iid.metric_registry import grain_of

        non_chain = [m for m in self.objective if grain_of(m) != "chain"]
        if non_chain:
            raise ValueError(
                f"metrics.objective must name composed (chain-grain) metrics; "
                f"{non_chain} are not. The projection runner does not emit "
                f"them, so a tune would fail or optimise a different quantity."
            )
        return self


class IIDProjectionConfig(BaseModel):
    """Complete IID projection configuration."""

    description: str | None = None
    data: DataConfig
    features: FeaturesConfig
    serve_model: ServeModelConfig = ServeModelConfig()
    validation: ValidationConfig = ValidationConfig()
    metrics: IIDMetricsConfig = IIDMetricsConfig()

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "IIDProjectionConfig":
        data: dict[str, Any] = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "IIDProjectionConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())


class ScoreStateModelConfig(BaseModel):
    """Score-state-dependent serve model configuration.

    Operates at point grain rather than match grain. Feature inputs mix
    match-level (broadcast to every point via server/returner perspective)
    and point-level (varying per point). Output: P(point_won_by_server | features).
    """

    type: Literal["logistic", "xgboost"] = "logistic"
    match_level_features: list[str] = []  # FeatureEngine specs, server-perspective
    point_level_features: list[str] = []  # columns from match_beats_points.parquet
    params: dict[str, Any] = {}


class ServeDiscoveryFeaturesConfig(BaseModel):
    """Candidate pool + base set for score-state serve forward selection.

    Empty candidate lists default to the full pool — match-level candidates
    come from the registered feature engine (matching classification /
    projection / IID FS behavior); point-level candidates come from the
    `match_beats_points.parquet` raw columns plus the registered derived
    features in `score_state_features.DERIVED_POINT_FEATURES`.
    """

    # Base sets: always included in every candidate model
    base_match_level_features: list[str] = []
    base_point_level_features: list[str] = []
    # Candidate pools: FS iterates over these looking for additions that improve the score.
    # Empty list → expand to full pool.
    candidate_match_level_features: list[str] = []
    candidate_point_level_features: list[str] = []
    # Optional blocklists applied AFTER pool resolution (so they work whether
    # the candidate list is explicit or expanded from the default pool).
    exclude_match_level_features: list[str] = []
    exclude_point_level_features: list[str] = []
    # Window sizes passed through to get_all_feature_specs when expanding the
    # default match-level pool. None = use the shared DEFAULT_day_windows
    # ([0, 7, 14, 30, 60, 90, 180, 365]) from model.discovery.discover.
    window_sizes: list[int] | None = None
    max_features: int | None = None  # cap on total selected features (base + FS additions)


# The two-level serve model's three arms (the code's "components"), in the
# order `two_level_serve_model.COMPONENTS` declares them. Restated here rather
# than imported: this module is the schema layer, and importing the estimator
# would pull polars and the whole serve_model stack into every config load.
# `serve_component`'s Literal below is the same list for the same reason.
SERVE_ARMS: tuple[str, ...] = ("first_in", "win_first", "win_second")


class JointArmConfig(BaseModel):
    """One arm's share of a joint forward selection (spec: #111).

    Empty lists keep their existing single-arm meanings: an empty match list
    expands to the full feature pool, an empty point list to the default point
    pool. `max_features` is an OPTIONAL per-arm cap on top of the run-wide
    `features.max_features`, so an arm that keeps losing rounds cannot be
    starved and an arm at its cap costs no compute.
    """

    candidate_match_level_features: list[str] = []
    candidate_point_level_features: list[str] = []
    max_features: int | None = None


class JointSelectionConfig(BaseModel):
    """The arms one run searches at once, in tie-break order.

    Insertion order is load-bearing: a round scores every (arm, candidate) pair
    on one chain metric and resolves an exact tie by arm order, then candidate
    name, so the ranking is the same on every machine.
    """

    arms: dict[str, JointArmConfig]


def substitute_arm_lists(
    serve_model: ServeModelConfig,
    lists: Mapping[str, tuple[list[str], list[str]]],
) -> ServeModelConfig:
    """A copy of `serve_model` with the named arms' feature lists replaced.

    The one place an arm's `(match, point)` pair is written into a two-level
    serve model. Both callers need it and must agree: `_build_candidate_model`
    at fit time and `_two_level_serve_block` at promotion time. They used to
    hand-mirror the same three-branch `if/elif`, which is a silent-wrong
    waiting to happen — FS would select against one model and promote another.

    Arms absent from `lists` keep the values the config gave them, which is how
    a run's held arms are carried forward.
    """
    out = serve_model.model_copy(deep=True)
    for name, (match_level, point_level) in lists.items():
        if name not in SERVE_ARMS:
            raise ValueError(
                f"unknown serve arm {name!r}; expected one of "
                "first_in / win_first / win_second"
            )
        setattr(out, f"{name}_match_features", list(match_level))
        setattr(out, f"{name}_point_features", list(point_level))
    return out


class ServeDiscoveryConfig(BaseModel):
    """Forward-selection discovery for the score-state serve model.

    Scores candidates using a single model form (default: logistic for speed).
    Optionally re-trains all `model_forms` on the selected feature set at the
    end to compare forms.
    """

    description: str | None = None
    data: DataConfig
    # FS runs at point-grain; sizes here are row counts in
    # match_beats_points.parquet (millions of rows).
    point_validation: ValidationConfig = ValidationConfig()
    # Match-grain validation, emitted verbatim into the promoted IID projection
    # config. The projection runner operates on one row per match.
    validation: ValidationConfig = ValidationConfig()
    features: ServeDiscoveryFeaturesConfig = ServeDiscoveryFeaturesConfig()
    # Two-level FS: which component of the serve tree this run selects for.
    #
    # None (default) selects for the single-level score-state model — existing
    # behaviour, untouched. When set, FS builds the TWO-LEVEL estimator and
    # perturbs ONLY the named component's feature list, holding the other two at
    # whatever `serve_model` below specifies. The score stays the configured
    # metric (the chain metric in practice), so each component is selected
    # against the thing that actually gets scored rather than a per-branch
    # proxy: the components multiply, so three locally-good fits can compose
    # into a worse whole.
    #
    # Each component's result is therefore CONDITIONAL on the other two's
    # current sets. Re-running a component after the others have moved is
    # legitimate and expected; `serve_model` is what carries their latest
    # selected sets between runs.
    serve_component: Literal["first_in", "win_first", "win_second"] | None = None
    # Joint FS across two or three arms at once (spec: #111), mutually
    # exclusive with `serve_component` above. Each round scores every
    # (arm, candidate) pair on ONE chain metric and adds the winner to the arm
    # it was scored in, so the metric decides which arm a feature belongs to
    # instead of the order the operator happened to run the components in.
    # Arms not listed here are held at their `serve_model` lists exactly as a
    # component run holds its other two.
    joint_selection: JointSelectionConfig | None = None
    # The two-level estimator this run selects a component of. Ignored when
    # `serve_component` is None. Its non-selected components supply the fixed
    # sets; the selected component's lists are overridden per candidate.
    serve_model: ServeModelConfig | None = None
    # Model form used to score candidates during FS (kept simple/fast).
    scoring_model: ScoreStateModelConfig = ScoreStateModelConfig(type="logistic")
    # All forms to compare on the final selected feature set (inherits params from model_params
    # if present, else library defaults).
    model_forms: list[Literal["logistic", "xgboost"]] = ["logistic", "xgboost"]
    model_params: dict[str, dict[str, Any]] = {}  # per-form params overrides
    metrics: IIDMetricsConfig = IIDMetricsConfig()
    metric: MetricName = "log_loss"
    selection_method: Literal["forward"] = "forward"
    # ABSOLUTE improvement required to accept a candidate, not fractional.
    # None (the default) resolves per metric via `resolved_min_delta()`:
    # one flat value makes a run halt earlier or later purely as a function
    # of which `metric:` a config names, since CRPS runs near 3.4 while
    # `branch_rate_wmse` is a squared residual on a [0, 1] rate. An explicit
    # value in the config still wins.
    min_delta: float | None = None
    # Cap on training rows per fold during candidate scoring (point-grain path only).
    # Has no effect when metric is a chain metric — use fs_match_subsample instead.
    # Final-form re-eval always runs on the full slice so reported metrics are honest.
    fs_train_subsample: int | None = None
    # Cap on training MATCHES per fold during candidate chain scoring (chain-metric path).
    # Mirrors fs_train_subsample for the match-grain path used by iid_total_cal etc.
    # Final-form re-eval always runs on the full slice so reported metrics are honest.
    fs_match_subsample: int | None = None
    fs_subsample_seed: int = 42
    # Number of candidates to score in parallel (chain-metric path only).
    # Uses threading — XGBoost releases the GIL during BLAS/tree ops.
    # n_jobs in scoring_model.params defaults to 1; n_parallel_candidates * n_jobs
    # must not exceed the logical processor count.
    n_parallel_candidates: int = 1
    # Which scorer a chain run uses (spec: #107). "fixed" is today's scorer:
    # every candidate is scored at the model's configured gap scale, which
    # rewards dispersion from any source rather than ordering alone. "proxy"
    # and "grid" fit the gap shrink on each fold's TRAIN side first, so a
    # candidate competes on the ordering it adds. The grid is the fitter's
    # DEFAULT_GRID and the proxy constant its default; neither gets a knob
    # here, because a run that wants a different one wants the fitter called
    # by hand, not an FS run nobody can reproduce from the yaml.
    chain_shrink: Literal["fixed", "proxy", "grid"] = "fixed"

    def joint_arms(self) -> list[str]:
        """The arms this run searches, in config (tie-break) order. Empty for a
        component run, which is the one-arm case of the same loop."""
        if self.joint_selection is None:
            return []
        return list(self.joint_selection.arms)

    def resolved_min_delta(self) -> float:
        """This config's `min_delta`, or the metric's scale-appropriate default.

        Mirrors `DiscoveryOptions.resolved_min_delta` on the classification
        side for the same reason: the threshold is absolute, so its right value
        tracks the metric's magnitude.
        """
        from mvp.projection.iid.metric_registry import default_serve_min_delta

        if self.min_delta is not None:
            return self.min_delta
        return default_serve_min_delta(self.metric)

    @model_validator(mode="after")
    def _validate_joint_selection(self) -> "ServeDiscoveryConfig":
        """A joint run is one kind of run, scored on one comparable number.

        Runs FIRST among the after-validators so a joint config's errors name
        `joint_selection` rather than `serve_component`: `_validate_branch_metric`
        would otherwise greet a joint config carrying a branch metric with
        "requires serve_component to be set", which is advice that contradicts
        rule 1. A no-op for every component run.

        The shared `features` lists are refused rather than reinterpreted
        (rules 6 and 7). Silently applying them would pin a feature into, or
        offer it to, an arm that never asked for it — and in joint mode there
        is no single arm they could sensibly mean.
        """
        from mvp.projection.iid.metric_registry import chain_metric_names, grain_of

        if self.joint_selection is None:
            return self
        if self.serve_component is not None:
            raise ValueError(
                "joint_selection and serve_component are mutually exclusive"
            )
        if self.serve_model is None or self.serve_model.type != "two_level":
            raise ValueError(
                "joint_selection requires serve_model.type == 'two_level'"
            )
        for name in self.joint_selection.arms:
            if name not in SERVE_ARMS:
                raise ValueError(
                    f"joint_selection.arms.{name}: not one of "
                    "first_in / win_first / win_second"
                )
        if len(self.joint_selection.arms) < 2:
            raise ValueError(
                "joint_selection needs at least two arms; use serve_component "
                "for a single arm"
            )
        grain = grain_of(self.metric)
        if grain != "chain":
            raise ValueError(
                f"metric {self.metric!r} is {grain}-grain; joint_selection "
                f"scores every arm on one chain metric (one of: "
                f"{', '.join(sorted(chain_metric_names()))})"
            )
        if (
            self.features.base_match_level_features
            or self.features.base_point_level_features
        ):
            raise ValueError(
                "joint_selection: features.base_match_level_features / "
                "base_point_level_features must be empty; pin per arm via "
                "serve_model.<arm>_match_features / <arm>_point_features"
            )
        if (
            self.features.candidate_match_level_features
            or self.features.candidate_point_level_features
        ):
            raise ValueError(
                "joint_selection: features.candidate_match_level_features / "
                "candidate_point_level_features must be empty; list candidates "
                "per arm under joint_selection.arms.<arm>"
            )
        return self

    @model_validator(mode="after")
    def _validate_branch_metric(self) -> "ServeDiscoveryConfig":
        """A branch metric scores the component this run selects for, on that
        component's own target — so the metric and the component have to agree
        about which target that is.

        The scorer dispatches on the COMPONENT, so a mismatched pair does not
        fail: `branch_log_loss` with `serve_component: first_in` would score
        the weighted rate MSE and label every artifact -- history, checkpoint,
        stop record, the promoted config's provenance -- `branch_log_loss`.
        Both are minimize, so nothing downstream notices. That is exactly the
        confound that makes two arms incomparable, so it is refused here.
        """
        from mvp.projection.iid.metric_registry import (
            branch_rate_metrics,
            is_branch_metric,
        )

        if not is_branch_metric(self.metric):
            return self
        if self.serve_component is None:
            raise ValueError(
                f"metric {self.metric!r} is per-branch and requires "
                f"serve_component to be set (first_in / win_first / win_second)"
            )
        rate_metrics = branch_rate_metrics()
        if self.metric in rate_metrics and self.serve_component != "first_in":
            raise ValueError(
                f"metric {self.metric!r} scores the first-serve-in rate and "
                f"requires serve_component=first_in; got "
                f"{self.serve_component!r}"
            )
        if self.metric not in rate_metrics and self.serve_component == "first_in":
            raise ValueError(
                f"metric {self.metric!r} scores point_won_by_server on a win "
                f"branch; serve_component=first_in must use one of "
                f"{sorted(rate_metrics)}"
            )
        return self

    @model_validator(mode="after")
    def _validate_chain_shrink(self) -> "ServeDiscoveryConfig":
        """Fitting a gap shrink only means anything to a run scored through the
        chain.

        A point or blind branch run never reaches the chain scorer, so a
        non-fixed value there would be silently ignored — an hour of compute
        spent producing a ranking the operator believes was calibrated.
        Refused at load instead, naming the grain the metric it was given
        actually has. Pydantic has already rejected anything outside the
        literal.
        """
        from mvp.projection.iid.metric_registry import grain_of, is_chain_metric

        if self.chain_shrink != "fixed" and not is_chain_metric(self.metric):
            raise ValueError(
                f"chain_shrink={self.chain_shrink!r} requires a chain metric "
                f"(iid_* family); got metric={self.metric!r}, which is a "
                f"{grain_of(self.metric)} metric"
            )
        return self

    @model_validator(mode="after")
    def _validate_parallelism(self) -> "ServeDiscoveryConfig":
        import os
        n_jobs = self.scoring_model.params.get("n_jobs", 1)
        cpu_count = os.cpu_count() or 1
        product = self.n_parallel_candidates * n_jobs
        if product > cpu_count:
            raise ValueError(
                f"n_parallel_candidates ({self.n_parallel_candidates}) * n_jobs ({n_jobs})"
                f" = {product}, which exceeds logical processor count ({cpu_count})."
                f" Reduce one or both."
            )
        return self

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "ServeDiscoveryConfig":
        data: dict[str, Any] = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "ServeDiscoveryConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())

    def to_iid_projection_config_dict(
        self,
        selected_match_level: list[str],
        selected_point_level: list[str],
        model_type: str = "logistic",
        model_params: dict[str, Any] | None = None,
        selected_by_arm: Mapping[str, tuple[list[str], list[str]]] | None = None,
    ) -> dict[str, Any]:
        """Emit a runnable IIDProjectionConfig-compatible dict from FS output.

        A joint run passes `selected_by_arm` and every searched arm's lists
        land at once; `selected_match_level` / `selected_point_level` are then
        the union used for the engine's include list. A component run passes
        neither and is unaffected.

        `features.include` gets the selected match-level specs plus ONLY the
        `opp_` columns the swap (B-serves) perspective actually reads — match-
        constant point features come straight from `match_beats_points.parquet`
        at fit time and are not engine-computed.

        The partner set comes from `swap_side_partner_specs` rather than pairing
        every `player_X` with an `opp_X`. Two reasons, one of them a crash:

        A diff-style feature (registry `mirror=False`) has no `opp_` column —
        its swap value is the NEGATION of the player value — so pairing emits a
        spec nothing reads. Harmless for registry-backed features, since the
        engine can compute `opp_X_diff` when the base name is registered, just
        wasted work.

        It is NOT harmless for TRANSFORM outputs. Those register explicit column
        names (`style_matchup_retrieval._OUTPUTS`) and only the `player_` diff
        exists; `opp_vs_opp_style_resid_flat_diff` is in no registry and no
        transform, so `_resolve_dependencies` falls through to
        `registry.get(base_name)` and raises KeyError. A promoted config that
        selected such a feature was unrunnable.

        Mirror features get their partner in BOTH directions. A selection can be
        `opp_`-prefixed — the shortlist's composite-side expansion puts those in
        the candidate pool and FS picks them (`opp_surface_matches(days=30)`) —
        and the swap side then reads the `player_` column. Emitting only the
        `opp_` direction under-declares the config: it still runs, because the
        engine computes player-side first and mirrors to get `opp_`, but the
        include list would be relying on that ordering instead of naming what
        `_match_feature_values` reads.
        """
        from mvp.model.engine import parse_feature_spec

        from mvp.projection.iid.serve_model import swap_side_partner_specs

        include_specs: list[str] = []
        seen: set[str] = set()

        for spec in selected_match_level:
            _prefix, _base, full_name, params = parse_feature_spec(spec)
            if params:
                param_str = ", ".join(f"{k}={v}" for k, v in params.items())
                own_spec = f"{full_name}({param_str})"
            else:
                own_spec = full_name
            if own_spec not in seen:
                include_specs.append(own_spec)
                seen.add(own_spec)

        for partner in swap_side_partner_specs(selected_match_level):
            if partner not in seen:
                include_specs.append(partner)
                seen.add(partner)

        joint_arms = self.joint_arms()
        if (
            self.serve_component is None
            and not joint_arms
            and selected_by_arm is None
        ):
            serve_block: dict[str, Any] = {
                "type": "score_state",
                "model_type": model_type,
                "match_level_features": selected_match_level,
                "point_level_features": selected_point_level,
                "params": model_params or {},
            }
        else:
            serve_block = self._two_level_serve_block(
                selected_match_level, selected_point_level,
                model_type, model_params,
                selected_by_arm=selected_by_arm,
            )
            # The engine has to compute EVERY component's match features, not
            # just the selected component's — the non-selected components are
            # carried forward and their features are read at predict time. An
            # include list built from the selected component alone would emit a
            # config that loads, then fails at predict on a missing column.
            from mvp.model.engine import parse_feature_spec as _parse

            component_specs = (
                serve_block["first_in_match_features"]
                + serve_block["win_first_match_features"]
                + serve_block["win_second_match_features"]
            )
            for spec in component_specs:
                _prefix, _base, full_name, params = _parse(spec)
                if params:
                    ps = ", ".join(f"{k}={v}" for k, v in params.items())
                    own_spec = f"{full_name}({ps})"
                else:
                    own_spec = full_name
                if own_spec not in seen:
                    include_specs.append(own_spec)
                    seen.add(own_spec)
            # Partners through the SAME helper the one-level path above uses.
            # This loop used to pair `player_X` with `opp_X` inline and
            # unconditionally, which re-introduced the crash this docstring
            # describes: a transform-output diff has only a `player_` column, so
            # the emitted `opp_` partner is in no registry and no transform and
            # `_resolve_dependencies` raises KeyError on it. Every two-level
            # config that selected `player_vs_opp_style_resid_flat_diff` — all
            # four Phase-B arms — was unrunnable.
            for partner in swap_side_partner_specs(component_specs):
                if partner not in seen:
                    include_specs.append(partner)
                    seen.add(partner)

        from mvp.projection.iid.metric_registry import grain_of

        base_description = (
            self.description
            or "IID score-state projection from forward-selected features"
        )
        # Provenance: the two non-selected components come from `serve_model`
        # with none of their own, so at minimum say what selected THIS one.
        if joint_arms:
            selected_by = (
                f"joint({','.join(joint_arms)}) selected on {self.metric}"
            )
        elif self.serve_component:
            selected_by = f"{self.serve_component} selected on {self.metric}"
        else:
            selected_by = f"selected on {self.metric}"
        # Carry the FS objective forward as the tune objective -- but only when
        # it is composed. The runner emits composed metrics only, so a
        # point/branch name here would fail a trial in, or silently tune
        # match-win `log_loss` instead of the point-grain one that was
        # selected on. Omitting leaves `mvp tune` to raise its own error
        # naming this file and the field, which is the honest failure.
        metrics_block: dict[str, Any] = {
            "total_lines": list(self.metrics.total_lines),
            "spread_lines": list(self.metrics.spread_lines),
        }
        if self.metrics.objective:
            metrics_block["objective"] = list(self.metrics.objective)
        elif grain_of(self.metric) == "chain":
            metrics_block["objective"] = [self.metric]

        return {
            "description": f"{base_description} [{selected_by}]",
            "data": self.data.model_dump(),
            "features": {"include": include_specs},
            "metrics": metrics_block,
            "serve_model": serve_block,
            "validation": self.validation.model_dump(),
        }

    def _two_level_serve_block(
        self,
        selected_match_level: list[str],
        selected_point_level: list[str],
        model_type: str,
        model_params: dict[str, Any] | None,
        selected_by_arm: Mapping[str, tuple[list[str], list[str]]] | None = None,
    ) -> dict[str, Any]:
        """Promoted `serve_model` for a two-level FS run.

        Takes the run's `serve_model` as the base — which holds the held arms'
        current sets — and substitutes the selected lists into the arm(s) this
        run was selecting for: every entry of `selected_by_arm` for a joint
        run, otherwise the single `serve_component`. Emitting
        `type: score_state` here (the single-level default) would silently
        discard the arm structure and the held arms entirely, producing a
        config that runs but is not the model FS just scored.

        Substitution goes through `substitute_arm_lists`, the same helper
        `ServeDiscoverySelector._build_candidate_model` uses at fit time. The
        two must agree or FS would select against one model and promote
        another, which is why it is one function and not two.
        """
        if self.serve_model is None:
            raise ValueError(
                "serve_component is set but serve_model is missing — there is "
                "nothing to carry the non-selected components forward"
            )
        if selected_by_arm is None:
            selected_by_arm = {
                self.serve_component: (selected_match_level, selected_point_level)
            }
        block = substitute_arm_lists(self.serve_model, selected_by_arm).model_dump()
        block["type"] = "two_level"
        block["model_type"] = model_type
        block["params"] = model_params or {}
        return block


class IIDDiscoveryFeaturesConfig(BaseModel):
    """Forward-selection candidate pool configuration."""

    include: list[str] = []           # optional allowlist of candidate specs
    exclude: list[str] = []           # optional blocklist of candidate specs
    base: list[str] = []              # features always kept (FS starts from these)
    window_sizes: list[int] = [60, 90]
    max_features: int | None = None   # cap on FS depth


class IIDDiscoveryConfig(BaseModel):
    """Forward-selection discovery configuration for the IID matchup serve model.

    Drives the IIDProjectionDiscovery orchestrator: defines the data slice,
    candidate pool, validation folds, regressor, and target metric.
    """

    description: str | None = None
    data: DataConfig
    validation: ValidationConfig = ValidationConfig()
    serve_model: ServeModelConfig = ServeModelConfig(type="matchup")
    metrics: IIDMetricsConfig = IIDMetricsConfig()
    features: IIDDiscoveryFeaturesConfig = IIDDiscoveryFeaturesConfig()
    metric: Literal[
        "mae",
        "rmse",
        "log_loss",
        "iid_crps_total_games",
        "iid_total_cal",
        "iid_spread_cal",
    ] = "mae"
    selection_method: Literal["forward"] = "forward"

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "IIDDiscoveryConfig":
        data: dict[str, Any] = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "IIDDiscoveryConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())

    def to_iid_config_dict(self, selected_specs: list[str]) -> dict[str, Any]:
        """Emit a runnable `IIDProjectionConfig`-compatible dict.

        `features.include` gets BOTH the player_* and opp_* versions of each
        selected spec (the matchup serve model's swap mechanism requires both
        perspectives loaded at fit and predict time). `serve_model.feature_columns`
        gets the resolved column name for each selected spec (row-player
        perspective, whatever prefix the FS picked).
        """
        from mvp.model.engine import build_column_name, parse_feature_spec

        include_specs: list[str] = []
        feature_columns: list[str] = []
        seen_specs: set[str] = set()

        for spec in selected_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            feature_columns.append(col_name)

            # Derive the swapped-perspective spec so the engine loads both
            # versions (needed by MatchupServeModel's two-perspective fit).
            if prefix == "player":
                swap_full = f"opp_{base_name}"
            elif prefix == "opp":
                swap_full = f"player_{base_name}"
            else:
                swap_full = full_name  # match-level, no swap

            if params:
                param_str = ", ".join(f"{k}={v}" for k, v in params.items())
                own_spec = f"{full_name}({param_str})"
                swap_spec = f"{swap_full}({param_str})"
            else:
                own_spec = full_name
                swap_spec = swap_full

            for s in (own_spec, swap_spec):
                if s not in seen_specs:
                    include_specs.append(s)
                    seen_specs.add(s)

        return {
            "description": (
                self.description or "IID matchup projection from forward-selected features"
            ),
            "data": self.data.model_dump(),
            "features": {"include": include_specs},
            "serve_model": {
                "type": "matchup",
                "window": self.serve_model.window,
                "clip_min": self.serve_model.clip_min,
                "clip_max": self.serve_model.clip_max,
                "feature_columns": feature_columns,
                "match_level_columns": list(self.serve_model.match_level_columns),
                "regressor": self.serve_model.regressor.model_dump(),
            },
            "validation": self.validation.model_dump(),
            "metrics": self.metrics.model_dump(),
        }
