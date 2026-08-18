"""Configuration schema for the IID projection runner."""

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

    type: Literal["identity", "matchup", "score_state", "two_level"] = "identity"
    window: int | None = 90
    clip_min: float = 0.30
    clip_max: float = 0.90
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
    min_delta: float = 0.0001  # minimum fractional improvement to accept a candidate
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
    ) -> dict[str, Any]:
        """Emit a runnable IIDProjectionConfig-compatible dict from FS output.

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

        if self.serve_component is None:
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
            )
            # The engine has to compute EVERY component's match features, not
            # just the selected component's — the non-selected components are
            # carried forward and their features are read at predict time. An
            # include list built from the selected component alone would emit a
            # config that loads, then fails at predict on a missing column.
            from mvp.model.engine import parse_feature_spec as _parse

            for spec in (
                serve_block["first_in_match_features"]
                + serve_block["win_first_match_features"]
                + serve_block["win_second_match_features"]
            ):
                prefix, base_name, full_name, params = _parse(spec)
                swap = (
                    f"opp_{base_name}" if prefix == "player"
                    else f"player_{base_name}" if prefix == "opp"
                    else full_name
                )
                if params:
                    ps = ", ".join(f"{k}={v}" for k, v in params.items())
                    pair = (f"{full_name}({ps})", f"{swap}({ps})")
                else:
                    pair = (full_name, swap)
                for s in pair:
                    if s not in seen:
                        include_specs.append(s)
                        seen.add(s)

        return {
            "description": (
                self.description
                or "IID score-state projection from forward-selected features"
            ),
            "data": self.data.model_dump(),
            "features": {"include": include_specs},
            # Carry the FS objective forward as the tune objective. Without it the
            # promoted config states nothing about what its features were selected
            # against, and `mvp tune` would have to be told per invocation.
            "metrics": {"objective": [self.metric]},
            "serve_model": serve_block,
            "validation": self.validation.model_dump(),
        }

    def _two_level_serve_block(
        self,
        selected_match_level: list[str],
        selected_point_level: list[str],
        model_type: str,
        model_params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Promoted `serve_model` for a per-component FS run.

        Takes the run's `serve_model` as the base — which holds the other two
        components' current sets — and substitutes the selected lists into the
        component this run was selecting for. Emitting `type: score_state` here
        (the single-level default) would silently discard the component
        structure and the other two components entirely, producing a config that
        runs but is not the model FS just scored.

        Substitution mirrors `ServeDiscoverySelector._build_candidate_model`
        exactly; the two must agree or FS would select against one model and
        promote another.
        """
        if self.serve_model is None:
            raise ValueError(
                "serve_component is set but serve_model is missing — there is "
                "nothing to carry the non-selected components forward"
            )
        block = self.serve_model.model_dump()
        block["type"] = "two_level"
        block["model_type"] = model_type
        block["params"] = model_params or {}
        if self.serve_component == "first_in":
            block["first_in_match_features"] = list(selected_match_level)
            block["first_in_point_features"] = list(selected_point_level)
        elif self.serve_component == "win_first":
            block["win_first_match_features"] = list(selected_match_level)
            block["win_first_point_features"] = list(selected_point_level)
        elif self.serve_component == "win_second":
            block["win_second_match_features"] = list(selected_match_level)
            block["win_second_point_features"] = list(selected_point_level)
        else:  # pragma: no cover - Literal-constrained
            raise ValueError(f"unknown serve_component: {self.serve_component!r}")
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
