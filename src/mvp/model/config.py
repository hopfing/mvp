"""Experiment configuration schema."""


from datetime import date
from typing import Any, Literal

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class _StrictModel(BaseModel):
    """Base model with extra='forbid' so typos in YAML configs surface as validation errors."""

    model_config = ConfigDict(extra="forbid")


def get_filter_feature_specs(filters: dict[str, Any] | None) -> list[str]:
    """Return filter column names that are computed features (not raw columns).

    These must be included in the feature computation so filter columns
    are available even when they aren't selected model features. Filter
    keys that already exist as raw columns in matches.parquet are skipped
    even when they share a name with a registered passthrough feature
    (e.g. `best_of`, `surface`) — re-loading them would duplicate-join.
    """
    if not filters:
        return []

    import mvp.model.features  # noqa: F401 - triggers registration
    from mvp.common.base_job import get_data_root
    from mvp.model.registry import get_registry

    matches_path = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
    raw_cols = set(pl.scan_parquet(matches_path).collect_schema().names())
    registry = get_registry()
    known = set(registry.list_features())
    specs = []
    for col in filters:
        if col in raw_cols:
            continue
        # Check if the column (with player_ prefix stripped) is a registered feature
        if col.startswith("player_"):
            base = col[7:]
        elif col.startswith("opp_"):
            base = col[4:]
        else:
            base = col
        if base in known:
            specs.append(col)
    return specs


def apply_filters(df: pl.DataFrame, filters: dict[str, Any]) -> pl.DataFrame:
    """Apply equality, list, and range filters to a DataFrame.

    Filter types by value:
      - scalar: equality (col == value)
      - list: membership (col in values)
      - dict with min/max: range (col >= min, col <= max)
      - dict with abs_min/abs_max: absolute value range (abs(col) >= abs_min, abs(col) <= abs_max)
    """
    for col, value in filters.items():
        if isinstance(value, list):
            df = df.filter(pl.col(col).is_in(value))
        elif isinstance(value, dict):
            if "min" in value:
                df = df.filter(pl.col(col) >= value["min"])
            if "max" in value:
                df = df.filter(pl.col(col) <= value["max"])
            if "abs_min" in value:
                df = df.filter(pl.col(col).abs() >= value["abs_min"])
            if "abs_max" in value:
                df = df.filter(pl.col(col).abs() <= value["abs_max"])
        else:
            df = df.filter(pl.col(col) == value)
    return df


class DateRange(_StrictModel):
    """Date range for data selection."""

    start: date
    end: date

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> date:
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            return date.fromisoformat(v)
        raise ValueError(f"Cannot parse date: {v}")


class DataConfig(_StrictModel):
    """Data selection configuration."""

    date_range: DateRange
    filters: dict[str, Any] | None = None  # Applied pre-split to the whole dataset
    train_filters: dict[str, Any] | None = None  # Applied post-split to train fold only
    eval_filters: dict[str, Any] | None = None  # Applied post-split to test fold only
    # Exclude all incomplete-match reasons (W/O, RET, DEF, UNP) instead of
    # only W/O. Default behavior keeps RET/DEF/UNP rows because `won` is
    # well-defined for retirements. Set true to match the MTL completeness
    # gate when comparing an MTL run against a single-task baseline — without
    # this, the baseline trains/evaluates on a different (slightly larger)
    # row set than the MTL path, confounding the comparison. Implicitly true
    # whenever `mtl:` is set on the experiment config.
    exclude_incomplete: bool = False


class FeaturesConfig(_StrictModel):
    """Feature selection configuration."""

    include: list[str]
    compute_only: list[str] = []


class EnsembleBaseModelRef(_StrictModel):
    """Reference to a base model in an ensemble."""

    config: str
    weight: float = 1.0


class EnsembleParams(_StrictModel):
    """Ensemble-specific parameters."""

    strategy: Literal["average", "weighted_average", "stacking"] = "average"
    base_models: list[EnsembleBaseModelRef]
    meta_features: list[str] = []
    meta_model_params: dict[str, Any] = {}

    @model_validator(mode="after")
    def validate_stacking_no_weights(self) -> "EnsembleParams":
        if self.strategy == "stacking":
            for ref in self.base_models:
                if ref.weight != 1.0:
                    raise ValueError(
                        "weight is not allowed with strategy='stacking' "
                        "(meta-model learns coefficients)"
                    )
        if self.meta_features and self.strategy != "stacking":
            raise ValueError(
                "meta_features is only allowed with strategy='stacking'"
            )
        if self.meta_model_params and self.strategy != "stacking":
            raise ValueError(
                "meta_model_params is only allowed with strategy='stacking'"
            )
        return self


class ModelConfig(_StrictModel):
    """Model configuration."""

    type: Literal["xgboost", "logistic", "random_forest", "ensemble", "neural_net", "sequence"]
    params: dict[str, Any] | None = None


class ValidationConfig(_StrictModel):
    """Validation strategy configuration."""

    type: Literal[
        "walk_forward",
        "expanding_window",
        "sliding_window",
        "date_window",
        "date_sliding",
        "date_expanding",
    ] = "walk_forward"
    # For walk_forward (n_splits mode)
    n_splits: int = 5
    min_train_size: int = 50000
    test_size: int = 10000
    # For expanding_window (step_size mode)
    initial_train_size: int | None = None
    step_size: int | None = None
    # For sliding_window
    train_size: int | None = None
    # For date_window
    test_start: date | None = None
    # For date_sliding (train_months) and date_expanding (initial_train_months)
    train_months: int | None = None
    initial_train_months: int | None = None
    test_months: int | None = None

    @model_validator(mode="after")
    def _validate_date_splitter_params(self) -> "ValidationConfig":
        if self.type == "date_sliding":
            if self.initial_train_months is not None:
                raise ValueError(
                    "initial_train_months is for date_expanding; use train_months with date_sliding"
                )
            if self.train_months is None or self.test_months is None:
                raise ValueError(
                    "date_sliding requires train_months and test_months"
                )
        elif self.type == "date_expanding":
            if self.train_months is not None:
                raise ValueError(
                    "train_months is for date_sliding; use initial_train_months with date_expanding"
                )
            if self.initial_train_months is None or self.test_months is None:
                raise ValueError(
                    "date_expanding requires initial_train_months and test_months"
                )
        else:
            if (
                self.train_months is not None
                or self.initial_train_months is not None
                or self.test_months is not None
            ):
                raise ValueError(
                    f"train_months / initial_train_months / test_months are only valid "
                    f"with date_sliding or date_expanding, not {self.type}"
                )
        return self


class MetricsConfig(_StrictModel):
    """Metrics configuration."""

    primary: str = "log_loss"
    secondary: list[str] = ["accuracy", "brier_score", "roc_auc"]


class SampleWeightConfig(_StrictModel):
    """Sample weighting configuration."""

    type: Literal["recency"] = "recency"
    half_life_days: int


class CalibrationConfig(_StrictModel):
    """Calibration configuration applied at training time and at inference.

    Absence of this block = pooled-only behavior.

    **Consumed only by `mvp model` (ProductionPredictor) and `mvp run`** —
    `mvp tune` IGNORES this block. Tuning evaluates raw predictor
    discrimination; including Platt in the HP search loop would conflate two
    optimizations (HPs + calibrator fit) and produce brittle winners that
    "game" the calibrator. Choose your calibration scheme separately from
    HP search and let `mvp model` apply it post-hoc at training time.
    """

    method: Literal["platt", "isotonic", "asymm_isotonic"] = "platt"
    segments: list[str] | None = None
    min_n: int = 200
    # Asymmetric-isotonic only: negative-class weight in the isotonic fit
    # (lambda_over=1.0 reduces to standard isotonic). H51 Phase 2a is the
    # non-segmented variant; segmented asymm_isotonic isn't implemented yet,
    # so combining `method: asymm_isotonic` with `segments` raises in
    # make_calibrator().
    lambda_over: float = 1.0


class MTLConfig(_StrictModel):
    """Multi-task learning configuration.

    When set on `ExperimentConfig.mtl`, the runner derives auxiliary regression
    targets from match outcome data and applies a stricter completeness filter
    (all incomplete-match reasons excluded, not just walkovers). The model
    wrapper handles training with the multi-task objective; this config block
    only controls which auxiliary targets are materialized.

    Per-target loss weights are NOT specified here — they are HP-search
    dimensions tuned by Optuna during the HP sweep.
    """

    auxiliary_targets: list[
        Literal[
            "game_margin",
            "set_margin",
            "set_count",
            "total_pts_won_diff",
            "service_pts_won_pct_diff",
            "return_pts_won_pct_diff",
            "first_serve_won_pct_diff",
            "bp_save_pct_diff",
            "svc_serve_rating_diff",
            "ret_return_rating_diff",
            "set1_games_diff",
            "set2_games_diff",
            "duration_seconds",
            "wl_continuous_proxy",
        ]
    ]

    @field_validator("auxiliary_targets")
    @classmethod
    def at_least_one_aux(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("mtl.auxiliary_targets must list at least one target")
        if len(set(v)) != len(v):
            raise ValueError("mtl.auxiliary_targets must not contain duplicates")
        return v


class ExperimentConfig(_StrictModel):
    """Complete experiment configuration."""

    description: str | None = None
    target: Literal["won", "deciding_set"] = "won"
    data: DataConfig
    features: FeaturesConfig | None = None
    model: ModelConfig
    validation: ValidationConfig = ValidationConfig()
    metrics: MetricsConfig = MetricsConfig()
    sample_weight: SampleWeightConfig | None = None
    calibration: CalibrationConfig | None = None
    mtl: MTLConfig | None = None

    @model_validator(mode="after")
    def validate_features_required(self) -> "ExperimentConfig":
        if self.model.type != "ensemble" and self.features is None:
            raise ValueError("features is required for non-ensemble models")
        return self

    @model_validator(mode="after")
    def validate_mtl_compatibility(self) -> "ExperimentConfig":
        """MTL is implemented for XGBoost vector-leaf only.

        Reject combinations the runner can't dispatch (ensemble, sequence,
        non-XGB) at config load time so the user gets a clear error before
        any compute.
        """
        if self.mtl is not None and self.model.type != "xgboost":
            raise ValueError(
                f"MTL requires model.type='xgboost'; got {self.model.type!r}"
            )
        return self

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "ExperimentConfig":
        """Parse config from YAML string."""
        data = yaml.safe_load(yaml_str)
        data.pop("name", None)  # Ignore legacy name field
        data.pop("selection_history", None)  # Discovery-written metadata, not a config field
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str) -> "ExperimentConfig":
        """Load config from YAML file."""
        with open(path) as f:
            return cls.from_yaml(f.read())
