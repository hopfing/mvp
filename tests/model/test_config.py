"""Tests for experiment config schema."""

from datetime import date

import pytest

from mvp.model.config import ExperimentConfig


class TestExperimentConfig:
    """Tests for ExperimentConfig parsing."""

    def test_minimal_config(self):
        """Parse minimal valid config."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: xgboost
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.data.date_range.start == date(2020, 1, 1)
        assert config.data.date_range.end == date(2024, 12, 31)
        assert config.features.include == ["win_rate(days=30)"]
        assert config.model.type == "xgboost"

    def test_walk_forward_validation(self):
        """Parse walk-forward validation config."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: xgboost
validation:
  type: walk_forward
  n_splits: 5
  min_train_size: 50000
  test_size: 10000
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.validation.type == "walk_forward"
        assert config.validation.n_splits == 5
        assert config.validation.min_train_size == 50000
        assert config.validation.test_size == 10000

    def test_default_validation(self):
        """Default validation is walk_forward with sensible defaults."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: xgboost
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.validation.type == "walk_forward"
        assert config.validation.n_splits == 5

    def test_metrics_config(self):
        """Parse metrics configuration."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: xgboost
metrics:
  primary: log_loss
  secondary:
    - accuracy
    - brier_score
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.metrics.primary == "log_loss"
        assert "accuracy" in config.metrics.secondary

    def test_default_metrics(self):
        """Default metrics when not specified."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: xgboost
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.metrics.primary == "log_loss"
        assert "accuracy" in config.metrics.secondary

    def test_compute_only_features(self):
        """compute_only features are parsed but separate from include."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
  compute_only:
    - player_elo_surface_diff
model:
  type: logistic
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.features.include == ["win_rate(days=30)"]
        assert config.features.compute_only == ["player_elo_surface_diff"]

    def test_compute_only_defaults_empty(self):
        """compute_only defaults to empty list when omitted."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.features.compute_only == []

    def test_scoped_filters_default_none(self):
        """train_filters and eval_filters default to None."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.data.filters is None
        assert config.data.train_filters is None
        assert config.data.eval_filters is None

    def test_scoped_filters_parsed(self):
        """train_filters and eval_filters parse independently."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
  train_filters:
    circuit: [chal, tour, itf]
  eval_filters:
    circuit: [chal, tour]
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.data.filters is None
        assert config.data.train_filters == {"circuit": ["chal", "tour", "itf"]}
        assert config.data.eval_filters == {"circuit": ["chal", "tour"]}

    def test_unknown_field_in_data_rejected(self):
        """Typos in DataConfig field names surface as validation errors instead of being silently dropped."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
  train_fitlers:
    circuit: [chal, tour]
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
"""
        with pytest.raises(ValueError, match="train_fitlers"):
            ExperimentConfig.from_yaml(yaml_str)

    def test_filters_and_scoped_can_coexist(self):
        """filters, train_filters, eval_filters can all be set together."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
  filters:
    draw_type: singles
  train_filters:
    circuit: [chal, tour, itf]
  eval_filters:
    circuit: [chal, tour]
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.data.filters == {"draw_type": "singles"}
        assert config.data.train_filters == {"circuit": ["chal", "tour", "itf"]}
        assert config.data.eval_filters == {"circuit": ["chal", "tour"]}

    def test_offset_with_early_stopping_validates(self):
        """two_stage_fit slices base_margin with its watch split, so an offset
        and early stopping now combine."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - player_elo_surface_indoor_diff
    - win_rate(days=30)
model:
  type: xgboost
metrics:
  objective: [log_loss]
early_stopping:
  enabled: true
offset:
  feature: player_elo_surface_indoor_diff
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.offset.feature == "player_elo_surface_indoor_diff"
        assert config.early_stopping.enabled


class TestDateValidationSplitterParams:
    """Cross-type validators for date_sliding / date_expanding params."""

    _PREFIX = """
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
"""

    def test_date_sliding_valid(self):
        yaml_str = self._PREFIX + """
validation:
  type: date_sliding
  train_months: 12
  test_months: 3
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.validation.train_months == 12
        assert config.validation.test_months == 3

    def test_date_expanding_valid(self):
        yaml_str = self._PREFIX + """
validation:
  type: date_expanding
  initial_train_months: 12
  test_months: 12
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.validation.initial_train_months == 12
        assert config.validation.test_months == 12

    def test_date_sliding_rejects_initial_train_months(self):
        yaml_str = self._PREFIX + """
validation:
  type: date_sliding
  train_months: 12
  initial_train_months: 24
  test_months: 3
"""
        with pytest.raises(ValueError, match="initial_train_months is for date_expanding"):
            ExperimentConfig.from_yaml(yaml_str)

    def test_date_expanding_rejects_train_months(self):
        yaml_str = self._PREFIX + """
validation:
  type: date_expanding
  train_months: 12
  initial_train_months: 24
  test_months: 12
"""
        with pytest.raises(ValueError, match="train_months is for date_sliding"):
            ExperimentConfig.from_yaml(yaml_str)

    def test_date_sliding_requires_train_months(self):
        yaml_str = self._PREFIX + """
validation:
  type: date_sliding
  test_months: 3
"""
        with pytest.raises(ValueError, match="date_sliding requires"):
            ExperimentConfig.from_yaml(yaml_str)

    def test_date_expanding_requires_initial_train_months(self):
        yaml_str = self._PREFIX + """
validation:
  type: date_expanding
  test_months: 12
"""
        with pytest.raises(ValueError, match="date_expanding requires"):
            ExperimentConfig.from_yaml(yaml_str)

    def test_non_date_type_rejects_date_params(self):
        yaml_str = self._PREFIX + """
validation:
  type: expanding_window
  initial_train_size: 25000
  step_size: 25000
  train_months: 12
"""
        with pytest.raises(ValueError, match="only valid with date_sliding"):
            ExperimentConfig.from_yaml(yaml_str)


class TestHoldoutEnd:
    """`data.date_range.holdout_end`: the held-out year selection never sees."""

    _PREFIX = """
data:
  date_range:
    start: "2021-01-01"
    end: "2024-12-31"
    holdout_end: {holdout_end}
features:
  include:
    - win_rate(days=30)
model:
  type: logistic
"""
    _DATE_VALIDATION = """
validation:
  type: date_expanding
  initial_train_months: 12
  test_months: 12
"""

    def _yaml(self, holdout_end: str, validation: str | None = None) -> str:
        return (
            self._PREFIX.format(holdout_end=holdout_end)
            + (self._DATE_VALIDATION if validation is None else validation)
        )

    def test_parsed_like_end(self):
        config = ExperimentConfig.from_yaml(self._yaml('"2025-12-31"'))
        assert config.data.date_range.holdout_end == date(2025, 12, 31)

    def test_defaults_to_none(self):
        config = ExperimentConfig.from_yaml(
            self._PREFIX.replace("    holdout_end: {holdout_end}\n", "")
        )
        assert config.data.date_range.holdout_end is None

    @pytest.mark.parametrize("value", ['"2024-12-31"', '"2024-06-30"'])
    def test_not_after_end_raises(self, value):
        with pytest.raises(ValueError, match="holdout_end must be after end"):
            ExperimentConfig.from_yaml(self._yaml(value))

    @pytest.mark.parametrize("value", ['"2026-01-01"', '"2026-06-30"'])
    def test_on_or_after_betting_start_raises(self, value):
        with pytest.raises(ValueError, match="holdout_end must be before the betting start 2026-01-01"):
            ExperimentConfig.from_yaml(self._yaml(value))

    def test_gap_before_betting_start_warns(self, caplog):
        with caplog.at_level("WARNING", logger="mvp.model.config"):
            ExperimentConfig.from_yaml(self._yaml('"2025-12-29"'))
        assert (
            "holdout_end leaves 2 days before the betting start that no period reads"
            in caplog.text
        )

    def test_no_gap_does_not_warn(self, caplog):
        with caplog.at_level("WARNING", logger="mvp.model.config"):
            ExperimentConfig.from_yaml(self._yaml('"2025-12-31"'))
        assert "no period reads" not in caplog.text

    @pytest.mark.parametrize("validation", [
        "",
        "\nvalidation:\n  type: expanding_window\n  initial_train_size: 100\n  step_size: 100\n",
        "\nvalidation:\n  type: date_window\n  test_start: 2024-01-01\n",
    ])
    def test_non_date_validation_raises(self, validation):
        with pytest.raises(
            ValueError,
            match="holdout_end needs date_expanding or date_sliding validation",
        ):
            ExperimentConfig.from_yaml(self._yaml('"2025-12-31"', validation))

    def test_date_sliding_accepted(self):
        config = ExperimentConfig.from_yaml(self._yaml(
            '"2025-12-31"',
            "\nvalidation:\n  type: date_sliding\n  train_months: 12\n  test_months: 6\n",
        ))
        assert config.data.date_range.holdout_end == date(2025, 12, 31)


def _holdout_data() -> dict:
    return {"date_range": {
        "start": "2021-01-01", "end": "2024-12-31", "holdout_end": "2025-12-31",
    }}


def _projection_family_cases() -> list:
    from mvp.projection.config import ProjectionConfig, ProjectionDiscoveryConfig
    from mvp.projection.iid.config import (
        IIDDiscoveryConfig,
        IIDProjectionConfig,
        ServeDiscoveryConfig,
    )
    from mvp.projection.lines.config import LinesDiscoveryConfig

    feats = {"include": ["win_rate(days=30)"]}
    return [
        pytest.param(ProjectionConfig, {"features": feats}, id="ProjectionConfig"),
        pytest.param(ProjectionDiscoveryConfig, {}, id="ProjectionDiscoveryConfig"),
        pytest.param(IIDProjectionConfig, {"features": feats}, id="IIDProjectionConfig"),
        pytest.param(ServeDiscoveryConfig, {}, id="ServeDiscoveryConfig"),
        pytest.param(IIDDiscoveryConfig, {}, id="IIDDiscoveryConfig"),
        pytest.param(
            LinesDiscoveryConfig, {"discovery": {"target": "total"}},
            id="LinesDiscoveryConfig",
        ),
    ]


class TestHoldoutEndRejectedOutsideClassification:
    @pytest.mark.parametrize("cls, extra", _projection_family_cases())
    def test_valid_without_the_field(self, cls, extra):
        data = _holdout_data()
        del data["date_range"]["holdout_end"]
        cls.model_validate({"data": data, **extra})

    @pytest.mark.parametrize("cls, extra", _projection_family_cases())
    def test_rejects_the_field(self, cls, extra):
        with pytest.raises(
            ValueError,
            match="holdout_end is not read by projection/IID/lines runs: their "
            "held-out read is the forward fit through end",
        ):
            cls.model_validate({"data": _holdout_data(), **extra})


class TestDiscoveryDateRangeHoldoutEnd:
    _YAML = """
data:
  date_range:
    start: "2021-01-01"
    end: "2024-12-31"
    holdout_end: "2025-12-31"
validation:
  type: date_expanding
  initial_train_months: 12
  test_months: 12
"""

    def test_carried_into_the_emitted_config(self):
        from mvp.model.discovery.config import DiscoveryConfig

        disc = DiscoveryConfig.from_yaml(self._YAML)
        emitted = disc.to_experiment_config_dict(["win_rate(days=30)"])
        assert emitted["data"]["date_range"]["holdout_end"] == date(2025, 12, 31)
        cfg = ExperimentConfig.model_validate(emitted)
        assert cfg.data.date_range.holdout_end == date(2025, 12, 31)

    def test_absent_field_is_not_emitted(self):
        from mvp.model.discovery.config import DiscoveryConfig

        disc = DiscoveryConfig.from_yaml(
            self._YAML.replace('    holdout_end: "2025-12-31"\n', "")
        )
        emitted = disc.to_experiment_config_dict(["win_rate(days=30)"])
        assert "holdout_end" not in emitted["data"]["date_range"]

    def test_unknown_keys_raise(self):
        from mvp.model.discovery.config import DiscoveryConfig

        with pytest.raises(ValueError, match="holdout_ned"):
            DiscoveryConfig.from_yaml(
                self._YAML.replace("holdout_end:", "holdout_ned:")
            )

    def test_same_validators(self):
        from mvp.model.discovery.config import DiscoveryConfig

        with pytest.raises(ValueError, match="holdout_end must be after end"):
            DiscoveryConfig.from_yaml(self._YAML.replace("2025-12-31", "2024-12-31"))
        with pytest.raises(ValueError, match="holdout_end must be before the betting start"):
            DiscoveryConfig.from_yaml(self._YAML.replace("2025-12-31", "2026-01-01"))
