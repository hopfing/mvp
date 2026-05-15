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
