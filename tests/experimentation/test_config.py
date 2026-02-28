"""Tests for experiment config schema."""

from datetime import date

from mvp.experimentation.config import ExperimentConfig


class TestExperimentConfig:
    """Tests for ExperimentConfig parsing."""

    def test_minimal_config(self):
        """Parse minimal valid config."""
        yaml_str = """
name: test_experiment
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
        assert config.name == "test_experiment"
        assert config.data.date_range.start == date(2020, 1, 1)
        assert config.data.date_range.end == date(2024, 12, 31)
        assert config.features.include == ["win_rate(days=30)"]
        assert config.model.type == "xgboost"

    def test_walk_forward_validation(self):
        """Parse walk-forward validation config."""
        yaml_str = """
name: test_experiment
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
name: test_experiment
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
name: test_experiment
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
name: test_experiment
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
