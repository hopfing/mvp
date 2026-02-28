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
