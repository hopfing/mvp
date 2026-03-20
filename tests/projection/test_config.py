"""Tests for projection configuration."""

import tempfile
from pathlib import Path

import pytest
import yaml

from mvp.projection.config import (
    ProjectionConfig,
    ProjectionDiscoveryConfig,
    ProjectionModelConfig,
)


class TestProjectionConfig:
    """Tests for ProjectionConfig loading and validation."""

    def test_from_yaml_minimal(self):
        """Load minimal projection config."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
features:
  include:
    - player_elo_surface_diff
model:
  type: xgb_regressor
"""
        config = ProjectionConfig.from_yaml(yaml_str)
        assert config.model.type == "xgb_regressor"
        assert config.features.include == ["player_elo_surface_diff"]
        assert config.metrics.primary == "mae"

    def test_from_file(self, tmp_path):
        """Load config from file."""
        config_dict = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "features": {"include": ["player_age_diff"]},
            "model": {"type": "ridge"},
        }
        path = tmp_path / "test.yaml"
        path.write_text(yaml.dump(config_dict))

        config = ProjectionConfig.from_file(path)
        assert config.model.type == "ridge"
        assert config.features.include == ["player_age_diff"]

    def test_all_model_types(self):
        """All regression model types are valid."""
        for model_type in ["xgb_regressor", "linear", "ridge"]:
            config = ProjectionModelConfig(type=model_type)
            assert config.type == model_type

    def test_invalid_model_type(self):
        """Invalid model type raises validation error."""
        with pytest.raises(Exception):
            ProjectionModelConfig(type="xgboost")

    def test_defaults(self):
        """Default values are applied correctly."""
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
features:
  include:
    - player_elo_surface_diff
"""
        config = ProjectionConfig.from_yaml(yaml_str)
        assert config.model.type == "xgb_regressor"
        assert config.metrics.primary == "mae"
        assert config.metrics.secondary == ["rmse", "r_squared"]
        assert config.validation.n_splits == 5


class TestProjectionDiscoveryConfig:
    """Tests for ProjectionDiscoveryConfig loading."""

    def test_from_yaml(self):
        """Load discovery config."""
        yaml_str = """
data:
  date_range:
    start: "2015-01-01"
    end: "2025-12-31"
discovery:
  metric: mae
  direction: minimize
  features:
    max: 10
model:
  type: xgb_regressor
"""
        config = ProjectionDiscoveryConfig.from_yaml(yaml_str)
        assert config.discovery.metric == "mae"
        assert config.discovery.direction == "minimize"
        assert config.discovery.features.max == 10

    def test_to_projection_config_dict(self):
        """Convert discovery config to projection config dict."""
        yaml_str = """
data:
  date_range:
    start: "2015-01-01"
    end: "2025-12-31"
discovery:
  metric: rmse
model:
  type: ridge
"""
        config = ProjectionDiscoveryConfig.from_yaml(yaml_str)
        result = config.to_projection_config_dict(["feat_a", "feat_b"])
        assert result["features"]["include"] == ["feat_a", "feat_b"]
        assert result["model"]["type"] == "ridge"
        assert result["metrics"]["primary"] == "rmse"
