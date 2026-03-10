"""Tests for discovery orchestration."""

from pathlib import Path

import pytest
import yaml

from mvp.model.discovery.config import DiscoveryConfig, DiscoveryOptions
from mvp.model.discovery.discover import (
    DiscoveryResult,
    FeatureDiscovery,
    get_all_feature_specs,
)


class TestDiscoveryConfig:
    """Tests for DiscoveryConfig."""

    def test_loads_minimal_config(self, tmp_path):
        """Should load config with minimal required fields."""
        config_dict = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        config = DiscoveryConfig.from_file(config_path)

        assert config.discovery.importance_method == "permutation"
        assert config.model.type == "xgboost"

    def test_loads_full_config(self, tmp_path):
        """Should load config with all fields."""
        config_dict = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
            "discovery": {
                "importance_method": "shap",
                "selection_method": "recursive",
                "sweep_params": False,
                "segment_analysis": False,
            },
            "model": {
                "type": "logistic",
            },
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        config = DiscoveryConfig.from_file(config_path)

        assert config.discovery.importance_method == "shap"
        assert config.discovery.selection_method == "recursive"
        assert config.discovery.sweep_params is False
        assert config.model.type == "logistic"

    def test_to_experiment_config_dict(self, tmp_path):
        """Should convert to experiment config format."""
        config_dict = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        config = DiscoveryConfig.from_file(config_path)
        experiment_dict = config.to_experiment_config_dict(
            features=["win_rate(window_days=30)", "h2h_record()"]
        )

        assert "name" not in experiment_dict  # Name derived from filename, not in config
        assert experiment_dict["features"]["include"] == [
            "win_rate(window_days=30)",
            "h2h_record()",
        ]
        assert "model" in experiment_dict
        assert "validation" in experiment_dict

    def test_to_experiment_config_dict_with_compute_only(self, tmp_path):
        """compute_only features pass through to experiment config."""
        config_dict = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
            "discovery": {
                "features": {
                    "compute_only": ["player_elo_surface_diff"],
                },
            },
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        config = DiscoveryConfig.from_file(config_path)
        experiment_dict = config.to_experiment_config_dict(
            features=["player_svc_elo_diff"]
        )

        assert experiment_dict["features"]["include"] == ["player_svc_elo_diff"]
        assert experiment_dict["features"]["compute_only"] == ["player_elo_surface_diff"]

    def test_to_experiment_config_dict_no_compute_only(self, tmp_path):
        """No compute_only key when list is empty."""
        config_dict = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        config = DiscoveryConfig.from_file(config_path)
        experiment_dict = config.to_experiment_config_dict(
            features=["player_svc_elo_diff"]
        )

        assert "compute_only" not in experiment_dict["features"]


class TestDiscoveryOptions:
    """Tests for DiscoveryOptions defaults."""

    def test_default_values(self):
        """Should have sensible defaults."""
        options = DiscoveryOptions()

        assert options.importance_method == "permutation"
        assert options.selection_method == "forward"
        assert options.sweep_params is True
        assert options.segment_analysis is True
        assert options.metric == "calibration_error"
        assert options.direction == "minimize"

    def test_features_defaults(self):
        """Feature config should have sensible defaults."""
        options = DiscoveryOptions()

        assert options.features.include == []
        assert options.features.exclude == []
        assert options.features.compute_only == []
        assert options.features.base == []
        assert options.features.min == 5
        assert options.features.max is None
        assert options.features.window_sizes is None


class TestGetAllFeatureSpecs:
    """Tests for get_all_feature_specs."""

    def test_returns_list(self):
        """Should return list of feature specs."""
        import mvp.model.features  # noqa: F401

        specs = get_all_feature_specs()

        assert isinstance(specs, list)
        assert len(specs) > 0
        assert all(isinstance(s, str) for s in specs)

    def test_default_includes_alltime_and_windows(self):
        import mvp.model.features  # noqa: F401

        specs = get_all_feature_specs()

        assert "player_win_pct_diff" in specs  # all-time
        assert "player_win_pct_diff(days=365)" in specs  # windowed
        assert "player_win_pct_diff(days=30)" in specs

    def test_window_sizes_only_specific_window(self):
        import mvp.model.features  # noqa: F401

        specs = get_all_feature_specs(window_sizes=[365])

        assert "player_win_pct_diff(days=365)" in specs
        assert "player_win_pct_diff" not in specs  # no all-time
        assert "player_win_pct_diff(days=30)" not in specs

    def test_window_sizes_zero_means_alltime(self):
        import mvp.model.features  # noqa: F401

        specs = get_all_feature_specs(window_sizes=[0])

        assert "player_win_pct_diff" in specs  # all-time
        assert "player_win_pct_diff(days=365)" not in specs

    def test_window_sizes_zero_plus_window(self):
        import mvp.model.features  # noqa: F401

        specs = get_all_feature_specs(window_sizes=[0, 365])

        assert "player_win_pct_diff" in specs  # all-time
        assert "player_win_pct_diff(days=365)" in specs
        assert "player_win_pct_diff(days=30)" not in specs

    def test_no_params_features_unaffected_by_window_sizes(self):
        import mvp.model.features  # noqa: F401

        specs_default = get_all_feature_specs()
        specs_narrow = get_all_feature_specs(window_sizes=[365])

        assert "player_elo_diff" in specs_default
        assert "player_elo_diff" in specs_narrow


class TestDiscoveryResult:
    """Tests for DiscoveryResult dataclass."""

    def test_holds_data(self):
        """Should store all result fields."""
        result = DiscoveryResult(
            selected_features=["win_rate(window_days=30)"],
            final_metric=0.042,
            n_experiments=10,
        )

        assert result.selected_features == ["win_rate(window_days=30)"]
        assert result.final_metric == 0.042
        assert result.n_experiments == 10
        assert result.selection_result is None
        assert result.sweep_result is None


class TestFeatureDiscovery:
    """Tests for FeatureDiscovery class."""

    @pytest.fixture
    def discovery_config(self, tmp_path):
        """Create a discovery config file."""
        config_dict = {
            "name": "test_discovery",
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
            "discovery": {
                "sweep_params": False,
                "segment_analysis": False,
            },
            "validation": {
                "n_splits": 2,
                "min_train_size": 1000,
                "test_size": 500,
            },
        }
        config_path = tmp_path / "discover.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)
        return config_path

    def test_initializes(self, discovery_config):
        """Should initialize from config."""
        discovery = FeatureDiscovery(
            config_path=discovery_config,
            verbose=False,
        )

        assert discovery.verbose is False

    def test_creates_temp_config(self, discovery_config):
        """Should create temporary experiment config."""
        discovery = FeatureDiscovery(config_path=discovery_config)

        temp_path = discovery._create_temp_config(
            features=["win_rate(window_days=30)"]
        )

        assert temp_path.exists()
        with open(temp_path) as f:
            config = yaml.safe_load(f)
        assert config["features"]["include"] == ["win_rate(window_days=30)"]

        # Cleanup
        temp_path.unlink()

    def test_creates_scorer(self, discovery_config):
        """Should create scorer function."""
        discovery = FeatureDiscovery(config_path=discovery_config)

        scorer = discovery._create_scorer()

        assert callable(scorer)
        # Empty features should return inf
        result = scorer([])
        assert result == float("inf")
