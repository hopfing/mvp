"""Tests for meta-feature discovery."""

import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml
from pydantic import ValidationError

from mvp.model.discovery.config import DiscoveryConfig, MetaDiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking

    importlib.reload(mvp.model.features.ranking)


@pytest.fixture
def sample_matches(tmp_path: Path) -> Path:
    n = 300
    rng = np.random.RandomState(42)
    df = pl.DataFrame(
        {
            "match_uid": [f"M{i}" for i in range(n)],
            "player_id": [f"P{i % 10}" for i in range(n)],
            "opp_id": [f"P{(i + 5) % 10}" for i in range(n)],
            "effective_match_date": [
                f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(n)
            ],
            "won": [bool(x) for x in rng.randint(0, 2, n)],
            "player_rankings_points": rng.randint(100, 2000, n).tolist(),
            "opp_rankings_points": rng.randint(100, 2000, n).tolist(),
            "player_rank": rng.randint(1, 200, n).tolist(),
            "opp_rank": rng.randint(1, 200, n).tolist(),
        }
    ).with_columns(pl.col("effective_match_date").str.to_datetime())
    path = tmp_path / "matches.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def discovery_config(tmp_path: Path) -> Path:
    config_dict = {
        "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "model": {"type": "logistic"},
        "validation": {
            "type": "walk_forward",
            "n_splits": 2,
            "min_train_size": 50,
            "test_size": 25,
        },
        "discovery": {"metric": "log_loss", "direction": "minimize"},
    }
    config_path = tmp_path / "discovery.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)
    return config_path


class TestMetaDiscoveryConfig:
    def test_meta_discovery_config_valid(self):
        config = MetaDiscoveryConfig(
            ensemble_config="models/ens_03_stacking.yaml",
            weighting="magnitude",
        )
        assert config.ensemble_config == "models/ens_03_stacking.yaml"
        assert config.weighting == "magnitude"

    def test_meta_discovery_config_defaults(self):
        config = MetaDiscoveryConfig(
            ensemble_config="models/ens_03_stacking.yaml",
        )
        assert config.weighting == "magnitude"

    def test_meta_discovery_in_discovery_options(self):
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
discovery:
  selection_method: forward
  metric: log_loss
  direction: minimize
  meta_discovery:
    ensemble_config: models/ens_03_stacking.yaml
    weighting: binary
model:
  type: logistic
"""
        config = DiscoveryConfig.from_yaml(yaml_str)
        assert config.discovery.meta_discovery is not None
        assert config.discovery.meta_discovery.ensemble_config == "models/ens_03_stacking.yaml"
        assert config.discovery.meta_discovery.weighting == "binary"

    def test_meta_discovery_none_by_default(self):
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
discovery:
  selection_method: forward
  metric: log_loss
model:
  type: logistic
"""
        config = DiscoveryConfig.from_yaml(yaml_str)
        assert config.discovery.meta_discovery is None


class TestBuildDisagreementDataset:
    def test_binary_disagreement(self):
        from mvp.model.discovery.discover import _build_disagreement_dataset

        y_true = np.array([1, 0, 1, 0, 1, 0])
        pred_0 = np.array([0.8, 0.6, 0.3, 0.4, 0.7, 0.2])
        pred_1 = np.array([0.3, 0.4, 0.7, 0.6, 0.8, 0.3])

        target, mask, weights = _build_disagreement_dataset(
            y_true, pred_0, pred_1, weighting="binary"
        )

        # side_0 = [1, 1, 0, 0, 1, 0], side_1 = [0, 0, 1, 1, 1, 0]
        # Disagreements: indices 0, 1, 2, 3
        # Index 0: side_0=1, truth=1 -> model 0 right -> target=1
        # Index 1: side_0=1, truth=0 -> model 0 wrong -> target=0
        # Index 2: side_0=0, truth=1 -> model 0 wrong -> target=0
        # Index 3: side_0=0, truth=0 -> model 0 right -> target=1
        assert mask.sum() == 4
        assert weights is None
        expected_target = np.array([1, 0, 0, 1])
        np.testing.assert_array_equal(target[mask], expected_target)

    def test_magnitude_weighting(self):
        from mvp.model.discovery.discover import _build_disagreement_dataset

        y_true = np.array([1, 0, 1, 0])
        pred_0 = np.array([0.8, 0.6, 0.3, 0.4])
        pred_1 = np.array([0.3, 0.5, 0.7, 0.6])

        target, mask, weights = _build_disagreement_dataset(
            y_true, pred_0, pred_1, weighting="magnitude"
        )

        assert mask is None
        assert weights is not None
        assert len(weights) == 4
        np.testing.assert_allclose(weights, np.abs(pred_0 - pred_1))
        err_0 = (pred_0 - y_true) ** 2
        err_1 = (pred_1 - y_true) ** 2
        expected_target = (err_0 < err_1).astype(int)
        np.testing.assert_array_equal(target, expected_target)

    def test_binary_no_disagreements(self):
        from mvp.model.discovery.discover import _build_disagreement_dataset

        y_true = np.array([1, 0])
        pred_0 = np.array([0.8, 0.3])
        pred_1 = np.array([0.9, 0.2])  # Same side as pred_0

        target, mask, weights = _build_disagreement_dataset(
            y_true, pred_0, pred_1, weighting="binary"
        )

        assert mask.sum() == 0
        assert weights is None

    def test_magnitude_equal_predictions(self):
        from mvp.model.discovery.discover import _build_disagreement_dataset

        y_true = np.array([1, 0])
        pred_0 = np.array([0.7, 0.3])
        pred_1 = np.array([0.7, 0.3])

        target, mask, weights = _build_disagreement_dataset(
            y_true, pred_0, pred_1, weighting="magnitude"
        )

        assert mask is None
        np.testing.assert_allclose(weights, [0.0, 0.0])


class TestFastForwardSelectorOverrides:
    def test_override_y(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        config = DiscoveryConfig.from_file(discovery_config)
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        n = len(fast.y)

        new_y = np.random.randint(0, 2, n)
        fast2 = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast2.precompute(override_y=new_y)

        np.testing.assert_array_equal(fast2.y, new_y)
        assert fast2.X_wide.shape[0] == n

    def test_row_mask(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        config = DiscoveryConfig.from_file(discovery_config)
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        n = len(fast.y)

        mask = np.zeros(n, dtype=bool)
        mask[: n // 2] = True

        fast2 = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast2.precompute(row_mask=mask)

        assert fast2.X_wide.shape[0] == n // 2
        assert fast2.y.shape[0] == n // 2

    def test_sample_weights(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        config = DiscoveryConfig.from_file(discovery_config)
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        n = len(fast.y)
        weights = np.random.rand(n)

        fast2 = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast2.precompute(sample_weights=weights)

        assert fast2.sample_weights is not None
        assert fast2.sample_weights.shape[0] == n

        scorer = fast2.create_scorer("log_loss")
        result = scorer(features)
        assert isinstance(result, float)
        assert np.isfinite(result)

    def test_no_overrides_unchanged(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        config = DiscoveryConfig.from_file(discovery_config)
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast1 = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast1.precompute()

        fast2 = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast2.precompute()

        np.testing.assert_array_equal(fast1.y, fast2.y)
        np.testing.assert_array_equal(fast1.X_wide, fast2.X_wide)
        assert fast2.sample_weights is None
