"""Tests for fast forward selection."""

import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector
from mvp.model.splitters import make_splitter


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    """Re-register features before each test."""
    import mvp.model.features.ranking
    import mvp.model.features.serve
    import mvp.model.features.win_rate

    importlib.reload(mvp.model.features.ranking)
    importlib.reload(mvp.model.features.serve)
    importlib.reload(mvp.model.features.win_rate)


@pytest.fixture
def sample_matches(tmp_path: Path) -> Path:
    """Create sample matches parquet with ranking and serve data."""
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
    """Create discovery config YAML."""
    config_dict = {
        "data": {
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-12-31",
            },
        },
        "model": {"type": "logistic"},
        "validation": {
            "type": "walk_forward",
            "n_splits": 2,
            "min_train_size": 50,
            "test_size": 25,
        },
        "discovery": {
            "metric": "log_loss",
            "direction": "minimize",
        },
    }
    config_path = tmp_path / "discovery.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)
    return config_path


class TestFastForwardSelector:
    """Tests for FastForwardSelector."""

    def test_precompute_builds_matrix(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Precompute should build X_wide, y, folds, and fold_medians."""
        config = DiscoveryConfig.from_file(discovery_config)
        feature_specs = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=feature_specs,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()

        assert fast.X_wide is not None
        assert fast.y is not None
        assert fast.X_wide.shape[0] == fast.y.shape[0]
        assert fast.X_wide.shape[1] == 1  # one feature
        assert len(fast.folds) == 2  # n_splits=2
        assert len(fast.fold_medians) == 2

    def test_precompute_multiple_features(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Precompute should handle multiple feature specs."""
        config = DiscoveryConfig.from_file(discovery_config)
        feature_specs = [
            "player_ranking_points_diff",
            "player_ranking_rank_diff",
        ]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=feature_specs,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()

        assert fast.X_wide.shape[1] == 2
        assert len(fast.col_to_idx) == 2

    def test_scorer_returns_float(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Scorer should return a finite float for valid features."""
        config = DiscoveryConfig.from_file(discovery_config)
        feature_specs = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=feature_specs,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        scorer = fast.create_scorer("log_loss")

        result = scorer(["player_ranking_points_diff"])

        assert isinstance(result, float)
        assert np.isfinite(result)

    def test_scorer_empty_features(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Scorer should return inf for empty feature list."""
        config = DiscoveryConfig.from_file(discovery_config)
        feature_specs = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=feature_specs,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        scorer = fast.create_scorer("log_loss")

        result = scorer([])

        assert result == float("inf")

    def test_scorer_unknown_feature(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Scorer should return inf for unknown feature (KeyError)."""
        config = DiscoveryConfig.from_file(discovery_config)
        feature_specs = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=feature_specs,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        scorer = fast.create_scorer("log_loss")

        result = scorer(["player_nonexistent_feature"])

        assert result == float("inf")

    def test_scorer_matches_full_runner(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Fast scorer should produce same metrics as full ExperimentRunner."""
        from mvp.model.runner import ExperimentRunner

        config = DiscoveryConfig.from_file(discovery_config)
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        # Fast path
        fast = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        scorer = fast.create_scorer("log_loss")
        fast_metric = scorer(features)

        # Slow path (full runner)
        exp_config_dict = config.to_experiment_config_dict(features)
        exp_config_path = tmp_path / "exp_config.yaml"
        with open(exp_config_path, "w") as f:
            yaml.dump(exp_config_dict, f)

        runner = ExperimentRunner(
            config_path=exp_config_path,
            matches_path=sample_matches,
            cache_dir=cache_dir,
            log_to_mlflow=False,
        )
        result = runner.run()
        runner_metric = result["metrics"]["raw_log_loss"]

        assert fast_metric == pytest.approx(runner_metric, abs=1e-10)

    def test_fold_indices_match_runner(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Fold indices from FastForwardSelector should match runner's splitter."""
        from mvp.model.engine import FeatureEngine, get_feature_columns

        config = DiscoveryConfig.from_file(discovery_config)
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        # Build the df the same way the runner does
        engine = FeatureEngine(
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        df = engine.compute(features)
        dr = config.data.date_range
        df = df.filter(
            (pl.col("effective_match_date") >= dr.start)
            & (pl.col("effective_match_date") <= dr.end)
        )
        df = df.filter(pl.col("won").is_not_null())

        val = config.validation
        splitter = make_splitter(
            val_type=val.type,
            n_splits=val.n_splits,
            min_train_size=val.min_train_size,
            test_size=val.test_size,
        )
        runner_folds = list(splitter.split(df))

        # Fast path
        fast = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()

        assert len(fast.folds) == len(runner_folds)
        for (fast_train, fast_test), (run_train, run_test) in zip(
            fast.folds, runner_folds
        ):
            assert list(fast_train) == run_train
            assert list(fast_test) == run_test


class TestMakeSplitter:
    """Tests for make_splitter factory."""

    def test_walk_forward(self):
        """Should create ExpandingWindowSplitter in n_splits mode."""
        from mvp.model.splitters import ExpandingWindowSplitter

        splitter = make_splitter("walk_forward", n_splits=3, min_train_size=100, test_size=50)
        assert isinstance(splitter, ExpandingWindowSplitter)

    def test_expanding_window(self):
        """Should create ExpandingWindowSplitter in step_size mode."""
        from mvp.model.splitters import ExpandingWindowSplitter

        splitter = make_splitter(
            "expanding_window", initial_train_size=100, step_size=50
        )
        assert isinstance(splitter, ExpandingWindowSplitter)

    def test_expanding_window_missing_params(self):
        """Should raise ValueError when required params are missing."""
        with pytest.raises(ValueError, match="initial_train_size"):
            make_splitter("expanding_window")

    def test_sliding_window(self):
        """Should create SlidingWindowSplitter."""
        from mvp.model.splitters import SlidingWindowSplitter

        splitter = make_splitter("sliding_window", train_size=100, test_size=50)
        assert isinstance(splitter, SlidingWindowSplitter)

    def test_sliding_window_missing_params(self):
        """Should raise ValueError when train_size is missing."""
        with pytest.raises(ValueError, match="train_size"):
            make_splitter("sliding_window")

    def test_unknown_type(self):
        """Should raise ValueError for unknown type."""
        with pytest.raises(ValueError, match="Unknown validation type"):
            make_splitter("unknown_type")
