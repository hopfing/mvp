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
            "circuit": ["tour" for _ in range(n)],
        }
    ).with_columns(pl.col("effective_match_date").str.to_datetime())
    path = tmp_path / "matches.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def discovery_config(tmp_path: Path) -> Path:
    """Create discovery config YAML.

    Uses XGBoost so the test suite exercises the NaN-tolerant path. The
    `ranking_points_diff` family used in fixtures is registered as
    impute=None (post Phase 2 audit), which the FS scorer must surface as
    NaN to the model — under non-NaN-tolerant models this is a contract
    violation and the scorer raises.
    """
    config_dict = {
        "data": {
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-12-31",
            },
        },
        "model": {"type": "xgboost"},
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


@pytest.fixture
def discovery_config_logistic(tmp_path: Path) -> Path:
    """Logistic-model discovery config for the impute-contract guard test."""
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
    config_path = tmp_path / "discovery_logistic.yaml"
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
        df = engine.compute(features, extra_columns=["won"])
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


def _write_eval_filter_config(
    tmp_path: Path, eval_filters: dict | None, name: str = "cfg"
) -> Path:
    """Write an XGBoost discovery config, optionally with data.eval_filters."""
    data: dict = {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}}
    if eval_filters is not None:
        data["eval_filters"] = eval_filters
    config_dict = {
        "data": data,
        "model": {"type": "xgboost"},
        "validation": {
            "type": "walk_forward",
            "n_splits": 2,
            "min_train_size": 50,
            "test_size": 25,
        },
        "discovery": {"metric": "log_loss", "direction": "minimize"},
    }
    config_path = tmp_path / f"discovery_{name}.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_dict, f)
    return config_path


class TestEvalFilters:
    """Tests for data.eval_filters — restricts the SCORING (test) fold to a
    slice while the model still fits on the full train fold.
    """

    def test_eval_filters_parsed(self, tmp_path: Path):
        """eval_filters is parsed onto the discovery DataConfig."""
        path = _write_eval_filter_config(tmp_path, {"player_rank": {"max": 100}})
        config = DiscoveryConfig.from_file(path)
        assert config.data.eval_filters == {"player_rank": {"max": 100}}

    def test_all_rows_matches_baseline(
        self, sample_matches: Path, tmp_path: Path
    ):
        """An eval_filters that passes every row must reproduce the no-filter
        score exactly (same fit, same test set) and set an all-True mask."""
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        base_path = _write_eval_filter_config(tmp_path, None, name="base")
        base = FastForwardSelector(
            config=DiscoveryConfig.from_file(base_path),
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        base.precompute()
        assert base.eval_mask is None
        base_metric = base.create_scorer("log_loss")(features)

        # ranks are in [1, 200); max: 999 passes all of them.
        allpass_path = _write_eval_filter_config(
            tmp_path, {"player_rank": {"max": 999}}, name="allpass"
        )
        allpass = FastForwardSelector(
            config=DiscoveryConfig.from_file(allpass_path),
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        allpass.precompute()
        assert allpass.eval_mask is not None
        assert allpass.eval_mask.all()
        allpass_metric = allpass.create_scorer("log_loss")(features)

        assert allpass_metric == pytest.approx(base_metric, abs=1e-9)

    def test_subset_restricts_scoring(
        self, sample_matches: Path, tmp_path: Path
    ):
        """A partitioning eval_filters yields a proper-subset mask and a
        finite score computed on that slice."""
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        path = _write_eval_filter_config(tmp_path, {"player_rank": {"max": 100}})
        fast = FastForwardSelector(
            config=DiscoveryConfig.from_file(path),
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()

        assert fast.eval_mask is not None
        assert fast.eval_mask.shape[0] == fast.X_wide.shape[0]
        n_kept = int(fast.eval_mask.sum())
        assert 0 < n_kept < fast.eval_mask.shape[0]

        result = fast.create_scorer("log_loss")(features)
        assert np.isfinite(result)

    def test_zero_rows_raises(self, sample_matches: Path, tmp_path: Path):
        """eval_filters matching no rows fails loudly at precompute rather than
        silently producing an empty evaluation set."""
        path = _write_eval_filter_config(tmp_path, {"player_rank": {"max": -1}})
        fast = FastForwardSelector(
            config=DiscoveryConfig.from_file(path),
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        with pytest.raises(ValueError, match="eval_filters matched 0 rows"):
            fast.precompute()

    def test_scorer_consumes_mask(self, sample_matches: Path, tmp_path: Path):
        """The scorer must actually restrict scoring to eval_mask.

        Toggles the mask on one precomputed selector: an all-True mask must
        reproduce the no-mask score, and a proper subset must change it. A
        scorer that built the mask but ignored it in the fold loop would pass
        the all-True check yet FAIL the subset check — so this is the real
        proof that scoring happens on the slice, not the full test fold.
        """
        features = ["player_ranking_points_diff"]
        path = _write_eval_filter_config(tmp_path, None, name="consume")
        fast = FastForwardSelector(
            config=DiscoveryConfig.from_file(path),
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        fast.precompute()
        n = fast.X_wide.shape[0]

        fast.eval_mask = None
        s_full = fast.create_scorer("log_loss")(features)

        fast.eval_mask = np.ones(n, dtype=bool)
        s_all = fast.create_scorer("log_loss")(features)

        fast.eval_mask = np.arange(n) % 2 == 0
        s_sub = fast.create_scorer("log_loss")(features)

        # All-True mask is a no-op relative to the whole test fold.
        assert s_all == pytest.approx(s_full, abs=1e-12)
        # A proper subset scores on a different row set → different metric.
        assert abs(s_sub - s_full) > 1e-6


class TestResolveColumnImpute:
    """Tests for _resolve_column_impute — maps a column name to (strategy, value).

    The function is the single source of truth for FS-time NaN handling: the
    scorer reads its output and applies the chosen fill per column. A
    regression here silently miscalibrates every FS run, so each impute
    flavor (None / numeric constant / "median" / unknown) is asserted.
    """

    def test_passthrough_for_impute_none(self):
        from mvp.model.discovery.fast_selection import _resolve_column_impute
        from mvp.model.registry import FeatureDef, FeatureRegistry

        registry = FeatureRegistry()
        registry.register(FeatureDef(
            name="my_feat", func=lambda: None, impute=None,
        ))
        assert _resolve_column_impute("player_my_feat", registry) == ("passthrough", 0.0)
        assert _resolve_column_impute("opp_my_feat", registry) == ("passthrough", 0.0)

    def test_constant_for_numeric_impute(self):
        from mvp.model.discovery.fast_selection import _resolve_column_impute
        from mvp.model.registry import FeatureDef, FeatureRegistry

        registry = FeatureRegistry()
        registry.register(FeatureDef(name="cnt", func=lambda: None, impute=0))
        registry.register(FeatureDef(name="rate", func=lambda: None, impute=0.5))
        assert _resolve_column_impute("player_cnt", registry) == ("constant", 0.0)
        assert _resolve_column_impute("player_rate", registry) == ("constant", 0.5)

    def test_median_default(self):
        from mvp.model.discovery.fast_selection import _resolve_column_impute
        from mvp.model.registry import FeatureDef, FeatureRegistry

        registry = FeatureRegistry()
        registry.register(FeatureDef(name="med_feat", func=lambda: None))
        assert _resolve_column_impute("player_med_feat", registry) == ("median", 0.0)

    def test_windowed_suffix_stripped(self):
        from mvp.model.discovery.fast_selection import _resolve_column_impute
        from mvp.model.registry import FeatureDef, FeatureRegistry

        registry = FeatureRegistry()
        registry.register(FeatureDef(
            name="win_rate", func=lambda: None, impute="median", params=["days"],
        ))
        # player_win_rate_30d → strip player_ → strip _30d → win_rate
        assert _resolve_column_impute("player_win_rate_30d", registry) == ("median", 0.0)

    def test_unknown_column_falls_back_to_median(self):
        from mvp.model.discovery.fast_selection import _resolve_column_impute
        from mvp.model.registry import FeatureRegistry

        registry = FeatureRegistry()
        # Aux columns and unmapped names — defensive fallback, never selected
        # for scoring directly but present in X_wide.
        assert _resolve_column_impute("aux_unknown_col", registry) == ("median", 0.0)

    def test_diff_inherits_via_its_own_registration(self):
        """Diffs are registered under their own name (e.g., "x_diff"), not
        looked up via the base. Resolver should hit the diff's own entry."""
        from mvp.model.discovery.fast_selection import _resolve_column_impute
        from mvp.model.registry import FeatureDef, FeatureRegistry

        registry = FeatureRegistry()
        registry.register(FeatureDef(
            name="x_diff", func=lambda: None, mirror=False, impute=None,
        ))
        # Diff columns have no player_/opp_ prefix
        assert _resolve_column_impute("x_diff", registry) == ("passthrough", 0.0)


class TestFillStrategyContract:
    """Tests for the FS scorer's per-strategy fill behavior.

    XGB consumes NaN natively, so impute=None features must reach it as NaN
    (matching its production training behavior). Logistic / RF / NN don't
    consume NaN, but production training for those wrappers applies a
    median-imputer (models._apply_median_imputer) — so FS median-fills
    impute=None features for those models to match production.
    """

    def test_logistic_falls_back_to_median_for_passthrough(
        self, discovery_config_logistic: Path, sample_matches: Path, tmp_path: Path
    ):
        """Logistic FS + impute=None feature: scorer falls back to per-fold
        median (mirrors LogisticModel's training-time median imputer)."""
        config = DiscoveryConfig.from_file(discovery_config_logistic)
        # ranking_points_diff was flipped to impute=None in the Phase 2 audit.
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()
        scorer = fast.create_scorer("log_loss")

        result = scorer(features)
        assert isinstance(result, float)
        assert np.isfinite(result)

    def test_xgboost_accepts_passthrough_features(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """XGB FS + impute=None feature scores normally (no raise)."""
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
        scorer = fast.create_scorer("log_loss")

        result = scorer(features)
        assert isinstance(result, float)
        assert np.isfinite(result)

    def test_xgboost_scorer_actually_passes_nan_to_model(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path,
        monkeypatch,
    ):
        """End-to-end verification that the scorer's fill loop honors the
        passthrough strategy: under XGB, an impute=None feature's NaN
        values must survive all the way into model.fit().

        Intercepts get_model so the fit call records X_train, then asserts
        the recorded matrix still carries NaN. If anything regresses (a
        stray fillna, a misrouted strategy, an over-broad median fill),
        the assertion fails and the contract is restored visibly.
        """
        config = DiscoveryConfig.from_file(discovery_config)
        # ranking_points_diff is impute=None post Phase 2 audit.
        features = ["player_ranking_points_diff"]
        cache_dir = tmp_path / "cache"

        fast = FastForwardSelector(
            config=config,
            all_feature_specs=features,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )
        fast.precompute()

        # Sanity: precompute classified the feature as passthrough.
        idx = fast.col_to_idx["player_ranking_points_diff"]
        assert fast.fill_strategies[idx] == "passthrough"

        # The sample fixture doesn't produce NaN naturally (all rankings
        # populated), so poison the column directly. The strategy is
        # already passthrough, so the scorer must preserve these NaN
        # values end-to-end. (polars→numpy gives read-only views; copy
        # to a writable buffer first.)
        fast.X_wide = np.array(fast.X_wide, copy=True)
        fast.X_wide[:5, idx] = np.nan
        # Recompute fold medians so the median entry for this column is
        # finite (otherwise the fold_median fallback for non-passthrough
        # strategies could propagate NaN unrelated to our test).
        for fold_idx, (train_idx, _test_idx) in enumerate(fast.folds):
            col_med = np.nanmedian(fast.X_wide[train_idx, idx])
            if np.isnan(col_med):
                col_med = 0.0
            fast.fold_medians[fold_idx][idx] = col_med

        captured: dict[str, np.ndarray] = {}

        class _RecordingModel:
            def fit(self, X, y, **kwargs):
                captured["X_train"] = X.copy()
            def predict_proba(self, X):
                # Constant 0.5 — must be finite regardless of NaN in X so
                # the downstream log_loss metric doesn't fail validation.
                n = X.shape[0]
                return np.column_stack([np.full(n, 0.5), np.full(n, 0.5)])

        def _fake_get_model(model_type, params, feature_names=None):
            return _RecordingModel()

        monkeypatch.setattr(
            "mvp.model.discovery.fast_selection.get_model", _fake_get_model
        )

        scorer = fast.create_scorer("log_loss")
        _ = scorer(features)  # invoke for at least one fold

        assert "X_train" in captured, "scorer did not invoke model.fit"
        x = captured["X_train"]
        # The passthrough contract: NaN must survive the fill loop. The
        # sample fixture produces NaN on first-occurrence rows; if the
        # scorer's fill loop incorrectly median-filled, this matrix would
        # be NaN-free.
        assert np.isnan(x).any(), (
            "scorer median-filled an impute=None feature — passthrough "
            "contract violated, FS evaluates a different signal than "
            "production XGB training will."
        )

    def test_precompute_records_strategies(
        self, discovery_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """precompute() should populate fill_strategies and fill_constants
        parallel to col_to_idx."""
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

        assert len(fast.fill_strategies) == len(fast.col_to_idx)
        assert fast.fill_constants is not None
        assert fast.fill_constants.shape == (len(fast.col_to_idx),)
        # ranking_points_diff is impute=None
        idx = fast.col_to_idx["player_ranking_points_diff"]
        assert fast.fill_strategies[idx] == "passthrough"


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
