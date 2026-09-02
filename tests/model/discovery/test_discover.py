"""Tests for discovery orchestration."""

from pathlib import Path

from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError

import mvp.model.features  # noqa: F401  (populate the feature registry)
from mvp.model.discovery.config import DiscoveryConfig, DiscoveryOptions
from mvp.model.discovery.discover import (
    DiscoveryResult,
    FeatureDiscovery,
    get_all_feature_specs,
    spec_base_feature,
)
from mvp.model.metrics import default_min_delta


def _mtl_cfg(metric="log_loss", select_on=None, extra_discovery=None):
    """Minimal MTL DiscoveryConfig dict for validator tests."""
    mtl = {"auxiliary_targets": ["game_margin"]}
    if select_on is not None:
        mtl["select_on"] = select_on
    discovery = {"metric": metric}
    if extra_discovery:
        discovery.update(extra_discovery)
    return {
        "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
        "discovery": discovery,
        "model": {"type": "xgboost"},
        "mtl": mtl,
    }


class TestMTLSelectOn:
    """Tests for the MTL select_on field and its validators."""

    def test_default_is_combined(self):
        cfg = DiscoveryConfig.model_validate(_mtl_cfg())
        assert cfg.mtl.select_on == "combined"

    def test_primary_parses(self):
        cfg = DiscoveryConfig.model_validate(_mtl_cfg(select_on="primary"))
        assert cfg.mtl.select_on == "primary"

    def test_primary_with_proper_tail_metric(self):
        cfg = DiscoveryConfig.model_validate(
            _mtl_cfg(metric="beta_tail_score", select_on="primary")
        )
        assert cfg.mtl.select_on == "primary"

    def test_accuracy_rejected_under_primary(self):
        with pytest.raises(ValidationError, match="threshold-based"):
            DiscoveryConfig.model_validate(_mtl_cfg(metric="accuracy", select_on="primary"))

    def test_accuracy_allowed_under_combined(self):
        # discovery.metric is ignored under combined, so accuracy is harmless.
        DiscoveryConfig.model_validate(_mtl_cfg(metric="accuracy", select_on="combined"))

    def test_mtl_rejected_with_stability_selection(self):
        with pytest.raises(ValidationError, match="stability_selection"):
            DiscoveryConfig.model_validate(
                _mtl_cfg(extra_discovery={"stability_selection": {}})
            )

    def test_mtl_rejected_with_meta_discovery(self):
        with pytest.raises(ValidationError, match="meta_discovery"):
            DiscoveryConfig.model_validate(
                _mtl_cfg(extra_discovery={"meta_discovery": {"ensemble_config": "x.yaml"}})
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
        assert options.sweep_params is False
        assert options.segment_analysis is False
        assert options.metric == "calibration_error"
        # direction defaults to None and is derived from the metric at use time.
        assert options.direction is None
        assert options.resolved_direction() == "minimize"

    def test_direction_derived_from_metric(self):
        """resolved_direction() derives from the metric when not set explicitly."""
        assert DiscoveryOptions(metric="log_loss").resolved_direction() == "minimize"
        assert DiscoveryOptions(metric="beta_tail_score").resolved_direction() == "minimize"
        assert DiscoveryOptions(metric="roc_auc").resolved_direction() == "maximize"
        assert (
            DiscoveryOptions(metric="weighted_concordance").resolved_direction()
            == "maximize"
        )
        assert (
            DiscoveryOptions(metric="partial_auc_tail").resolved_direction() == "maximize"
        )

    def test_explicit_direction_overrides_and_warns(self, caplog):
        """An explicit direction is honored even when it contradicts the metric,
        and the contradiction is warned about."""
        import logging

        with caplog.at_level(logging.WARNING):
            options = DiscoveryOptions(metric="roc_auc", direction="minimize")
        assert options.resolved_direction() == "minimize"
        assert any("contradicts" in r.message for r in caplog.records)

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


class TestOffsetPoolMembership:
    """A seeded offset feature must have a matrix column. Base seeds are
    unioned into the pool (matrix columns, not candidates), so base-seeded
    offset features survive any filter — and config validation guarantees
    every offset feature IS base-seeded."""

    def _config(self, tmp_path, base=("player_elo_diff",), **features):
        import mvp.model.features  # noqa: F401

        config_dict = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "model": {"type": "xgboost"},
            "validation": {"n_splits": 2, "min_train_size": 1000, "test_size": 500},
            "discovery": {
                "sweep_params": False,
                "segment_analysis": False,
                "features": {"base": list(base), **features},
            },
            "offset": {"feature": "player_elo_diff"},
        }
        config_path = tmp_path / "offset_pool.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)
        return config_path

    def test_offset_feature_in_pool_passes(self, tmp_path):
        discovery = FeatureDiscovery(config_path=self._config(tmp_path), verbose=False)

        pool = discovery._build_candidate_pool()

        assert "player_elo_diff" in pool

    def test_base_seeded_offset_feature_survives_exclude(self, tmp_path):
        # exclude keeps it out of the CANDIDATE pool, but the base union
        # restores its matrix column, so the offset fit works.
        config_path = self._config(tmp_path, exclude=["player_elo_diff"])
        discovery = FeatureDiscovery(config_path=config_path, verbose=False)

        assert "player_elo_diff" in discovery._build_candidate_pool()

    def test_base_seeded_offset_feature_survives_include_omission(self, tmp_path):
        """An `include` list copied from an older run that predates the
        feature being promoted to a seed — the base union restores it."""
        config_path = self._config(tmp_path, include=["player_win_pct_diff"])
        discovery = FeatureDiscovery(config_path=config_path, verbose=False)

        assert "player_elo_diff" in discovery._build_candidate_pool()

    # An offset feature missing from base is impossible past config load —
    # validate_offset_compatibility raises (covered in test_fast_selection) —
    # so with the base union there is no reachable "offset feature not in
    # pool" state left to test.


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


class TestExcludeBase:
    """Base-name exclusion from the discovery candidate pool."""

    def _config(self, tmp_path, exclude_base):
        config_dict = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "discovery": {"features": {"exclude_base": exclude_base}},
            "model": {"type": "xgboost"},
        }
        path = tmp_path / "cfg.yaml"
        with open(path, "w") as f:
            yaml.dump(config_dict, f)
        return path

    def test_default_empty(self):
        assert DiscoveryOptions().features.exclude_base == []

    def test_spec_base_feature_player_opp_diff(self):
        assert spec_base_feature("player_days_since_surface") == "days_since_surface"
        assert spec_base_feature("opp_days_since_surface") == "days_since_surface"
        assert (
            spec_base_feature("player_days_since_surface_diff")
            == "days_since_surface_diff"
        )

    def test_spec_base_feature_windowed(self):
        assert spec_base_feature("player_match_count(days=30)") == "match_count"
        assert spec_base_feature("opp_match_count(days=365)") == "match_count"

    def test_spec_base_feature_singles_not_confused(self):
        # exact-name mapping: the _singles sibling is its own base, not the parent
        assert (
            spec_base_feature("player_days_since_surface_singles")
            == "days_since_surface_singles"
        )

    def test_baseline_pool_contains_family(self, tmp_path):
        # sanity: without exclusion the family IS in the pool
        disc = FeatureDiscovery(config_path=self._config(tmp_path, []))
        pool = disc._build_candidate_pool()
        assert "player_days_since_surface" in pool
        assert "player_days_since_surface_diff" in pool

    def test_excludes_whole_family(self, tmp_path):
        disc = FeatureDiscovery(config_path=self._config(tmp_path, ["days_since_surface"]))
        pool = disc._build_candidate_pool()
        # parent family gone: player, opp, and diff variants
        assert "player_days_since_surface" not in pool
        assert "opp_days_since_surface" not in pool
        assert "player_days_since_surface_diff" not in pool
        # distinct _singles base survives (exact match, no substring bleed)
        assert "player_days_since_surface_singles" in pool
        assert "player_days_since_surface_singles_diff" in pool

    def test_unknown_base_raises(self, tmp_path):
        disc = FeatureDiscovery(
            config_path=self._config(tmp_path, ["not_a_real_feature"])
        )
        with pytest.raises(ValueError, match="matches no registered feature"):
            disc._build_candidate_pool()


class TestBaseUnionedIntoPool:
    """Base seeds join the matrix pool: a base spec with no matrix column
    would KeyError every base+candidate evaluation to -inf and FS would halt
    having discovered nothing (the model=-parameterized prior seed bug)."""

    def _config(self, tmp_path, base, exclude=None):
        features: dict = {"base": base}
        if exclude:
            features["exclude"] = exclude
        config_dict = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "discovery": {"features": features},
            "model": {"type": "xgboost"},
        }
        path = tmp_path / "cfg.yaml"
        with open(path, "w") as f:
            yaml.dump(config_dict, f)
        return path

    def test_parameterized_base_spec_joins_pool(self, tmp_path):
        # model=-parameterized specs are never enumerated; base must union them
        spec = "player_prior_logit(model=two_level_flat)"
        disc = FeatureDiscovery(config_path=self._config(tmp_path, [spec]))
        assert spec in disc._build_candidate_pool()

    def test_enumerable_base_spec_not_duplicated(self, tmp_path):
        spec = "player_days_since_surface"
        disc = FeatureDiscovery(config_path=self._config(tmp_path, [spec]))
        assert disc._build_candidate_pool().count(spec) == 1

    def test_base_wins_over_exclude_in_the_matrix(self, tmp_path):
        # An excluded spec stays out of the CANDIDATE pool, but a base seed the
        # model will contain must still get a matrix column or every
        # evaluation breaks. The selector keeps it out of `remaining` anyway.
        spec = "player_days_since_surface"
        disc = FeatureDiscovery(
            config_path=self._config(tmp_path, [spec], exclude=[spec])
        )
        assert spec in disc._build_candidate_pool()


class TestExtraCandidates:
    """features.extra: additional CANDIDATES for specs enumeration never
    lists (model=-parameterized transforms). Bypasses the enumeration
    filters — an explicitly typed spec must not be silently starved by a
    narrowing include list — but never the exclusion safety rules."""

    def _config(self, tmp_path, extra, **features):
        config_dict = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "discovery": {"features": {"extra": extra, **features}},
            "model": {"type": "xgboost"},
        }
        path = tmp_path / "cfg.yaml"
        with open(path, "w") as f:
            yaml.dump(config_dict, f)
        return path

    def test_extra_joins_the_pool(self, tmp_path):
        spec = "player_chain_egames(model=two_level_flat)"
        disc = FeatureDiscovery(config_path=self._config(tmp_path, [spec]))
        assert spec in disc._build_candidate_pool()

    def test_extra_survives_a_narrowing_include(self, tmp_path):
        spec = "player_chain_egames(model=two_level_flat)"
        disc = FeatureDiscovery(config_path=self._config(
            tmp_path, [spec], include=["player_win_pct_diff"]
        ))
        pool = disc._build_candidate_pool()
        assert spec in pool
        assert pool.count(spec) == 1

    def test_extra_never_overrides_exclude(self, tmp_path):
        spec = "player_chain_egames(model=two_level_flat)"
        disc = FeatureDiscovery(config_path=self._config(
            tmp_path, [spec], exclude=[spec]
        ))
        assert spec not in disc._build_candidate_pool()

    def test_extra_is_a_candidate_not_a_seed(self, tmp_path):
        spec = "player_chain_egames(model=two_level_flat)"
        disc = FeatureDiscovery(config_path=self._config(tmp_path, [spec]))
        assert spec not in disc.config.discovery.features.base

    def test_extra_never_overrides_compute_only(self, tmp_path):
        spec = "player_chain_egames(model=two_level_flat)"
        disc = FeatureDiscovery(config_path=self._config(
            tmp_path, [spec], compute_only=[spec]
        ))
        assert spec not in disc._build_candidate_pool()


class TestEnsurePriorSources:
    """The completeness pass regenerates every model=<stem> source the run
    references (offset.prior + base + extra) before precompute — without it,
    a missing/stale source crashes at the transform's refusal mid-run."""

    def _disc(self, tmp_path, base=(), extra=(), offset_prior=None):
        config_dict = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "discovery": {"features": {"base": list(base), "extra": list(extra)}},
            "model": {"type": "xgboost"},
        }
        if offset_prior:
            config_dict["offset"] = {"prior": offset_prior}
        path = tmp_path / "cfg.yaml"
        with open(path, "w") as f:
            yaml.dump(config_dict, f)
        return FeatureDiscovery(config_path=path)

    def test_collects_offset_base_and_extra_stems_deduped(
        self, tmp_path, monkeypatch
    ):
        import mvp.model.features.prior as prior_mod

        ensured: list[str] = []
        monkeypatch.setattr(
            prior_mod, "resolve_prior",
            lambda stem: SimpleNamespace(
                model=stem, config_path=f"{stem}.yaml", fp="feedfeedfeed"
            ),
        )
        monkeypatch.setattr(
            prior_mod, "ensure_prior_artifacts",
            lambda source, regenerate: ensured.append(
                (source.model, regenerate)
            ),
        )
        disc = self._disc(
            tmp_path,
            base=["player_prior_logit(model=stem_a)"],
            extra=[
                "player_chain_egames(model=stem_b)",
                # same stem twice across lists -> ensured once
                "player_chain_gstd(model=stem_b)",
            ],
            offset_prior="stem_a",
        )
        disc._ensure_prior_sources()
        assert ensured == [("stem_a", True), ("stem_b", True)]

    def test_no_sources_is_a_noop(self, tmp_path, monkeypatch):
        import mvp.model.features.prior as prior_mod

        def _boom(*a, **k):
            raise AssertionError("should not resolve anything")

        monkeypatch.setattr(prior_mod, "resolve_prior", _boom)
        disc = self._disc(tmp_path, base=["player_elo_diff"])
        disc._ensure_prior_sources()


class TestResolvedMinDelta:
    """discovery.min_delta resolution: None -> metric-scaled default, else override."""

    def test_none_resolves_to_metric_default(self):
        # Unset (None) uses the metric's scale-appropriate default, and switching
        # the metric rescales it — log_loss and beta_tail_score differ ~10x.
        assert DiscoveryOptions(metric="log_loss").resolved_min_delta() == default_min_delta("log_loss")
        assert DiscoveryOptions(metric="beta_tail_score").resolved_min_delta() == default_min_delta("beta_tail_score")
        assert (
            DiscoveryOptions(metric="log_loss").resolved_min_delta()
            > DiscoveryOptions(metric="beta_tail_score").resolved_min_delta()
        )

    def test_explicit_override_wins(self):
        assert DiscoveryOptions(metric="log_loss", min_delta=5e-3).resolved_min_delta() == 5e-3

    def test_zero_override_is_honored_not_treated_as_unset(self):
        # 0.0 is a real value (accept any improvement), distinct from None.
        assert DiscoveryOptions(metric="beta_tail_score", min_delta=0.0).resolved_min_delta() == 0.0

    def test_default_is_none(self):
        assert DiscoveryOptions().min_delta is None
