"""Tests for the null-importance pre-filter."""

import numpy as np
import pytest

from mvp.model.discovery import null_importance as ni
from mvp.model.discovery.config import (
    DiscoveryConfig,
    NullImportanceConfig,
)
from mvp.model.discovery.fast_selection import FastForwardSelector


def _fast(model_type: str = "xgboost") -> FastForwardSelector:
    cfg = DiscoveryConfig.model_validate(
        {
            "data": {"date_range": {"start": "2020-01-01", "end": "2024-12-31"}},
            "model": {"type": model_type},
            "validation": {
                "type": "date_sliding",
                "train_months": 24,
                "test_months": 12,
            },
        }
    )
    sel = FastForwardSelector(
        cfg, all_feature_specs=["a", "b", "c"], matches_path="x.parquet", cache_dir="c"
    )
    sel.X_wide = np.random.default_rng(0).normal(size=(20, 3))
    sel.y = np.array([0, 1] * 10)
    sel.col_to_idx = {"a": 0, "b": 1, "c": 2}
    sel.sample_weights = None
    return sel


class _FakeModel:
    def fit(self, X, y, **kw):
        return self


@pytest.fixture
def patch_fits(monkeypatch):
    """Stub model fitting; gain_importance returns scripted importances per fit.

    Call 0 = real fit; calls 1..n = shuffled-target fits.
    """
    monkeypatch.setattr(ni, "get_model", lambda *a, **k: _FakeModel())
    state = {"i": 0, "scripted": []}

    def fake_gain(model, col_names):
        d = state["scripted"][state["i"]]
        state["i"] += 1
        return d

    monkeypatch.setattr(ni, "gain_importance", fake_gain)
    return state


def test_keeps_features_that_beat_their_null(patch_fits):
    fast = _fast()
    # Real fit: a and b carry importance, c little. Nulls: a,b never beaten;
    # c's null importance always exceeds its real -> c dropped.
    real = {"a": 0.5, "b": 0.3, "c": 0.2}
    nulls = [{"a": 0.0, "b": 0.0, "c": 1.0} for _ in range(20)]
    patch_fits["scripted"] = [real, *nulls]

    config = NullImportanceConfig(n_runs=20, alpha=0.05)
    res = ni.run_null_importance(fast, all_features=["a", "b", "c"], config=config)

    assert set(res.kept_features) == {"a", "b"}
    assert res.dropped_features == ["c"]
    # a beat all 20 nulls -> p = 1/21
    assert res.p_value["a"] == pytest.approx(1 / 21)
    assert res.p_value["c"] == pytest.approx(21 / 21)


def test_alpha_controls_strictness(patch_fits):
    fast = _fast()
    real = {"a": 0.5, "b": 0.3, "c": 0.2}
    # Each feature beaten by exactly 1 of 4 null runs -> p = 2/5 = 0.4.
    nulls = [
        {"a": 1.0, "b": 1.0, "c": 1.0},  # beats all
        {"a": 0.0, "b": 0.0, "c": 0.0},
        {"a": 0.0, "b": 0.0, "c": 0.0},
        {"a": 0.0, "b": 0.0, "c": 0.0},
    ]
    patch_fits["scripted"] = [real, *nulls]
    config = NullImportanceConfig(n_runs=4, alpha=0.05)
    res = ni.run_null_importance(fast, all_features=["a", "b", "c"], config=config)
    # p=0.4 > 0.05 -> all dropped.
    assert res.kept_features == []
    assert res.p_value["a"] == pytest.approx(2 / 5)


def test_non_tree_model_raises():
    fast = _fast(model_type="logistic")
    config = NullImportanceConfig()
    with pytest.raises(ValueError, match="tree model"):
        ni.run_null_importance(fast, all_features=["a"], config=config)


def test_median_fill_for_non_nan_tolerant(patch_fits):
    """random_forest path fills NaNs (no exception); xgboost passes them through."""
    fast = _fast(model_type="random_forest")
    fast.X_wide[0, 0] = np.nan
    real = {"a": 0.6, "b": 0.4, "c": 0.0}
    nulls = [{"a": 0.0, "b": 0.0, "c": 0.0} for _ in range(5)]
    patch_fits["scripted"] = [real, *nulls]
    config = NullImportanceConfig(n_runs=5, alpha=0.2)
    res = ni.run_null_importance(fast, all_features=["a", "b", "c"], config=config)
    assert "a" in res.kept_features


def test_cache_skips_refit_on_hit(patch_fits, tmp_path):
    fast = _fast()
    real = {"a": 0.5, "b": 0.3, "c": 0.2}
    nulls = [{"a": 0.0, "b": 0.0, "c": 1.0} for _ in range(20)]
    patch_fits["scripted"] = [real, *nulls]
    config = NullImportanceConfig(n_runs=20, alpha=0.05)

    # First run: computes (consumes all 21 scripted importances) and caches.
    r1 = ni.run_null_importance(
        fast, all_features=["a", "b", "c"], config=config, cache_dir=tmp_path
    )
    assert patch_fits["i"] == 21  # 1 real + 20 null fits happened
    assert set(r1.kept_features) == {"a", "b"}

    # Second run: cache hit -> NO further fits (counter stays at 21).
    r2 = ni.run_null_importance(
        fast, all_features=["a", "b", "c"], config=config, cache_dir=tmp_path
    )
    assert patch_fits["i"] == 21  # unchanged: nothing refit
    assert r2.p_value == r1.p_value
    assert set(r2.kept_features) == {"a", "b"}


def test_cache_reused_when_only_alpha_changes(patch_fits, tmp_path):
    """alpha is excluded from the fingerprint, so changing it reuses the cache."""
    fast = _fast()
    real = {"a": 0.5, "b": 0.3, "c": 0.2}
    nulls = [{"a": 0.0, "b": 0.0, "c": 1.0} for _ in range(20)]
    patch_fits["scripted"] = [real, *nulls]

    ni.run_null_importance(
        fast, all_features=["a", "b", "c"],
        config=NullImportanceConfig(n_runs=20, alpha=0.05), cache_dir=tmp_path,
    )
    assert patch_fits["i"] == 21

    # Different alpha -> still a cache hit (no refit), just re-thresholded.
    r = ni.run_null_importance(
        fast, all_features=["a", "b", "c"],
        config=NullImportanceConfig(n_runs=20, alpha=1.0), cache_dir=tmp_path,
    )
    assert patch_fits["i"] == 21  # no refit
    assert set(r.kept_features) == {"a", "b", "c"}  # alpha=1.0 keeps all


def test_fingerprint_changes_with_pool_and_nruns():
    cfg = DiscoveryConfig.model_validate(
        {
            "data": {"date_range": {"start": "2020-01-01", "end": "2024-12-31"}},
            "model": {"type": "xgboost"},
            "discovery": {"null_importance": {"n_runs": 20}},
        }
    )
    nic20 = cfg.discovery.null_importance
    base = ni._screen_fingerprint(cfg, nic20, ["a", "b"])
    assert base == ni._screen_fingerprint(cfg, nic20, ["b", "a"])  # order-insensitive
    assert base != ni._screen_fingerprint(cfg, nic20, ["a", "b", "c"])  # pool change

    from mvp.model.discovery.config import NullImportanceConfig as _NIC
    assert base != ni._screen_fingerprint(cfg, _NIC(n_runs=50), ["a", "b"])  # n_runs
    # alpha is NOT in the fingerprint:
    assert base == ni._screen_fingerprint(cfg, _NIC(n_runs=20, alpha=0.5), ["a", "b"])


def test_run_stability_reduces_pool_via_null_importance(tmp_path, monkeypatch):
    """run_stability runs the screen and passes the reduced pool to stability."""
    from mvp.model.discovery import discover as disc
    from mvp.model.discovery.stability import StabilityResult

    cfg_path = tmp_path / "c.yaml"
    cfg_path.write_text(
        "data:\n  date_range:\n    start: 2020-01-01\n    end: 2024-12-31\n"
        "discovery:\n  metric: calibration_error\n"
        "  stability_selection:\n    n_resamples: 2\n"
        "  null_importance:\n    n_runs: 3\n"
        "validation:\n  type: date_sliding\n  train_months: 24\n  test_months: 12\n"
    )
    discovery = disc.FeatureDiscovery(cfg_path)

    monkeypatch.setattr(discovery, "_build_candidate_pool", lambda *a: ["a", "b", "c"])
    monkeypatch.setattr(FastForwardSelector, "precompute", lambda self, **k: None)
    monkeypatch.setattr(
        disc, "run_null_importance",
        lambda fast, all_features, config, cache_dir=None: ni.NullImportanceResult(
            real_importance={"a": 0.5}, null_mean={"a": 0.0}, p_value={"a": 0.0},
            kept_features=["a"], dropped_features=["b", "c"], n_runs=3, alpha=0.05,
        ),
    )
    captured = {}

    def fake_stability(fast, config, **kw):
        captured.update(kw)
        return StabilityResult(
            selection_frequency={"a": 1.0}, selected_features=["a"], threshold=0.6,
            n_resamples_effective=2, n_resamples_requested=2,
        )

    monkeypatch.setattr(disc, "run_stability_selection", fake_stability)

    result = discovery.run_stability()
    assert captured["all_features"] == ["a"]  # reduced pool reached stability
    assert result.selected_features == ["a"]
