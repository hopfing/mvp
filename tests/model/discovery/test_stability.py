"""Tests for stability-selection orchestration.

The base forward selector is stubbed so each resample's selected set is scripted,
letting us assert the frequency aggregation / thresholding / skip handling without
running real model fits.
"""

import json
from datetime import date

import numpy as np
import pytest

from mvp.model.discovery import stability as stab
from mvp.model.discovery.config import DiscoveryConfig, StabilitySelectionConfig
from mvp.model.discovery.fast_selection import FastForwardSelector
from mvp.model.discovery.selection import SelectionResult


def _fast() -> FastForwardSelector:
    cfg = DiscoveryConfig.model_validate(
        {
            "data": {"date_range": {"start": "2020-01-01", "end": "2024-12-31"}},
            "validation": {
                "type": "date_sliding",
                "train_months": 24,
                "test_months": 12,
            },
        }
    )
    sel = FastForwardSelector(
        cfg, all_feature_specs=["a"], matches_path="x.parquet", cache_dir="c"
    )
    sel.y = np.array([0, 1, 0, 1])
    sel.tournament_key = np.array(["t1_2020", "t1_2021", "t2_2022", "t2_2023"])
    sel.row_dates = np.array(
        [date(2020, 6, 1), date(2021, 6, 1), date(2022, 6, 1), date(2023, 6, 1)],
        dtype="datetime64[D]",
    )
    sel.fold_windows = [
        (date(2020, 1, 1), date(2022, 1, 1), date(2022, 1, 1), date(2023, 1, 1)),
        (date(2021, 1, 1), date(2023, 1, 1), date(2023, 1, 1), date(2024, 1, 1)),
    ]
    sel.fold_medians = [np.array([0.0]), np.array([1.0])]
    return sel


class _ScriptedSelector:
    """Stub FeatureSelector that returns a scripted selection per call."""

    scripted: list[list[str]] = []
    calls = 0

    def __init__(self, **kwargs):
        pass

    def run(self, verbose=False):
        sel = _ScriptedSelector.scripted[_ScriptedSelector.calls]
        _ScriptedSelector.calls += 1
        return SelectionResult(
            selected_features=sel, excluded_features=[], history=[], final_metric=0.0
        )


@pytest.fixture
def patched(monkeypatch):
    _ScriptedSelector.calls = 0
    monkeypatch.setattr(stab, "FeatureSelector", _ScriptedSelector)
    # Scorer is irrelevant to the stub selector; make it a no-op.
    monkeypatch.setattr(
        FastForwardSelector, "create_scorer",
        lambda self, metric, folds=None, fold_medians=None: (lambda feats: 0.0),
    )


def _run(fast, scripted, **overrides):
    _ScriptedSelector.scripted = scripted
    config = StabilitySelectionConfig(
        n_resamples=len(scripted),
        subsample_fraction=1.0,  # all units -> deterministic full mask
        min_fold_rows=1,
        selection_threshold=overrides.pop("threshold", 0.6),
        **overrides,
    )
    return stab.run_stability_selection(
        fast, config,
        metric="calibration_error", direction="minimize",
        all_features=["a", "b", "c"], min_features=1, max_features=3,
    )


def test_frequency_and_threshold(patched):
    fast = _fast()
    res = _run(fast, [["a", "b"], ["a", "c"], ["a"]])
    assert res.n_resamples_effective == 3
    assert res.selection_frequency["a"] == pytest.approx(1.0)
    assert res.selection_frequency["b"] == pytest.approx(1 / 3)
    assert res.selection_frequency["c"] == pytest.approx(1 / 3)
    # Only "a" clears the 0.6 threshold.
    assert res.selected_features == ["a"]
    assert res.stopping_rounds == [2, 2, 1]
    assert res.resample_match_counts == [4, 4, 4]


def test_selected_ordered_by_frequency(patched):
    fast = _fast()
    res = _run(fast, [["a", "b"], ["a", "b"], ["b"], ["a"]], threshold=0.4)
    # b: 3/4=0.75, a: 3/4=0.75 -> both pass 0.4; order is by frequency desc.
    assert set(res.selected_features) == {"a", "b"}
    freqs = [res.selection_frequency[f] for f in res.selected_features]
    assert freqs == sorted(freqs, reverse=True)


def test_all_degenerate_raises(patched):
    fast = _fast()
    _ScriptedSelector.scripted = [["a"]]
    config = StabilitySelectionConfig(
        n_resamples=2, subsample_fraction=1.0,
        min_fold_rows=1000,  # forces every fold below threshold
        selection_threshold=0.6,
    )
    with pytest.raises(RuntimeError, match="no usable resamples"):
        stab.run_stability_selection(
            fast, config, metric="calibration_error", direction="minimize",
            all_features=["a"], min_features=1, max_features=1,
        )


def test_resample_mask_tournament_requires_key():
    fast = _fast()
    fast.tournament_key = None
    config = StabilitySelectionConfig(resample_unit="tournament")
    rng = np.random.default_rng(0)
    with pytest.raises(ValueError, match="tournament_id/year"):
        stab._resample_mask(rng, fast, config)


def test_resample_mask_match_level():
    fast = _fast()
    config = StabilitySelectionConfig(resample_unit="match", subsample_fraction=0.5)
    rng = np.random.default_rng(0)
    mask = stab._resample_mask(rng, fast, config)
    assert mask.dtype == bool
    assert mask.sum() == 2  # round(4 * 0.5)


def test_checkpoint_resume_skips_completed_resamples(patched, tmp_path):
    fast = _fast()
    cp = tmp_path / "stab_cp.json"
    _ScriptedSelector.scripted = [["a", "b"], ["a"], ["a", "c"]]
    config = StabilitySelectionConfig(
        n_resamples=3, subsample_fraction=1.0, min_fold_rows=1,
        selection_threshold=0.6,
    )

    def _call():
        return stab.run_stability_selection(
            fast, config, metric="log_loss", direction="minimize",
            all_features=["a", "b", "c"], min_features=1, max_features=3,
            checkpoint_path=cp,
        )

    res = _call()
    # 3 resamples ran -> 3 scripted selectors consumed.
    assert _ScriptedSelector.calls == 3
    assert res.n_resamples_effective == 3
    # On success the checkpoint is removed.
    assert not cp.exists()


def test_checkpoint_partial_then_resume(patched, tmp_path):
    """A checkpoint written for 2 of 3 resamples is resumed without re-running them."""
    fast = _fast()
    cp = tmp_path / "stab_cp.json"
    # Hand-write a checkpoint with resamples 0 and 1 already done.
    fp = stab._resample_fingerprint(
        fast.config,
        StabilitySelectionConfig(n_resamples=3, subsample_fraction=1.0, min_fold_rows=1),
        ["a", "b", "c"],
        metric="log_loss", direction="minimize",
        min_features=1, max_features=3, min_delta=0.0, base_features=None,
    )
    cp.write_text(json.dumps({
        "fingerprint": fp,
        "completed": [
            {"index": 0, "selected": ["a", "b"], "match_count": 4, "fold_skips": 0},
            {"index": 1, "selected": ["a"], "match_count": 4, "fold_skips": 0},
        ],
    }))
    # Only resample 2 should run -> exactly 1 scripted selector consumed.
    _ScriptedSelector.scripted = [["a", "c"]]
    config = StabilitySelectionConfig(
        n_resamples=3, subsample_fraction=1.0, min_fold_rows=1, selection_threshold=0.5,
    )
    res = stab.run_stability_selection(
        fast, config, metric="log_loss", direction="minimize",
        all_features=["a", "b", "c"], min_features=1, max_features=3,
        checkpoint_path=cp,
    )
    assert _ScriptedSelector.calls == 1  # resamples 0,1 resumed; only 2 ran
    assert res.n_resamples_effective == 3
    # a in all 3 -> 1.0; clears 0.5
    assert res.selection_frequency["a"] == pytest.approx(1.0)


def test_checkpoint_fingerprint_mismatch_starts_fresh(patched, tmp_path):
    fast = _fast()
    cp = tmp_path / "stab_cp.json"
    cp.write_text(json.dumps({
        "fingerprint": "deadbeef0000",
        "completed": [{"index": 0, "selected": ["x"], "match_count": 4, "fold_skips": 0}],
    }))
    _ScriptedSelector.scripted = [["a"], ["a"]]
    config = StabilitySelectionConfig(
        n_resamples=2, subsample_fraction=1.0, min_fold_rows=1, selection_threshold=0.6,
    )
    res = stab.run_stability_selection(
        fast, config, metric="log_loss", direction="minimize",
        all_features=["a", "b", "c"], min_features=1, max_features=3,
        checkpoint_path=cp,
    )
    # Stale fingerprint ignored -> both resamples run fresh, "x" never appears.
    assert _ScriptedSelector.calls == 2
    assert "x" not in res.selection_frequency


def test_run_branches_into_stability_workflow(tmp_path, monkeypatch):
    """FeatureDiscovery.run() routes to the stability workflow when configured."""
    from mvp.model.discovery import FeatureDiscovery
    from mvp.model.discovery.stability import StabilityResult

    cfg_path = tmp_path / "stab.yaml"
    cfg_path.write_text(
        "data:\n"
        "  date_range:\n"
        "    start: 2020-01-01\n"
        "    end: 2024-12-31\n"
        "discovery:\n"
        "  metric: calibration_error\n"
        "  stability_selection:\n"
        "    n_resamples: 3\n"
        "validation:\n"
        "  type: date_sliding\n"
        "  train_months: 24\n"
        "  test_months: 12\n"
    )
    discovery = FeatureDiscovery(cfg_path)

    canned = StabilityResult(
        selection_frequency={"a": 1.0, "b": 0.2},
        selected_features=["a"],
        threshold=0.6,
        n_resamples_requested=3,
        n_resamples_effective=3,
    )
    monkeypatch.setattr(discovery, "run_stability", lambda: canned)
    monkeypatch.setattr(
        discovery, "_run_experiment",
        lambda features, log_to_mlflow=False, run_name=None: {
            "metrics": {"calibration_error": 0.12}
        },
    )

    result = discovery.run()
    assert result.selected_features == ["a"]
    assert result.stability_result is canned
    assert result.final_metric == 0.12
