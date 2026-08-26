"""Tests for the nested calibration driver (FS-protocol redesign item 6)."""

import importlib
import json
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.nested_calibration import (
    NestedFoldResult,
    _inner_fold_count,
    headline_gain,
    run_nested_calibration,
    summarize_gaps,
)
from mvp.model.discovery.selection import SelectionResult


def _sel(history, final_metric) -> SelectionResult:
    return SelectionResult(
        selected_features=[], excluded_features=[], history=history,
        final_metric=final_metric,
    )


class TestHeadlineGain:
    def test_minimize_is_base_minus_final(self):
        sel = _sel([{"action": "base", "metric": 0.70}], 0.65)
        assert headline_gain(sel, "minimize") == pytest.approx(0.05)

    def test_maximize_is_final_minus_base(self):
        sel = _sel([{"action": "base", "metric": 0.70}], 0.75)
        assert headline_gain(sel, "maximize") == pytest.approx(0.05)

    def test_unseeded_run_has_no_headline(self):
        sel = _sel([{"action": "add", "feature": "x", "metric": 0.6}], 0.6)
        assert headline_gain(sel, "minimize") is None
        assert headline_gain(_sel([], 0.6), "minimize") is None


class TestSummarizeGaps:
    def _fold(self, i, gap):
        return NestedFoldResult(
            fold=i, test_window="", inner_end="", n_inner_folds=1, gap=gap,
        )

    def test_ignores_missing_gaps(self):
        folds = [self._fold(0, 0.01), self._fold(1, None), self._fold(2, 0.03)]
        mean, median = summarize_gaps(folds)
        assert mean == pytest.approx(0.02)
        assert median == pytest.approx(0.02)

    def test_all_missing(self):
        assert summarize_gaps([self._fold(0, None)]) == (None, None)


class TestInnerFoldCount:
    def _config(self) -> DiscoveryConfig:
        return DiscoveryConfig.model_validate({
            "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
            "model": {"type": "xgboost"},
            "validation": {
                "type": "date_expanding",
                "initial_train_months": 4,
                "test_months": 2,
            },
        })

    def test_counts_calendar_windows_on_truncated_range(self):
        cfg = self._config()
        anchor = date(2024, 1, 1)
        # first test window starts 2024-05-01; range ending 06-30 holds one
        assert _inner_fold_count(cfg, anchor, date(2024, 6, 30)) == 1
        # ending 04-30, the first test window would start past the data
        assert _inner_fold_count(cfg, anchor, date(2024, 4, 30)) == 0
        assert _inner_fold_count(cfg, anchor, date(2024, 10, 31)) == 3


# --- integration (engine-touching) --------------------------------------

FAMILY_SPECS = [
    "player_win_pct(days=90)",
    "opp_win_pct(days=90)",
    "player_win_pct_diff(days=90)",
]
BASE_SPEC = "player_ranking_points_diff"


@pytest.fixture
def registered_features(isolated_registry):
    import mvp.model.features.ranking
    import mvp.model.features.serve
    import mvp.model.features.win_rate

    importlib.reload(mvp.model.features.ranking)
    importlib.reload(mvp.model.features.serve)
    importlib.reload(mvp.model.features.win_rate)


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
            "circuit": ["tour" for _ in range(n)],
        }
    ).with_columns(pl.col("effective_match_date").str.to_datetime())
    path = tmp_path / "matches.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def nested_config(tmp_path: Path) -> Path:
    config_dict = {
        "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "model": {"type": "xgboost"},
        "validation": {
            "type": "date_expanding",
            "initial_train_months": 4,
            "test_months": 2,
        },
        "discovery": {
            "metric": "log_loss",
            "direction": "minimize",
            "selection_unit": "family",
            "min_delta": 0.0,
            "features": {"base": [BASE_SPEC]},
        },
        "offset": {"feature": BASE_SPEC},
    }
    path = tmp_path / "nested.yaml"
    with open(path, "w") as f:
        yaml.dump(config_dict, f)
    return path


class TestRunNestedCalibration:
    def test_report_shape_and_skips(
        self, registered_features, nested_config: Path, sample_matches: Path,
        tmp_path: Path,
    ):
        run_dir = tmp_path / "nested_run"
        report = run_nested_calibration(
            config_path=nested_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            run_dir=run_dir,
            all_features=[BASE_SPEC, *FAMILY_SPECS],
            min_inner_folds=1,
        )

        assert report.metric == "log_loss"
        assert report.folds[0].skipped is not None  # no inner folds yet
        evaluated = [f for f in report.folds if f.gap is not None]
        assert evaluated, "later outer folds should evaluate"
        for f in evaluated:
            assert f.headline_gain is not None
            assert f.realized_gain is not None
            assert f.gap == pytest.approx(f.headline_gain - f.realized_gain)
        written = json.loads(
            (run_dir / "nested_calibration_report.json").read_text()
        )
        assert len(written["folds"]) == len(report.folds)
