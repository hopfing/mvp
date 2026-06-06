"""Tests for frozen fold geometry used by stability selection.

These exercise FastForwardSelector.resample_folds in isolation by setting the
frozen-geometry attributes directly, so no data pipeline / B: drive is needed.
"""

from datetime import date

import numpy as np
import pytest

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector


def _selector() -> FastForwardSelector:
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
        cfg, all_feature_specs=["player_x"], matches_path="x.parquet", cache_dir="c"
    )
    # Four matches, one per mid-year: 2020, 2021, 2022, 2023.
    sel.row_dates = np.array(
        [date(2020, 6, 1), date(2021, 6, 1), date(2022, 6, 1), date(2023, 6, 1)],
        dtype="datetime64[D]",
    )
    # Two frozen sliding windows (train 24mo / test 12mo).
    sel.fold_windows = [
        (date(2020, 1, 1), date(2022, 1, 1), date(2022, 1, 1), date(2023, 1, 1)),
        (date(2021, 1, 1), date(2023, 1, 1), date(2023, 1, 1), date(2024, 1, 1)),
    ]
    sel.fold_medians = [np.array([0.0]), np.array([1.0])]
    return sel


def test_resample_folds_assigns_rows_to_frozen_windows():
    sel = _selector()
    mask = np.ones(4, dtype=bool)
    folds, medians, skipped = sel.resample_folds(mask, min_fold_rows=1)
    assert skipped == 0
    assert len(folds) == 2
    assert len(medians) == 2
    # Window 0: train 2020+2021, test 2022.
    np.testing.assert_array_equal(folds[0][0], [0, 1])
    np.testing.assert_array_equal(folds[0][1], [2])
    # Window 1: train 2021+2022, test 2023.
    np.testing.assert_array_equal(folds[1][0], [1, 2])
    np.testing.assert_array_equal(folds[1][1], [3])
    # Medians are the frozen per-window arrays, aligned to surviving folds.
    assert medians[0][0] == 0.0 and medians[1][0] == 1.0


def test_min_fold_rows_skips_degenerate_folds():
    sel = _selector()
    mask = np.ones(4, dtype=bool)
    # Each test side has exactly 1 row; require 2 -> both folds skipped.
    folds, medians, skipped = sel.resample_folds(mask, min_fold_rows=2)
    assert folds == []
    assert medians == []
    assert skipped == 2


def test_windows_do_not_shift_when_early_rows_dropped():
    """Frozen invariant: masking out the earliest match must not move a window.

    The 2022 test fold stays the 2022 test fold; only its populated rows change.
    """
    sel = _selector()
    mask = np.array([False, True, True, True])  # drop 2020 match
    folds, medians, skipped = sel.resample_folds(mask, min_fold_rows=1)
    assert skipped == 0
    # Window 0 train is now just the 2021 row; test still the 2022 row.
    np.testing.assert_array_equal(folds[0][0], [1])
    np.testing.assert_array_equal(folds[0][1], [2])


def test_dropping_a_test_tournament_skips_that_fold_only():
    sel = _selector()
    mask = np.array([True, True, False, True])  # drop the 2022 match (window 0 test)
    folds, medians, skipped = sel.resample_folds(mask, min_fold_rows=1)
    # Window 0 loses its only test row -> skipped; window 1 unaffected.
    assert skipped == 1
    assert len(folds) == 1
    np.testing.assert_array_equal(folds[0][1], [3])
    assert medians[0][0] == 1.0  # the surviving window's frozen median


def test_requires_frozen_windows():
    sel = _selector()
    sel.fold_windows = []
    with pytest.raises(ValueError, match="date splitter"):
        sel.resample_folds(np.ones(4, dtype=bool), min_fold_rows=1)


def test_geometry_length_mismatch_raises():
    sel = _selector()
    sel.fold_medians = [np.array([0.0])]  # only 1, windows has 2
    with pytest.raises(RuntimeError, match="length mismatch"):
        sel.resample_folds(np.ones(4, dtype=bool), min_fold_rows=1)
