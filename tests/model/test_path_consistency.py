"""Cross-path consistency: discovery scorer vs runner must produce identical predictions.

If this test fails, it means the FastForwardSelector (discovery) and ExperimentRunner
(training) process the same data differently — which invalidates discovery results.
"""

import numpy as np
import pytest
import warnings
from sklearn.linear_model import LogisticRegression

from mvp.model.imputation import (
    ImputeBuildResult,
    ImputeSpec,
    RecomputeInfo,
    apply_imputation,
    augmented_col_indices,
    fit_imputation,
    subset_impute_state,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rng():
    return np.random.RandomState(42)


@pytest.fixture
def base_data(rng):
    """500 rows, 6 'wide' columns: 4 model features + 2 that exist in the wide
    matrix but aren't selected as model features (simulating the full candidate
    pool that FastForwardSelector precomputes).

    Columns:
        0: player_elo       (base, impute=1500)
        1: opp_elo           (base, impute=1500)
        2: elo_diff          (derived = col0 - col1, recompute)
        3: win_rate          (base, impute=median)
        4: unrelated_feat_a  (not selected)
        5: unrelated_feat_b  (not selected)
    """
    n = 500
    cols = [rng.normal(1500, 200, n) for _ in range(6)]

    # Make col 2 a true diff
    cols[2] = cols[0] - cols[1]

    # Inject ~15% NaN per column
    for arr in cols:
        idx = rng.choice(n, size=int(n * 0.15), replace=False)
        arr[idx] = np.nan

    X_wide = np.column_stack(cols)
    y = rng.randint(0, 2, n)
    circuit = np.where(rng.rand(n) < 0.6, "TOUR", "CHAL")

    train_idx = np.arange(350)
    test_idx = np.arange(350, n)

    return X_wide, y, circuit, train_idx, test_idx


# Impute specs for the full 6-column wide matrix
WIDE_SPECS = [
    ImputeSpec(col_index=0, strategy="constant", constant=1500.0),
    ImputeSpec(col_index=1, strategy="constant", constant=1500.0),
    ImputeSpec(
        col_index=2,
        strategy="recompute",
        recompute=RecomputeInfo(player_base_idx=0, opp_base_idx=1),
    ),
    ImputeSpec(col_index=3, strategy="median"),
    ImputeSpec(col_index=4, strategy="median"),
    ImputeSpec(col_index=5, strategy="median"),
]


def _run_fast_selection_path(
    X_wide, y, circuit, train_idx, test_idx, col_indices, wide_specs
):
    """Replicate the FastForwardSelector scorer inner loop."""
    aug_indices, n_model = augmented_col_indices(col_indices, wide_specs)

    # Pre-fitted imputation on full wide matrix (done once in precompute)
    full_state = fit_imputation(X_wide[train_idx], circuit[train_idx], wide_specs)

    # Subset to candidate columns
    X_train = X_wide[np.ix_(train_idx, aug_indices)].copy()
    X_test = X_wide[np.ix_(test_idx, aug_indices)].copy()
    sub_state = subset_impute_state(full_state, aug_indices)

    X_train = apply_imputation(X_train, circuit[train_idx], sub_state)
    X_test = apply_imputation(X_test, circuit[test_idx], sub_state)
    X_train = X_train[:, :n_model]
    X_test = X_test[:, :n_model]

    # Scale from pre-imputation raw model columns
    X_train_raw = X_wide[np.ix_(train_idx, col_indices)]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        mean = np.nanmean(X_train_raw, axis=0)
        std = np.nanstd(X_train_raw, axis=0)
    mean = np.where(np.isnan(mean), 0.0, mean)
    std = np.where(np.isnan(std), 1.0, std)
    std[std == 0] = 1.0
    X_train = (X_train - mean) / std
    X_test = (X_test - mean) / std

    model = LogisticRegression(random_state=42, max_iter=1000)
    model.fit(X_train, y[train_idx])
    return model.predict_proba(X_test)[:, 1]


def _run_runner_path(X_narrow, y, circuit, train_idx, test_idx, narrow_specs, n_model):
    """Replicate the ExperimentRunner fold-loop inner logic."""
    X_train = X_narrow[train_idx].copy()
    X_test = X_narrow[test_idx].copy()
    circuit_train = circuit[train_idx]
    circuit_test = circuit[test_idx]

    impute_state = fit_imputation(X_train, circuit_train, narrow_specs)

    # Scale from pre-imputation model columns
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        train_mean = np.nanmean(X_train[:, :n_model], axis=0)
        train_std = np.nanstd(X_train[:, :n_model], axis=0)
    train_mean = np.where(np.isnan(train_mean), 0.0, train_mean)
    train_std = np.where(np.isnan(train_std), 1.0, train_std)
    train_std[train_std == 0] = 1.0

    X_train = apply_imputation(X_train, circuit_train, impute_state)
    X_test = apply_imputation(X_test, circuit_test, impute_state)
    X_train = X_train[:, :n_model]
    X_test = X_test[:, :n_model]
    X_train = (X_train - train_mean) / train_std
    X_test = (X_test - train_mean) / train_std

    model = LogisticRegression(random_state=42, max_iter=1000)
    model.fit(X_train, y[train_idx])
    return model.predict_proba(X_test)[:, 1]


# ===========================================================================
# Tests
# ===========================================================================

class TestPathConsistency:
    """Discovery scorer and runner must produce identical predictions."""

    def test_base_features_only(self, base_data):
        """All model features are base features — no aux columns needed."""
        X_wide, y, circuit, train_idx, test_idx = base_data
        # Select cols 0, 3 (player_elo, win_rate) — no derived features
        col_indices = np.array([0, 3])

        y_fs = _run_fast_selection_path(
            X_wide, y, circuit, train_idx, test_idx, col_indices, WIDE_SPECS
        )

        # Runner receives only the selected columns
        narrow_specs = [
            ImputeSpec(col_index=0, strategy="constant", constant=1500.0),
            ImputeSpec(col_index=1, strategy="median"),
        ]
        X_narrow = X_wide[:, col_indices]
        y_run = _run_runner_path(
            X_narrow, y, circuit, train_idx, test_idx, narrow_specs, n_model=2
        )

        np.testing.assert_allclose(y_fs, y_run, atol=1e-12)

    def test_derived_feature_with_bases_in_model(self, base_data):
        """Model includes elo_diff AND both bases — no aux needed."""
        X_wide, y, circuit, train_idx, test_idx = base_data
        # Select cols 0, 1, 2 (player_elo, opp_elo, elo_diff)
        col_indices = np.array([0, 1, 2])

        y_fs = _run_fast_selection_path(
            X_wide, y, circuit, train_idx, test_idx, col_indices, WIDE_SPECS
        )

        narrow_specs = [
            ImputeSpec(col_index=0, strategy="constant", constant=1500.0),
            ImputeSpec(col_index=1, strategy="constant", constant=1500.0),
            ImputeSpec(
                col_index=2,
                strategy="recompute",
                recompute=RecomputeInfo(player_base_idx=0, opp_base_idx=1),
            ),
        ]
        X_narrow = X_wide[:, col_indices]
        y_run = _run_runner_path(
            X_narrow, y, circuit, train_idx, test_idx, narrow_specs, n_model=3
        )

        np.testing.assert_allclose(y_fs, y_run, atol=1e-12)

    def test_derived_feature_bases_not_in_model(self, base_data):
        """Model includes elo_diff but NOT the bases — aux columns required.

        This is the critical case: FastForwardSelector subsets from the wide
        pre-fitted imputation state, while the runner fits fresh on the narrow
        augmented matrix.  Both must produce identical predictions.
        """
        X_wide, y, circuit, train_idx, test_idx = base_data
        # Select cols 2, 3 (elo_diff, win_rate) — bases 0,1 become aux
        col_indices = np.array([2, 3])

        y_fs = _run_fast_selection_path(
            X_wide, y, circuit, train_idx, test_idx, col_indices, WIDE_SPECS
        )

        # Runner: model features are elo_diff (idx 0) and win_rate (idx 1),
        # aux features are player_elo (idx 2) and opp_elo (idx 3)
        narrow_specs = [
            ImputeSpec(
                col_index=0,
                strategy="recompute",
                recompute=RecomputeInfo(player_base_idx=2, opp_base_idx=3),
            ),
            ImputeSpec(col_index=1, strategy="median"),
            ImputeSpec(col_index=2, strategy="constant", constant=1500.0),
            ImputeSpec(col_index=3, strategy="constant", constant=1500.0),
        ]
        # Narrow matrix: [elo_diff, win_rate, player_elo, opp_elo]
        X_narrow = X_wide[:, [2, 3, 0, 1]]
        y_run = _run_runner_path(
            X_narrow, y, circuit, train_idx, test_idx, narrow_specs, n_model=2
        )

        np.testing.assert_allclose(y_fs, y_run, atol=1e-12)

    def test_high_nan_rate_feature(self, rng):
        """Feature with ~50% NaN — stresses imputation path divergence."""
        n = 600
        feat_a = rng.normal(0, 1, n)
        feat_b = rng.normal(0, 1, n)

        # 50% NaN on feat_a
        nan_idx = rng.choice(n, size=n // 2, replace=False)
        feat_a[nan_idx] = np.nan

        X_wide = np.column_stack([feat_a, feat_b])
        y = rng.randint(0, 2, n)
        circuit = np.where(rng.rand(n) < 0.5, "TOUR", "CHAL")

        train_idx = np.arange(400)
        test_idx = np.arange(400, n)

        wide_specs = [
            ImputeSpec(col_index=0, strategy="median"),
            ImputeSpec(col_index=1, strategy="median"),
        ]
        col_indices = np.array([0, 1])

        y_fs = _run_fast_selection_path(
            X_wide, y, circuit, train_idx, test_idx, col_indices, wide_specs
        )
        X_narrow = X_wide[:, col_indices]
        y_run = _run_runner_path(
            X_narrow, y, circuit, train_idx, test_idx, wide_specs, n_model=2
        )

        np.testing.assert_allclose(y_fs, y_run, atol=1e-12)
