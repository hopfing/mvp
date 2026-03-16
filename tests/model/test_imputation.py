"""Tests for mvp.model.imputation."""

import numpy as np
import pytest

from mvp.model.registry import FeatureDef, FeatureRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_registry(*features: tuple[str, float | str]) -> FeatureRegistry:
    """Build a minimal registry with the given (name, impute) pairs."""
    reg = FeatureRegistry()
    for name, impute in features:
        reg.register(FeatureDef(name=name, func=lambda: None, impute=impute))
    return reg


# ===========================================================================
# TestBuildImputeSpecs
# ===========================================================================

class TestBuildImputeSpecs:
    def test_median_default(self):
        from mvp.model.imputation import build_impute_specs

        reg = _make_registry(("win_pct", "median"))
        specs = build_impute_specs(["player_win_pct"], reg)

        assert len(specs) == 1
        assert specs[0].strategy == "median"
        assert specs[0].constant is None

    def test_constant_impute(self):
        from mvp.model.imputation import build_impute_specs

        reg = _make_registry(("h2h_win_pct", 0.5))
        specs = build_impute_specs(["player_h2h_win_pct"], reg)

        assert len(specs) == 1
        assert specs[0].strategy == "constant"
        assert specs[0].constant == 0.5

    def test_column_index_assignment(self):
        from mvp.model.imputation import build_impute_specs

        reg = _make_registry(("win_pct", "median"), ("h2h_win_pct", 0.5))
        specs = build_impute_specs(
            ["player_win_pct", "player_h2h_win_pct"], reg
        )

        assert specs[0].col_index == 0
        assert specs[1].col_index == 1


# ===========================================================================
# TestFitImputation
# ===========================================================================

class TestFitImputation:
    def test_circuit_stratified_medians(self):
        from mvp.model.imputation import ImputeSpec, fit_imputation

        specs = [ImputeSpec(col_index=0, strategy="median")]
        # Tour rows: [10, 20, 30], Chal rows: [100, 200, 300]
        X = np.array([[10.0], [20.0], [30.0], [100.0], [200.0], [300.0]])
        circuit = np.array(["TOUR", "TOUR", "TOUR", "CHAL", "CHAL", "CHAL"])

        state = fit_imputation(X, circuit, specs)

        assert state.circuit_medians["TOUR"][0] == pytest.approx(20.0)
        assert state.circuit_medians["CHAL"][0] == pytest.approx(200.0)

    def test_constant_feature_ignores_circuit(self):
        from mvp.model.imputation import ImputeSpec, fit_imputation

        specs = [ImputeSpec(col_index=0, strategy="constant", constant=0.5)]
        X = np.array([[10.0], [20.0], [100.0], [200.0]])
        circuit = np.array(["TOUR", "TOUR", "CHAL", "CHAL"])

        state = fit_imputation(X, circuit, specs)

        # Constant features should not appear in circuit medians
        # (they won't be used for this column anyway)
        # Global medians still computed for all columns
        assert state.global_medians[0] == pytest.approx(60.0)  # median of [10,20,100,200]

    def test_sparse_circuit_falls_back_to_global(self):
        from mvp.model.imputation import ImputeSpec, fit_imputation

        specs = [ImputeSpec(col_index=0, strategy="median")]
        # TOUR: 50 non-NaN, CHAL: only 2 non-NaN (below min_circuit_samples=30)
        tour_vals = np.full((50, 1), 10.0)
        chal_vals = np.array([[100.0], [200.0]])
        X = np.vstack([tour_vals, chal_vals])
        circuit = np.array(["TOUR"] * 50 + ["CHAL"] * 2)

        state = fit_imputation(X, circuit, specs, min_circuit_samples=30)

        # CHAL median should fall back to global median
        global_med = np.nanmedian(X[:, 0])
        assert state.circuit_medians["CHAL"][0] == pytest.approx(global_med)
        # TOUR should keep its own median
        assert state.circuit_medians["TOUR"][0] == pytest.approx(10.0)

    def test_all_nan_column_falls_back_to_zero(self):
        from mvp.model.imputation import ImputeSpec, fit_imputation

        specs = [ImputeSpec(col_index=0, strategy="median")]
        X = np.array([[np.nan], [np.nan], [np.nan]])
        circuit = np.array(["TOUR", "TOUR", "TOUR"])

        state = fit_imputation(X, circuit, specs)

        assert state.global_medians[0] == 0.0
        assert state.circuit_medians["TOUR"][0] == 0.0


# ===========================================================================
# TestApplyImputation
# ===========================================================================

class TestApplyImputation:
    def test_median_uses_circuit_specific_value(self):
        from mvp.model.imputation import ImputeSpec, fit_imputation, apply_imputation

        specs = [ImputeSpec(col_index=0, strategy="median")]
        X_train = np.array([[10.0], [20.0], [30.0], [100.0], [200.0], [300.0]])
        circuit_train = np.array(["TOUR", "TOUR", "TOUR", "CHAL", "CHAL", "CHAL"])
        state = fit_imputation(X_train, circuit_train, specs)

        X_test = np.array([[np.nan], [np.nan]])
        circuit_test = np.array(["TOUR", "CHAL"])

        result = apply_imputation(X_test, circuit_test, state)

        assert result[0, 0] == pytest.approx(20.0)   # TOUR median
        assert result[1, 0] == pytest.approx(200.0)   # CHAL median

    def test_constant_feature_ignores_circuit(self):
        from mvp.model.imputation import ImputeSpec, ImputeState, apply_imputation

        state = ImputeState(
            specs=[ImputeSpec(col_index=0, strategy="constant", constant=0.5)],
            circuit_medians={"TOUR": np.array([99.0])},
            global_medians=np.array([99.0]),
            circuit_labels=["TOUR"],
        )

        X = np.array([[np.nan], [np.nan]])
        circuit = np.array(["TOUR", "CHAL"])

        result = apply_imputation(X, circuit, state)

        assert result[0, 0] == pytest.approx(0.5)
        assert result[1, 0] == pytest.approx(0.5)

    def test_unknown_circuit_uses_global_median(self):
        from mvp.model.imputation import ImputeSpec, fit_imputation, apply_imputation

        specs = [ImputeSpec(col_index=0, strategy="median")]
        X_train = np.array([[10.0], [20.0], [30.0]])
        circuit_train = np.array(["TOUR", "TOUR", "TOUR"])
        state = fit_imputation(X_train, circuit_train, specs)

        X_test = np.array([[np.nan]])
        circuit_test = np.array(["UNKNOWN"])

        result = apply_imputation(X_test, circuit_test, state)

        assert result[0, 0] == pytest.approx(state.global_medians[0])

    def test_returns_copy(self):
        from mvp.model.imputation import ImputeSpec, ImputeState, apply_imputation

        state = ImputeState(
            specs=[ImputeSpec(col_index=0, strategy="constant", constant=0.0)],
            circuit_medians={},
            global_medians=np.array([0.0]),
            circuit_labels=[],
        )

        X = np.array([[np.nan], [5.0]])
        circuit = np.array(["TOUR", "TOUR"])

        result = apply_imputation(X, circuit, state)

        # Input not mutated
        assert np.isnan(X[0, 0])
        # Result is a different object
        assert result is not X

    def test_no_nan_passthrough(self):
        from mvp.model.imputation import ImputeSpec, ImputeState, apply_imputation

        state = ImputeState(
            specs=[ImputeSpec(col_index=0, strategy="median")],
            circuit_medians={"TOUR": np.array([99.0])},
            global_medians=np.array([99.0]),
            circuit_labels=["TOUR"],
        )

        X = np.array([[1.0], [2.0], [3.0]])
        circuit = np.array(["TOUR", "TOUR", "TOUR"])

        result = apply_imputation(X, circuit, state)

        np.testing.assert_array_equal(result, X)
