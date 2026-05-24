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


def _make_registry_ext(
    *features: tuple[str, float | str, list[str], bool],
) -> FeatureRegistry:
    """Build registry with (name, impute, depends_on, mirror) tuples."""
    reg = FeatureRegistry()
    for name, impute, depends_on, mirror in features:
        reg.register(
            FeatureDef(
                name=name,
                func=lambda: None,
                impute=impute,
                depends_on=depends_on,
                mirror=mirror,
            )
        )
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

        state = fit_imputation(X, circuit, specs, min_circuit_samples=1)

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
        state = fit_imputation(X_train, circuit_train, specs, min_circuit_samples=1)

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


class TestSubsetImputeState:
    """Tests for subset_impute_state."""

    def test_remaps_indices(self):
        """Column indices are remapped to subset positions."""
        from mvp.model.imputation import ImputeSpec, ImputeState, subset_impute_state

        state = ImputeState(
            specs=[
                ImputeSpec(col_index=0, strategy="median"),
                ImputeSpec(col_index=1, strategy="constant", constant=0.5),
                ImputeSpec(col_index=2, strategy="median"),
            ],
            circuit_medians={"TOUR": np.array([10.0, 20.0, 30.0])},
            global_medians=np.array([10.0, 20.0, 30.0]),
            circuit_labels=["TOUR"],
        )
        # Select columns 0 and 2 (skip 1)
        sub = subset_impute_state(state, np.array([0, 2]))
        assert len(sub.specs) == 2
        assert sub.specs[0].col_index == 0  # was 0, now 0
        assert sub.specs[0].strategy == "median"
        assert sub.specs[1].col_index == 1  # was 2, now 1
        assert sub.specs[1].strategy == "median"
        np.testing.assert_array_equal(sub.global_medians, [10.0, 30.0])
        np.testing.assert_array_equal(sub.circuit_medians["TOUR"], [10.0, 30.0])

    def test_works_with_apply_imputation(self):
        """Subset state produces correct imputation on narrow matrix."""
        from mvp.model.imputation import (
            ImputeSpec,
            ImputeState,
            apply_imputation,
            subset_impute_state,
        )

        state = ImputeState(
            specs=[
                ImputeSpec(col_index=0, strategy="median"),
                ImputeSpec(col_index=1, strategy="constant", constant=0.5),
                ImputeSpec(col_index=2, strategy="median"),
            ],
            circuit_medians={"TOUR": np.array([10.0, 20.0, 30.0])},
            global_medians=np.array([10.0, 20.0, 30.0]),
            circuit_labels=["TOUR"],
        )
        # Full matrix: 3 columns
        X_full = np.array([[np.nan, np.nan, np.nan]])
        circuit = np.array(["TOUR"])
        full_result = apply_imputation(X_full, circuit, state)

        # Subset to columns [0, 2]
        col_indices = np.array([0, 2])
        X_sub = X_full[:, col_indices].copy()
        sub_state = subset_impute_state(state, col_indices)
        sub_result = apply_imputation(X_sub, circuit, sub_state)

        # Should match the corresponding columns from full imputation
        np.testing.assert_array_equal(sub_result[:, 0], full_result[:, 0])
        np.testing.assert_array_equal(sub_result[:, 1], full_result[:, 2])


# ===========================================================================
# TestBuildImputation (new recompute-aware builder)
# ===========================================================================

class TestBuildImputation:
    """Tests for build_imputation with recompute support."""

    def test_single_dep_diff_detected(self):
        """Single-dep diff feature gets recompute strategy."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("win_pct", "median", [], True),       # base feature
            ("win_pct_diff", 0, ["win_pct"], False),  # diff feature
        )
        result = build_imputation(["player_win_pct_diff"], reg)

        # Should have 3 specs: 1 recompute (diff) + 2 median (aux bases)
        assert result.n_model_features == 1
        assert len(result.aux_base_col_names) == 2
        assert "player_win_pct" in result.aux_base_col_names
        assert "opp_win_pct" in result.aux_base_col_names

        recompute_specs = [s for s in result.specs if s.strategy == "recompute"]
        assert len(recompute_specs) == 1
        assert recompute_specs[0].col_index == 0
        assert recompute_specs[0].recompute is not None

    def test_two_dep_matchup_detected(self):
        """Two-dep matchup feature gets recompute with different bases."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("svc_won_pct", "median", [], True),
            ("ret_won_pct", "median", [], True),
            ("svc_matchup", 0, ["svc_won_pct", "ret_won_pct"], False),
        )
        result = build_imputation(["player_svc_matchup"], reg)

        assert result.n_model_features == 1
        assert "player_svc_won_pct" in result.aux_base_col_names
        assert "opp_ret_won_pct" in result.aux_base_col_names

    def test_base_already_in_features_no_duplicate(self):
        """If base column is already a model feature, no aux column added."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("win_pct", "median", [], True),
            ("win_pct_diff", 0, ["win_pct"], False),
        )
        # Both base and diff are model features
        result = build_imputation(
            ["player_win_pct", "opp_win_pct", "player_win_pct_diff"], reg
        )

        assert result.n_model_features == 3
        # Base columns are already model features — no aux needed
        assert len(result.aux_base_col_names) == 0

        recompute_specs = [s for s in result.specs if s.strategy == "recompute"]
        assert len(recompute_specs) == 1
        # Should reference the existing model feature indices
        assert recompute_specs[0].recompute.player_base_idx == 0
        assert recompute_specs[0].recompute.opp_base_idx == 1

    def test_median_feature_not_recomputed(self):
        """Feature with impute='median' is not recomputed even with depends_on."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("style_winner_rate", "median", [], True),
            ("is_aggressive", "median", ["style_winner_rate"], True),
        )
        result = build_imputation(["player_is_aggressive"], reg)

        assert len(result.aux_base_col_names) == 0
        assert all(s.strategy == "median" for s in result.specs)

    def test_mirror_true_not_recomputed(self):
        """Feature with mirror=True is not recomputed."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("base_stat", "median", [], True),
            ("derived_stat", 0, ["base_stat"], True),  # mirror=True
        )
        result = build_imputation(["player_derived_stat"], reg)

        assert len(result.aux_base_col_names) == 0
        assert all(s.strategy == "constant" for s in result.specs)

    def test_with_params(self):
        """Recompute works with parameterized features (days=30)."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("win_pct", "median", [], True),
            ("win_pct_diff", 0, ["win_pct"], False),
        )
        result = build_imputation(["player_win_pct_diff(days=30)"], reg)

        assert "player_win_pct_30d" in result.aux_base_col_names
        assert "opp_win_pct_30d" in result.aux_base_col_names


# ===========================================================================
# TestRecomputeImputation (end-to-end recompute behavior)
# ===========================================================================

class TestRecomputeImputation:
    """Tests for two-phase imputation with recompute."""

    def _build_augmented_state(self):
        """Build a test scenario: diff + 2 base columns, with circuit medians.

        Augmented matrix layout: [diff, player_base, opp_base]
        Columns 1 and 2 are aux base columns imputed via median.
        Column 0 is recomputed as col1 - col2.
        """
        from mvp.model.imputation import ImputeSpec, ImputeState, RecomputeInfo

        specs = [
            ImputeSpec(
                col_index=0,
                strategy="recompute",
                recompute=RecomputeInfo(player_base_idx=1, opp_base_idx=2),
            ),
            ImputeSpec(col_index=1, strategy="median"),
            ImputeSpec(col_index=2, strategy="median"),
        ]
        state = ImputeState(
            specs=specs,
            circuit_medians={
                "TOUR": np.array([0.0, 0.60, 0.55]),
                "CHAL": np.array([0.0, 0.50, 0.45]),
            },
            global_medians=np.array([0.0, 0.55, 0.50]),
            circuit_labels=["TOUR", "CHAL"],
        )
        return state

    def test_asymmetric_nan_player_has_data(self):
        """Player has data, opp is NaN → diff uses imputed opp base."""
        from mvp.model.imputation import apply_imputation

        state = self._build_augmented_state()

        #             diff    player_base  opp_base
        X = np.array([[np.nan, 0.70,        np.nan]])  # opp missing
        circuit = np.array(["TOUR"])

        result = apply_imputation(X, circuit, state)

        # opp_base imputed to TOUR median (0.55)
        # diff recomputed: 0.70 - 0.55 = 0.15
        assert result[0, 0] == pytest.approx(0.15)
        assert result[0, 1] == pytest.approx(0.70)  # player unchanged
        assert result[0, 2] == pytest.approx(0.55)  # opp imputed

    def test_asymmetric_nan_opp_has_data(self):
        """Opp has data, player is NaN → diff uses imputed player base."""
        from mvp.model.imputation import apply_imputation

        state = self._build_augmented_state()

        X = np.array([[np.nan, np.nan, 0.40]])  # player missing
        circuit = np.array(["CHAL"])

        result = apply_imputation(X, circuit, state)

        # player_base imputed to CHAL median (0.50)
        # diff recomputed: 0.50 - 0.40 = 0.10
        assert result[0, 0] == pytest.approx(0.10)

    def test_symmetric_nan_both_imputed(self):
        """Both bases NaN → diff is median - median ≈ small nonzero."""
        from mvp.model.imputation import apply_imputation

        state = self._build_augmented_state()

        X = np.array([[np.nan, np.nan, np.nan]])
        circuit = np.array(["TOUR"])

        result = apply_imputation(X, circuit, state)

        # Both imputed to TOUR medians: 0.60 - 0.55 = 0.05
        assert result[0, 0] == pytest.approx(0.05)

    def test_no_nan_recomputed_identically(self):
        """Both bases present → recompute matches original diff."""
        from mvp.model.imputation import apply_imputation

        state = self._build_augmented_state()

        # Original diff was computed as 0.70 - 0.40 = 0.30
        X = np.array([[0.30, 0.70, 0.40]])
        circuit = np.array(["TOUR"])

        result = apply_imputation(X, circuit, state)

        # Recomputed: 0.70 - 0.40 = 0.30 (same)
        assert result[0, 0] == pytest.approx(0.30)

    def test_two_dep_matchup_recompute(self):
        """Matchup feature (player_X - opp_Y) with different base deps."""
        from mvp.model.imputation import ImputeSpec, ImputeState, RecomputeInfo, apply_imputation

        # Layout: [matchup, player_svc, opp_ret]
        specs = [
            ImputeSpec(
                col_index=0,
                strategy="recompute",
                recompute=RecomputeInfo(player_base_idx=1, opp_base_idx=2),
            ),
            ImputeSpec(col_index=1, strategy="median"),
            ImputeSpec(col_index=2, strategy="median"),
        ]
        state = ImputeState(
            specs=specs,
            circuit_medians={
                "TOUR": np.array([0.0, 0.65, 0.35]),
            },
            global_medians=np.array([0.0, 0.65, 0.35]),
            circuit_labels=["TOUR"],
        )

        # Player has serve data, opp return is NaN
        X = np.array([[np.nan, 0.72, np.nan]])
        circuit = np.array(["TOUR"])

        result = apply_imputation(X, circuit, state)

        # opp_ret imputed to 0.35, matchup = 0.72 - 0.35 = 0.37
        assert result[0, 0] == pytest.approx(0.37)


class TestSubsetImputeStateRecompute:
    """Tests for subset_impute_state with recompute specs."""

    def test_recompute_spec_remapped(self):
        """Recompute spec indices are correctly remapped in subset."""
        from mvp.model.imputation import (
            ImputeSpec,
            ImputeState,
            RecomputeInfo,
            subset_impute_state,
        )

        state = ImputeState(
            specs=[
                ImputeSpec(col_index=0, strategy="median"),
                ImputeSpec(
                    col_index=1,
                    strategy="recompute",
                    recompute=RecomputeInfo(player_base_idx=3, opp_base_idx=4),
                ),
                ImputeSpec(col_index=2, strategy="median"),
                ImputeSpec(col_index=3, strategy="median"),
                ImputeSpec(col_index=4, strategy="median"),
            ],
            circuit_medians={"TOUR": np.array([1.0, 2.0, 3.0, 4.0, 5.0])},
            global_medians=np.array([1.0, 2.0, 3.0, 4.0, 5.0]),
            circuit_labels=["TOUR"],
        )

        # Select diff (1) + its bases (3, 4)
        sub = subset_impute_state(state, np.array([1, 3, 4]))

        recompute_specs = [s for s in sub.specs if s.strategy == "recompute"]
        assert len(recompute_specs) == 1
        assert recompute_specs[0].col_index == 0  # was 1, now 0
        assert recompute_specs[0].recompute.player_base_idx == 1  # was 3, now 1
        assert recompute_specs[0].recompute.opp_base_idx == 2  # was 4, now 2

    def test_recompute_dropped_when_bases_missing(self):
        """Recompute spec is dropped if base columns aren't in the subset."""
        from mvp.model.imputation import (
            ImputeSpec,
            ImputeState,
            RecomputeInfo,
            subset_impute_state,
        )

        state = ImputeState(
            specs=[
                ImputeSpec(
                    col_index=0,
                    strategy="recompute",
                    recompute=RecomputeInfo(player_base_idx=1, opp_base_idx=2),
                ),
                ImputeSpec(col_index=1, strategy="median"),
                ImputeSpec(col_index=2, strategy="median"),
            ],
            circuit_medians={"TOUR": np.array([0.0, 1.0, 2.0])},
            global_medians=np.array([0.0, 1.0, 2.0]),
            circuit_labels=["TOUR"],
        )

        # Select only the diff column without its bases
        sub = subset_impute_state(state, np.array([0]))

        # Recompute spec should be dropped (bases not available)
        assert len(sub.specs) == 0

    def test_subset_recompute_end_to_end(self):
        """Full flow: subset + apply_imputation with recompute."""
        from mvp.model.imputation import (
            ImputeSpec,
            ImputeState,
            RecomputeInfo,
            apply_imputation,
            subset_impute_state,
        )

        state = ImputeState(
            specs=[
                ImputeSpec(col_index=0, strategy="median"),
                ImputeSpec(
                    col_index=1,
                    strategy="recompute",
                    recompute=RecomputeInfo(player_base_idx=2, opp_base_idx=3),
                ),
                ImputeSpec(col_index=2, strategy="median"),
                ImputeSpec(col_index=3, strategy="median"),
            ],
            circuit_medians={
                "TOUR": np.array([99.0, 0.0, 0.60, 0.55]),
            },
            global_medians=np.array([99.0, 0.0, 0.60, 0.55]),
            circuit_labels=["TOUR"],
        )

        # Select diff (1) + bases (2, 3)
        col_indices = np.array([1, 2, 3])
        sub_state = subset_impute_state(state, col_indices)

        # Asymmetric NaN: player has data, opp missing
        X = np.array([[np.nan, 0.70, np.nan]])
        circuit = np.array(["TOUR"])

        result = apply_imputation(X, circuit, sub_state)

        # opp imputed to 0.55, diff = 0.70 - 0.55 = 0.15
        assert result[0, 0] == pytest.approx(0.15)
        assert result[0, 1] == pytest.approx(0.70)
        assert result[0, 2] == pytest.approx(0.55)


class TestAugmentedColIndices:
    """Tests for augmented_col_indices helper."""

    def test_no_recompute_returns_original(self):
        from mvp.model.imputation import ImputeSpec, augmented_col_indices

        specs = [
            ImputeSpec(col_index=0, strategy="median"),
            ImputeSpec(col_index=1, strategy="constant", constant=0.0),
        ]
        model_idx = np.array([0, 1])
        aug, n_model = augmented_col_indices(model_idx, specs)

        np.testing.assert_array_equal(aug, [0, 1])
        assert n_model == 2

    def test_adds_aux_for_recompute(self):
        from mvp.model.imputation import ImputeSpec, RecomputeInfo, augmented_col_indices

        specs = [
            ImputeSpec(
                col_index=0,
                strategy="recompute",
                recompute=RecomputeInfo(player_base_idx=3, opp_base_idx=4),
            ),
            ImputeSpec(col_index=1, strategy="median"),
            ImputeSpec(col_index=3, strategy="median"),
            ImputeSpec(col_index=4, strategy="median"),
        ]
        # Model selects column 0 (diff) — needs aux 3, 4
        model_idx = np.array([0])
        aug, n_model = augmented_col_indices(model_idx, specs)

        assert n_model == 1
        assert set(aug.tolist()) == {0, 3, 4}
        assert aug[0] == 0  # model feature first

    def test_base_already_in_model_no_duplicate(self):
        from mvp.model.imputation import ImputeSpec, RecomputeInfo, augmented_col_indices

        specs = [
            ImputeSpec(
                col_index=0,
                strategy="recompute",
                recompute=RecomputeInfo(player_base_idx=1, opp_base_idx=2),
            ),
            ImputeSpec(col_index=1, strategy="median"),
            ImputeSpec(col_index=2, strategy="median"),
        ]
        # All columns already selected as model features
        model_idx = np.array([0, 1, 2])
        aug, n_model = augmented_col_indices(model_idx, specs)

        np.testing.assert_array_equal(aug, [0, 1, 2])
        assert n_model == 3


# ===========================================================================
# Passthrough strategy (impute=None) — added when impute=None was introduced
# to let NaN values flow through to NaN-tolerant models (XGBoost).
# ===========================================================================


class TestPassthroughBuildImputeSpecs:
    def test_none_impute_yields_passthrough(self):
        from mvp.model.imputation import build_impute_specs

        reg = _make_registry(("elo_diff", None))
        specs = build_impute_specs(["player_elo_diff"], reg)

        assert len(specs) == 1
        assert specs[0].strategy == "passthrough"
        assert specs[0].constant is None

    def test_mixed_strategies_all_resolve(self):
        from mvp.model.imputation import build_impute_specs

        reg = _make_registry(
            ("win_pct", "median"),
            ("elo_diff", None),
            ("h2h_win_pct", 0.5),
        )
        specs = build_impute_specs(
            ["player_win_pct", "player_elo_diff", "player_h2h_win_pct"], reg
        )

        assert [s.strategy for s in specs] == ["median", "passthrough", "constant"]
        assert specs[1].constant is None
        assert specs[2].constant == 0.5


class TestPassthroughBuildImputation:
    def test_none_diff_not_recomputed(self):
        """A diff feature with impute=None must NOT enter the recompute branch."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("elo", "median", [], True),
            ("elo_diff", None, ["elo"], False),
        )
        result = build_imputation(["player_elo_diff"], reg)

        model_specs = result.specs[: result.n_model_features]
        assert model_specs[0].strategy == "passthrough"
        # No aux base columns should have been added — recompute didn't fire
        assert result.aux_base_col_names == []

    def test_none_base_propagates_to_aux(self):
        """A recompute that pulls in a passthrough base produces a passthrough aux spec."""
        from mvp.model.imputation import build_imputation

        # diff_a depends on base_a (impute=None), uses recompute path because
        # impute_val=0 triggers recompute eligibility.
        reg = _make_registry_ext(
            ("base_a", None, [], True),
            ("diff_a", 0, ["base_a"], False),
        )
        result = build_imputation(["player_diff_a"], reg)

        model_specs = result.specs[: result.n_model_features]
        aux_specs = result.specs[result.n_model_features :]

        assert model_specs[0].strategy == "recompute"
        # Two aux specs (player + opp base) both passthrough since base is None
        assert len(aux_specs) == 2
        assert all(s.strategy == "passthrough" for s in aux_specs)

    def test_none_diff_skips_recompute_with_median_base(self):
        """When the diff itself is impute=None, recompute is bypassed even if bases are median."""
        from mvp.model.imputation import build_imputation

        reg = _make_registry_ext(
            ("base_a", "median", [], True),
            ("diff_a", None, ["base_a"], False),
        )
        result = build_imputation(["player_diff_a"], reg)

        # Should NOT have triggered recompute despite a recomputable shape
        model_specs = result.specs[: result.n_model_features]
        assert model_specs[0].strategy == "passthrough"
        assert result.aux_base_col_names == []


class TestPassthroughApplyImputation:
    def test_passthrough_column_keeps_nan(self):
        """A passthrough column must retain NaN end-to-end while neighbors are filled."""
        from mvp.model.imputation import (
            ImputeSpec,
            apply_imputation,
            fit_imputation,
        )

        X = np.array(
            [
                [1.0, np.nan, 100.0],
                [2.0, np.nan, 200.0],
                [np.nan, 5.0, np.nan],
                [4.0, 6.0, 400.0],
            ]
        )
        circuit = np.array(["tour", "tour", "chal", "chal"])
        specs = [
            ImputeSpec(col_index=0, strategy="median"),
            ImputeSpec(col_index=1, strategy="constant", constant=0.0),
            ImputeSpec(col_index=2, strategy="passthrough"),
        ]

        state = fit_imputation(X, circuit, specs, min_circuit_samples=1)
        result = apply_imputation(X, circuit, state)

        # Col 0: median-filled (no NaN)
        assert not np.isnan(result[:, 0]).any()
        # Col 1: constant-filled to 0
        assert not np.isnan(result[:, 1]).any()
        assert result[0, 1] == 0.0
        # Col 2: passthrough — NaN preserved
        assert np.isnan(result[2, 2])
        # Non-NaN values in col 2 should be untouched
        assert result[0, 2] == 100.0
        assert result[3, 2] == 400.0


class TestPassthroughSubsetImputeState:
    def test_passthrough_spec_round_trips(self):
        from mvp.model.imputation import (
            ImputeSpec,
            ImputeState,
            subset_impute_state,
        )

        state = ImputeState(
            specs=[
                ImputeSpec(col_index=0, strategy="median"),
                ImputeSpec(col_index=1, strategy="passthrough"),
                ImputeSpec(col_index=2, strategy="constant", constant=0.5),
            ],
            circuit_medians={"tour": np.array([1.0, 2.0, 3.0])},
            global_medians=np.array([1.0, 2.0, 3.0]),
            circuit_labels=["tour"],
        )
        # Subset selects all 3 columns in a new order
        new_state = subset_impute_state(state, np.array([2, 1, 0]))

        # Passthrough spec carried over with the remapped col_index
        passthrough = [s for s in new_state.specs if s.strategy == "passthrough"]
        assert len(passthrough) == 1
        assert passthrough[0].col_index == 1  # was idx 1, still at position 1 after subset


class TestValidateImputeCompatNoOp:
    """validate_impute_compat is a no-op kept for backwards compatibility.

    Non-XGB model wrappers handle NaN internally via per-column training
    median fitted at fit() time, so the central pipeline no longer needs
    to enforce a compatibility contract.
    """

    def test_no_op_for_any_combination(self):
        from mvp.model.imputation import ImputeSpec, validate_impute_compat

        specs = [
            ImputeSpec(col_index=0, strategy="passthrough"),
            ImputeSpec(col_index=1, strategy="median"),
        ]
        # Never raises, regardless of model type
        for mt in (
            "xgboost", "logistic", "random_forest", "neural_net",
            "sequence", "ensemble",
        ):
            validate_impute_compat(specs, ["a", "b"], mt)


class TestPassthroughStateRoundTrip:
    def test_pickle_round_trip(self):
        """ImputeState containing a passthrough spec survives pickle round-trip."""
        import pickle

        from mvp.model.imputation import ImputeSpec, ImputeState

        state = ImputeState(
            specs=[
                ImputeSpec(col_index=0, strategy="median"),
                ImputeSpec(col_index=1, strategy="passthrough"),
                ImputeSpec(col_index=2, strategy="constant", constant=0.0),
            ],
            circuit_medians={"tour": np.array([1.0, 2.0, 3.0])},
            global_medians=np.array([1.0, 2.0, 3.0]),
            circuit_labels=["tour"],
        )

        restored = pickle.loads(pickle.dumps(state))
        assert [s.strategy for s in restored.specs] == [
            "median", "passthrough", "constant",
        ]
        assert restored.specs[1].constant is None
