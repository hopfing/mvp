"""Tests for feature selection algorithms."""

import pytest

from mvp.model.discovery.selection import FeatureSelector, SelectionResult


class TestForwardSelection:
    """Tests for forward selection."""

    @pytest.fixture
    def mock_scorer(self):
        """Scorer that prefers features a > b > c."""
        def scorer(features: list[str]) -> float:
            # Lower is better
            score = 1.0
            if "a" in features:
                score -= 0.3
            if "b" in features:
                score -= 0.2
            if "c" in features:
                score -= 0.05
            if "noise" in features:
                score += 0.1  # noise hurts
            return score

        return scorer

    def test_selects_best_features_first(self, mock_scorer):
        """Should add features in order of improvement."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c", "noise"],
            method="forward",
            direction="minimize",
        )

        result = selector.run()

        # Should select a first (biggest improvement)
        assert result.history[0]["feature"] == "a"
        # Then b
        assert result.history[1]["feature"] == "b"
        # c might be selected, noise should not
        assert "noise" not in result.selected_features

    def test_stops_when_no_improvement(self, mock_scorer):
        """Should stop when adding features doesn't help."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c", "noise"],
            method="forward",
            direction="minimize",
        )

        result = selector.run()

        # Should have stopped before adding noise
        assert "noise" in result.excluded_features

    def test_respects_max_features(self, mock_scorer):
        """Should stop at max_features."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c"],
            method="forward",
            direction="minimize",
            max_features=1,
        )

        result = selector.run()

        assert len(result.selected_features) == 1
        assert result.selected_features[0] == "a"

    def test_handles_maximize_direction(self):
        """Should work with maximize direction."""
        def scorer(features):
            # Higher is better
            return len(features) * 0.1

        selector = FeatureSelector(
            scorer=scorer,
            all_features=["a", "b", "c"],
            method="forward",
            direction="maximize",
        )

        result = selector.run()

        # Should select all features (each adds 0.1)
        assert len(result.selected_features) == 3

    def test_returns_selection_result(self, mock_scorer):
        """Should return SelectionResult with all fields."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b"],
            method="forward",
        )

        result = selector.run()

        assert isinstance(result, SelectionResult)
        assert isinstance(result.selected_features, list)
        assert isinstance(result.excluded_features, list)
        assert isinstance(result.history, list)
        assert isinstance(result.final_metric, float)


class TestRecursiveElimination:
    """Tests for recursive elimination."""

    @pytest.fixture
    def mock_scorer(self):
        """Scorer where noise hurts, others help."""
        def scorer(features: list[str]) -> float:
            score = 0.5
            for f in features:
                if f == "noise":
                    score += 0.1  # noise hurts
                else:
                    score -= 0.05  # others help
            return score

        return scorer

    @pytest.fixture
    def mock_importance_fn(self):
        """Importance function that ranks noise lowest."""
        def importance_fn(features: list[str]) -> dict[str, float]:
            result = {}
            for f in features:
                if f == "noise":
                    result[f] = 0.01
                elif f == "a":
                    result[f] = 0.4
                elif f == "b":
                    result[f] = 0.3
                else:
                    result[f] = 0.2
            # Normalize
            total = sum(result.values())
            return {k: v / total for k, v in result.items()}

        return importance_fn

    def test_removes_least_important_first(self, mock_scorer, mock_importance_fn):
        """Should remove features in order of importance."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c", "noise"],
            method="recursive",
            direction="minimize",
            importance_fn=mock_importance_fn,
            min_features=1,
        )

        result = selector.run()

        # Noise should be removed first (lowest importance)
        removed = [h["feature"] for h in result.history if h.get("action") == "remove"]
        assert removed[0] == "noise"

    def test_stops_when_removal_hurts(self, mock_importance_fn):
        """Should stop when removing any feature degrades performance."""
        def scorer(features):
            # All features help equally
            return 1.0 - 0.1 * len(features)

        selector = FeatureSelector(
            scorer=scorer,
            all_features=["a", "b", "c"],
            method="recursive",
            direction="minimize",
            importance_fn=mock_importance_fn,
            min_features=1,
        )

        result = selector.run()

        # Should keep all or stop when removal hurts
        assert len(result.selected_features) >= 1

    def test_respects_min_features(self, mock_scorer, mock_importance_fn):
        """Should not go below min_features."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c", "noise"],
            method="recursive",
            direction="minimize",
            importance_fn=mock_importance_fn,
            min_features=2,
        )

        result = selector.run()

        assert len(result.selected_features) >= 2

    def test_requires_importance_fn(self, mock_scorer):
        """Should raise if importance_fn not provided."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b"],
            method="recursive",
        )

        with pytest.raises(ValueError, match="requires importance_fn"):
            selector.run()


class TestThresholdSelection:
    """Tests for threshold-based selection."""

    @pytest.fixture
    def mock_scorer(self):
        """Simple scorer."""
        return lambda features: 0.5 - 0.1 * len(features)

    @pytest.fixture
    def mock_importance_fn(self):
        """Importance with clear threshold separation."""
        def importance_fn(features: list[str]) -> dict[str, float]:
            importances = {
                "high1": 0.3,
                "high2": 0.25,
                "medium": 0.15,
                "low1": 0.03,
                "low2": 0.02,
            }
            result = {f: importances.get(f, 0.05) for f in features}
            total = sum(result.values())
            return {k: v / total for k, v in result.items()}

        return importance_fn

    def test_keeps_features_above_threshold(self, mock_scorer, mock_importance_fn):
        """Should keep features with importance >= threshold."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["high1", "high2", "medium", "low1", "low2"],
            method="threshold",
            importance_threshold=0.1,
            importance_fn=mock_importance_fn,
        )

        result = selector.run()

        # high1, high2, medium should be kept (>= 10%)
        assert "high1" in result.selected_features
        assert "high2" in result.selected_features
        # low features should be excluded
        assert "low1" in result.excluded_features
        assert "low2" in result.excluded_features

    def test_respects_min_features(self, mock_scorer, mock_importance_fn):
        """Should keep at least min_features even if below threshold."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["high1", "low1", "low2"],
            method="threshold",
            importance_threshold=0.5,  # Very high threshold
            importance_fn=mock_importance_fn,
            min_features=2,
        )

        result = selector.run()

        assert len(result.selected_features) >= 2

    def test_requires_importance_fn(self, mock_scorer):
        """Should raise if importance_fn not provided."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b"],
            method="threshold",
        )

        with pytest.raises(ValueError, match="requires importance_fn"):
            selector.run()


class TestFeatureSelectorRun:
    """Tests for the run() method dispatch."""

    def test_dispatches_to_forward(self):
        """Should call forward_selection for method='forward'."""
        selector = FeatureSelector(
            scorer=lambda f: 0.5,
            all_features=["a"],
            method="forward",
        )

        result = selector.run()
        assert isinstance(result, SelectionResult)

    def test_dispatches_to_recursive(self):
        """Should call recursive_elimination for method='recursive'."""
        selector = FeatureSelector(
            scorer=lambda f: 0.5,
            all_features=["a"],
            method="recursive",
            importance_fn=lambda f: {"a": 1.0},
        )

        result = selector.run()
        assert isinstance(result, SelectionResult)

    def test_dispatches_to_threshold(self):
        """Should call threshold_selection for method='threshold'."""
        selector = FeatureSelector(
            scorer=lambda f: 0.5,
            all_features=["a"],
            method="threshold",
            importance_fn=lambda f: {"a": 1.0},
        )

        result = selector.run()
        assert isinstance(result, SelectionResult)

    def test_raises_for_unknown_method(self):
        """Should raise for unknown method."""
        selector = FeatureSelector(
            scorer=lambda f: 0.5,
            all_features=["a"],
            method="unknown",  # type: ignore
        )

        with pytest.raises(ValueError, match="Unknown selection method"):
            selector.run()
