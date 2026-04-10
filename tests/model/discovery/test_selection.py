"""Tests for feature selection algorithms."""

from datetime import datetime, timezone

import pytest

from mvp.model.discovery.checkpoint import SelectionCheckpoint, save_checkpoint
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


class TestForwardSelectionCheckpoint:
    """Tests for checkpoint/resume in forward selection."""

    @pytest.fixture
    def mock_scorer(self):
        """Scorer that prefers features a > b > c > d."""
        def scorer(features: list[str]) -> float:
            score = 1.0
            if "a" in features:
                score -= 0.3
            if "b" in features:
                score -= 0.2
            if "c" in features:
                score -= 0.1
            if "d" in features:
                score -= 0.05
            return score

        return scorer

    def test_no_checkpoint_path_behaves_as_before(self, mock_scorer):
        """Without checkpoint_path, behaves exactly as before."""
        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c"],
            method="forward",
            direction="minimize",
        )

        result = selector.forward_selection(verbose=False)

        assert result.selected_features[0] == "a"
        assert isinstance(result, SelectionResult)

    def test_resume_skips_completed_rounds(self, mock_scorer, tmp_path):
        """Resuming from checkpoint skips already-selected features."""
        cp_path = tmp_path / "checkpoint.json"
        save_checkpoint(cp_path, SelectionCheckpoint(
            run_name="test",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            completed_rounds=[{"feature": "a", "metric": 0.7}],
            current_round=2,
            total_candidates=3,
            current_round_scores={},
            best_metric=0.7,
            direction="minimize",
            max_features=10,
        ))

        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c", "d"],
            method="forward",
            direction="minimize",
        )

        result = selector.forward_selection(
            verbose=False, checkpoint_path=cp_path,
        )

        # "a" was already selected via checkpoint
        assert result.selected_features[0] == "a"
        # "b" should be selected next (best remaining)
        assert result.selected_features[1] == "b"
        # Checkpoint file should be cleaned up on success
        assert not cp_path.exists()

    def test_resume_skips_evaluated_candidates(self, mock_scorer, tmp_path):
        """Resuming mid-round skips already-evaluated candidates."""
        call_log = []
        original_scorer = mock_scorer

        def tracking_scorer(features):
            call_log.append(features[-1])  # log which candidate was evaluated
            return original_scorer(features)

        cp_path = tmp_path / "checkpoint.json"
        # Checkpoint says: round 1, already evaluated "a" and "b"
        save_checkpoint(cp_path, SelectionCheckpoint(
            run_name="test",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            completed_rounds=[],
            current_round=1,
            total_candidates=4,
            current_round_scores={"a": 0.7, "b": 0.8},
            best_metric=float("inf"),
            direction="minimize",
            max_features=10,
        ))

        selector = FeatureSelector(
            scorer=tracking_scorer,
            all_features=["a", "b", "c", "d"],
            method="forward",
            direction="minimize",
        )

        result = selector.forward_selection(
            verbose=False, checkpoint_path=cp_path,
        )

        # Round 1 should NOT have re-evaluated "a" or "b"
        round1_calls = [c for c in call_log if len({"a", "b"} & {c}) > 0]
        # First two calls should be only c, d (since a, b came from checkpoint)
        assert "a" not in call_log[:2]
        assert "b" not in call_log[:2]
        # Result should still pick the best overall: a (score 0.7 from ckpt)
        assert result.selected_features[0] == "a"

    def test_checkpoint_deleted_on_completion(self, mock_scorer, tmp_path):
        """Checkpoint file is deleted after successful forward selection."""
        cp_path = tmp_path / "checkpoint.json"

        selector = FeatureSelector(
            scorer=mock_scorer,
            all_features=["a", "b", "c", "d"],
            method="forward",
            direction="minimize",
        )

        result = selector.forward_selection(
            verbose=False,
            checkpoint_path=cp_path,
            checkpoint_interval=1,
        )

        # Checkpoint should be deleted on successful completion
        assert not cp_path.exists()
        # Selection should have worked normally
        assert result.selected_features[0] == "a"

    def test_checkpoint_written_during_round(self, mock_scorer, tmp_path):
        """Checkpoint file gets written while inside a round."""
        cp_path = tmp_path / "checkpoint.json"

        # Scorer that interrupts after 2 evaluations, simulating Ctrl+C.
        # KeyboardInterrupt is not caught by `except Exception`, so it
        # propagates out and leaves the checkpoint file behind.
        call_count = {"n": 0}

        def interrupting_scorer(features):
            call_count["n"] += 1
            if call_count["n"] >= 3:
                raise KeyboardInterrupt("stop")
            return mock_scorer(features)

        selector = FeatureSelector(
            scorer=interrupting_scorer,
            all_features=["a", "b", "c", "d"],
            method="forward",
            direction="minimize",
        )

        with pytest.raises(KeyboardInterrupt):
            selector.forward_selection(
                verbose=False,
                checkpoint_path=cp_path,
                checkpoint_interval=1,
            )

        # Checkpoint should exist with at least one scored candidate
        assert cp_path.exists()
        from mvp.model.discovery.checkpoint import load_checkpoint
        cp = load_checkpoint(cp_path)
        assert len(cp.current_round_scores) >= 1


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
