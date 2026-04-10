"""Tests for discovery checkpoint I/O."""

import json
from datetime import datetime, timezone

from mvp.model.discovery.checkpoint import (
    SelectionCheckpoint,
    format_checkpoint_info,
    load_checkpoint,
    save_checkpoint,
)


class TestCheckpointRoundTrip:
    """Checkpoint saves and loads correctly."""

    def test_save_and_load(self, tmp_path):
        """Saved checkpoint loads back with same data."""
        path = tmp_path / "checkpoint.json"
        cp = SelectionCheckpoint(
            run_name="my_run",
            started_at=datetime(2026, 4, 9, 10, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, 14, 32, tzinfo=timezone.utc),
            completed_rounds=[
                {"feature": "feat_a", "metric": 0.680},
                {"feature": "feat_b", "metric": 0.672},
            ],
            current_round=3,
            total_candidates=2156,
            current_round_scores={"feat_c": 0.669, "feat_d": 0.671},
            best_metric=0.672,
            direction="minimize",
            max_features=15,
        )
        save_checkpoint(path, cp)
        loaded = load_checkpoint(path)

        assert loaded.run_name == "my_run"
        assert len(loaded.completed_rounds) == 2
        assert loaded.completed_rounds[0]["feature"] == "feat_a"
        assert loaded.current_round == 3
        assert loaded.total_candidates == 2156
        assert loaded.current_round_scores == {"feat_c": 0.669, "feat_d": 0.671}
        assert loaded.best_metric == 0.672
        assert loaded.direction == "minimize"
        assert loaded.max_features == 15

    def test_load_nonexistent_returns_none(self, tmp_path):
        """Loading from a path that doesn't exist returns None."""
        path = tmp_path / "nope.json"
        assert load_checkpoint(path) is None

    def test_save_overwrites_existing(self, tmp_path):
        """Saving to an existing path overwrites it."""
        path = tmp_path / "checkpoint.json"
        cp1 = SelectionCheckpoint(
            run_name="run1",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            completed_rounds=[],
            current_round=1,
            total_candidates=100,
            current_round_scores={},
            best_metric=float("inf"),
            direction="minimize",
            max_features=10,
        )
        save_checkpoint(path, cp1)

        cp2 = SelectionCheckpoint(
            run_name="run1",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, 1, tzinfo=timezone.utc),
            completed_rounds=[{"feature": "x", "metric": 0.5}],
            current_round=2,
            total_candidates=99,
            current_round_scores={"y": 0.48},
            best_metric=0.5,
            direction="minimize",
            max_features=10,
        )
        save_checkpoint(path, cp2)

        loaded = load_checkpoint(path)
        assert loaded.current_round == 2
        assert len(loaded.completed_rounds) == 1

    def test_save_is_valid_json(self, tmp_path):
        """Checkpoint file is human-readable JSON."""
        path = tmp_path / "checkpoint.json"
        cp = SelectionCheckpoint(
            run_name="test",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            completed_rounds=[],
            current_round=1,
            total_candidates=50,
            current_round_scores={},
            best_metric=float("inf"),
            direction="minimize",
            max_features=5,
        )
        save_checkpoint(path, cp)

        raw = json.loads(path.read_text())
        assert raw["run_name"] == "test"
        assert raw["current_round"] == 1

    def test_infinity_round_trips(self, tmp_path):
        """float('inf') survives the JSON round-trip."""
        path = tmp_path / "checkpoint.json"
        cp = SelectionCheckpoint(
            run_name="fresh",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            completed_rounds=[],
            current_round=1,
            total_candidates=10,
            current_round_scores={},
            best_metric=float("inf"),
            direction="minimize",
            max_features=5,
        )
        save_checkpoint(path, cp)
        loaded = load_checkpoint(path)
        assert loaded.best_metric == float("inf")

    def test_negative_infinity_round_trips(self, tmp_path):
        """float('-inf') survives the JSON round-trip (maximize direction)."""
        path = tmp_path / "checkpoint.json"
        cp = SelectionCheckpoint(
            run_name="fresh",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            completed_rounds=[],
            current_round=1,
            total_candidates=10,
            current_round_scores={},
            best_metric=float("-inf"),
            direction="maximize",
            max_features=5,
        )
        save_checkpoint(path, cp)
        loaded = load_checkpoint(path)
        assert loaded.best_metric == float("-inf")


class TestFormatCheckpointInfo:
    """format_checkpoint_info produces a useful message."""

    def test_with_progress(self):
        """Shows round and candidate counts."""
        cp = SelectionCheckpoint(
            run_name="my_run",
            started_at=datetime(2026, 4, 9, 10, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, 14, 32, tzinfo=timezone.utc),
            completed_rounds=[
                {"feature": "feat_a", "metric": 0.680},
                {"feature": "feat_b", "metric": 0.672},
            ],
            current_round=3,
            total_candidates=2156,
            current_round_scores={"feat_c": 0.669, "feat_d": 0.671},
            best_metric=0.672,
            direction="minimize",
            max_features=15,
        )
        msg = format_checkpoint_info(cp)
        assert "my_run" in msg
        assert "round 3/15" in msg
        assert "2/2156" in msg
        assert "2026-04-09" in msg

    def test_no_current_round_scores(self):
        """Works when checkpoint is between rounds."""
        cp = SelectionCheckpoint(
            run_name="clean",
            started_at=datetime(2026, 4, 9, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc),
            completed_rounds=[{"feature": "a", "metric": 0.5}],
            current_round=2,
            total_candidates=100,
            current_round_scores={},
            best_metric=0.5,
            direction="minimize",
            max_features=10,
        )
        msg = format_checkpoint_info(cp)
        assert "round 2/10" in msg
        assert "0/100" in msg
