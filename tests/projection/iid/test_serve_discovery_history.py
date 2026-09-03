"""The serve FS history file: a halted round leaves a `stop` record carrying
the round's ranking, in the same shape the classification path writes
(selection.py), so one reader serves both."""

import json
import math

from mvp.model.discovery.selection import _append_fs_history
from mvp.projection.iid.serve_discovery import ServeDiscoverySelector

_CLASSIFICATION_STOP_KEYS = {
    "round", "action", "reason", "metric", "best_candidate",
    "best_candidate_metric", "ranking",
}


class TestStopRecord:
    def test_halt_record_ranks_finite_candidates_and_keeps_shape(self):
        scores = {
            "a": 0.6010, "b": 0.6005, "c": math.nan, "d": 0.6020, "e": math.inf,
        }
        rec = ServeDiscoverySelector._stop_record(
            8, "improvement 0.000050 < min_delta 0.000100", 0.6006, "b", 0.6005,
            scores, minimize=True,
        )
        assert _CLASSIFICATION_STOP_KEYS <= set(rec)
        assert rec["action"] == "stop"
        assert rec["round"] == 8
        assert rec["best_candidate"] == "b"
        assert rec["best_candidate_metric"] == 0.6005
        assert rec["n_non_finite"] == 2
        assert [f for f, _ in rec["ranking"]] == ["b", "a", "d"]

    def test_maximize_direction_and_no_candidate(self):
        rec = ServeDiscoverySelector._stop_record(
            3, "no candidate produced a finite score", 0.70, None, -math.inf,
            {"x": 0.71, "y": 0.72}, minimize=False,
        )
        assert rec["best_candidate"] is None
        assert rec["best_candidate_metric"] is None
        assert [f for f, _ in rec["ranking"]] == ["y", "x"]

    def test_record_round_trips_through_the_history_file(self, tmp_path):
        hist = tmp_path / "fs_history_unit.jsonl"
        _append_fs_history(hist, {"round": 1, "action": "add", "feature": "a"})
        _append_fs_history(hist, ServeDiscoverySelector._stop_record(
            2, "improvement 0.000010 < min_delta 0.000100", 0.60, "b", 0.59999,
            {"b": 0.59999, "c": 0.601}, minimize=True,
        ))
        lines = [json.loads(ln) for ln in hist.read_text().splitlines()]
        assert [ln["action"] for ln in lines] == ["add", "stop"]
        assert lines[-1]["ranking"][0] == ["b", 0.59999]
