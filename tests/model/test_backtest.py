"""Smoke tests for mvp.model.backtest."""


import json
from pathlib import Path

import polars as pl
import pytest

from mvp.model.backtest import (
    _apply_present_filters,
    _find_diagnostics_json,
    _load_tier_lookup,
    classify_cal_tier,
)


class TestApplyPresentFilters:
    """data.filters must hold on the per-side bet output.

    A directional filter on an anti-symmetric diff feature (player_age_diff) is
    undone by the two-sided bet expansion: it drops the excluded orientation at
    predict, then both sides are rebuilt. Re-applying on the bet rows — where
    each side carries its own orientation's value — makes the filter stick.
    """

    def _bets(self) -> pl.DataFrame:
        # Two matches, both sides each. Match A: p1 younger (-4), p2 older (+4).
        # Match B: p1 younger with the older side's diff carried as null (the
        # predict-time filter drops that orientation from the feature frame).
        return pl.DataFrame(
            {
                "match_uid": ["A", "A", "B", "B"],
                "player_id": ["a1", "a2", "b1", "b2"],
                "side": ["p1", "p2", "p1", "p2"],
                "circuit": ["tour", "tour", "chal", "chal"],
                "player_age_diff": [-4.0, 4.0, -2.0, None],
            }
        )

    def test_directional_diff_filter_drops_excluded_side(self):
        out = _apply_present_filters(self._bets(), {"player_age_diff": {"max": 0}})
        # Keeps only the younger side of each match; the +4 side and the null
        # side (mirror of a kept orientation) are both dropped.
        assert out["player_id"].to_list() == ["a1", "b1"]
        assert all(v <= 0 for v in out["player_age_diff"].to_list())

    def test_absent_filter_column_is_skipped(self):
        # draw_type isn't carried onto bet rows — skip it rather than raise.
        out = _apply_present_filters(
            self._bets(),
            {"draw_type": "singles", "player_age_diff": {"max": 0}},
        )
        assert out["player_id"].to_list() == ["a1", "b1"]

    def test_present_match_level_filter_is_noop_when_all_pass(self):
        out = _apply_present_filters(self._bets(), {"circuit": ["tour", "chal"]})
        assert len(out) == 4

    def test_none_and_empty_return_unchanged(self):
        bets = self._bets()
        assert _apply_present_filters(bets, None).equals(bets)
        assert _apply_present_filters(bets, {}).equals(bets)
        # A filter naming only absent columns is a pass-through, not an error.
        assert _apply_present_filters(bets, {"draw_type": "singles"}).equals(bets)


class TestClassifyCalTier:
    """Tier thresholds mirror scripts/review_models.py."""

    def test_none_returns_none(self):
        assert classify_cal_tier(None) is None

    @pytest.mark.parametrize(
        "cal,expected",
        [
            (0.05, "UnderC"),
            (0.02, "UnderC"),
            (0.019, "Optimal"),
            (0.0, "Optimal"),
            (-0.001, "Border"),
            (-0.005, "Border"),
            (-0.0051, "Risky"),
            (-0.01, "Risky"),
            (-0.0101, "Danger"),
            (-0.05, "Danger"),
        ],
    )
    def test_thresholds(self, cal, expected):
        assert classify_cal_tier(cal) == expected


class TestFindDiagnostics:
    def test_returns_none_when_no_mlruns(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert _find_diagnostics_json("nonexistent") is None

    def test_returns_latest_json_alongside_matching_yaml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        mlruns = tmp_path / "mlruns" / "15" / "abc" / "artifacts"
        mlruns.mkdir(parents=True)
        (mlruns / "test_cfg.yaml").write_text("data: {}\n")
        diag = mlruns / "diag.json"
        diag.write_text("{}")
        result = _find_diagnostics_json("test_cfg")
        assert result is not None
        assert result.resolve() == diag.resolve()


class TestLoadTierLookup:
    def test_missing_diagnostics_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        lookup, run_id = _load_tier_lookup("nope")
        assert lookup == {}
        assert run_id is None

    def test_extracts_per_circuit_round_calibration(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        artifacts = tmp_path / "mlruns" / "15" / "deadbeef99" / "artifacts"
        artifacts.mkdir(parents=True)
        (artifacts / "my_cfg.yaml").write_text("data: {}\n")
        diag_payload = {
            "segments": {
                "by_circuit": {
                    "chal": {
                        "round": {
                            "R32": {"signed_calibration": -0.012},
                            "QF": {"signed_calibration": 0.005},
                        }
                    },
                    "tour": {
                        "round": {
                            "R64": {"signed_calibration": -0.008},
                        }
                    },
                }
            }
        }
        (artifacts / "diag.json").write_text(json.dumps(diag_payload))
        lookup, run_id = _load_tier_lookup("my_cfg")
        assert lookup[("chal", "R32")] == pytest.approx(-0.012)
        assert lookup[("chal", "QF")] == pytest.approx(0.005)
        assert lookup[("tour", "R64")] == pytest.approx(-0.008)
        assert run_id == "deadbeef"
