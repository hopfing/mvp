"""Smoke tests for mvp.model.backtest."""


import json
from pathlib import Path

import pytest

from mvp.model.backtest import (
    _find_diagnostics_json,
    _load_tier_lookup,
    classify_cal_tier,
)


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
