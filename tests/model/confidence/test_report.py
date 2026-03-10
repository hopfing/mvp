"""Tests for confidence validation report."""

from mvp.model.confidence.metrics import ReliabilityProfile, WindowDistribution
from mvp.model.confidence.report import format_report
from mvp.model.confidence.validator import ValidationResult


class TestFormatReport:
    def _make_result(self) -> ValidationResult:
        dist = WindowDistribution(
            median=0.005, p25=-0.002, p75=0.012,
            min=-0.03, max=0.05, n_windows=33, median_n_per_window=100,
        )
        profile = ReliabilityProfile(
            n_matches=1000, accuracy=0.665, err80=0.13,
            signed_cal=0.005,
            cal_3mo=dist, cal_6mo=dist, cal_12mo=dist,
        )
        return ValidationResult(
            n_total=2000,
            profiles={
                "overall": {"overall": profile},
                "circuit:chal": {"overall": profile, "60-65%": profile},
                "circuit:tour": {"overall": profile},
                "elo_level:Q1(1350-1450)": {"overall": profile},
            },
        )

    def test_returns_string(self):
        result = self._make_result()
        report = format_report(result, model_name="test_model")
        assert isinstance(report, str)
        assert len(report) > 100

    def test_contains_model_name(self):
        result = self._make_result()
        report = format_report(result, model_name="test_model")
        assert "test_model" in report

    def test_contains_structural_sections(self):
        result = self._make_result()
        report = format_report(result, model_name="test_model")
        assert "circuit:chal" in report or "CHAL" in report.upper()

    def test_contains_modifier_sections(self):
        result = self._make_result()
        report = format_report(result, model_name="test_model")
        assert "elo_level" in report

    def test_shows_signed_calibration_direction(self):
        result = self._make_result()
        report = format_report(result, model_name="test_model")
        assert "under" in report.lower() or "+" in report
