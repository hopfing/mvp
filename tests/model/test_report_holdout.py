"""The held-out read in model-report and model-rank: both read the
diagnostics.json `holdout` block a held-out-read run writes."""

from mvp.model.rank import _summary_from_diagnostics
from mvp.model.report import format_section_holdout

_HOLDOUT = {
    "end": "2024-12-31",
    "holdout_end": "2025-12-31",
    "n_folds": 2,
    "fold_meta": [
        {"test_start": "2025-01-01", "test_end": "2025-06-29", "n_rows": 1200,
         "log_loss": 0.6123},
        {"test_start": "2025-07-01", "test_end": "2025-11-30", "n_rows": 800,
         "log_loss": 0.6345},
    ],
    "metrics": {"log_loss": 0.6211, "brier_score": 0.2142,
                "calibration_error": 0.0123},
}


def _diag(holdout=None) -> dict:
    d = {
        "segments": {"by_circuit": {"tour": {"overall": {
            "n_matches": 100, "log_loss": 0.6, "accuracy": 0.6, "roc_auc": 0.7,
            "brier_score": 0.2, "calibration_error": 0.01,
            "error_rate_80plus": 0.1, "signed_calibration": 0.0,
        }}}},
        "temporal": {},
    }
    if holdout is not None:
        d["holdout"] = holdout
    return d


class TestReportSection:
    def test_renders_the_holdout_block(self):
        out = format_section_holdout(_diag(_HOLDOUT))
        assert "Held-out read (2025-01-01 .. 2025-12-31)" in out
        assert "N=2,000" in out
        assert "LL=0.6211" in out
        assert "Brier=0.2142" in out
        assert "CalErr=1.23%" in out
        assert "2025-01-01 .. 2025-06-29" in out
        assert "0.6123" in out and "0.6345" in out

    def test_absent_without_the_block(self):
        assert format_section_holdout(_diag()) is None


class TestRankColumn:
    def test_holdout_log_loss_from_the_block(self):
        assert _summary_from_diagnostics(_diag(_HOLDOUT))["holdout_log_loss"] == 0.6211

    def test_blank_when_absent(self):
        assert _summary_from_diagnostics(_diag())["holdout_log_loss"] is None
