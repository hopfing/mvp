"""Tests for tuning review output."""

import optuna
import pytest

from mvp.model.tune_review import format_leaderboard, format_param_importance


@pytest.fixture
def populated_study(tmp_path):
    """Create a study with a few completed trials."""
    storage = f"sqlite:///{tmp_path / 'test.db'}"
    study = optuna.create_study(
        study_name="test_review",
        storage=storage,
        direction="minimize",
    )

    # Each trial has both tuning and holdout metrics. Holdout values are
    # arranged so the holdout ranking differs from the tuning ranking — this
    # is what lets us verify that the leaderboard always sorts by holdout.
    trial_data = [
        {
            "C": 0.1, "ll": 0.65, "cal": 0.02, "scal": 0.01, "err80": 0.12,
            "h_ll": 0.66, "h_cal": 0.025, "h_err80": 0.13,
        },
        {
            "C": 1.0, "ll": 0.63, "cal": 0.015, "scal": -0.005, "err80": 0.10,
            "h_ll": 0.62, "h_cal": 0.018, "h_err80": 0.11,
        },
        {
            "C": 10.0, "ll": 0.68, "cal": 0.03, "scal": 0.02, "err80": 0.15,
            "h_ll": 0.67, "h_cal": 0.012, "h_err80": 0.14,
        },
    ]

    for td in trial_data:
        trial = optuna.trial.create_trial(
            params={"C": td["C"]},
            distributions={"C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)},
            values=[td["ll"]],
            user_attrs={
                "log_loss": td["ll"],
                "calibration_error": td["cal"],
                "signed_calibration": td["scal"],
                "error_rate_80plus": td["err80"],
                "holdout_log_loss": td["h_ll"],
                "holdout_calibration_error": td["h_cal"],
                "holdout_error_rate_80plus": td["h_err80"],
                "duration_s": 5.0,
            },
        )
        study.add_trial(trial)

    return study


@pytest.fixture
def study_without_holdout(tmp_path):
    """Study from before holdout support — no holdout_* user_attrs."""
    storage = f"sqlite:///{tmp_path / 'legacy.db'}"
    study = optuna.create_study(
        study_name="legacy_review",
        storage=storage,
        direction="minimize",
    )
    for c, ll in [(0.1, 0.65), (1.0, 0.63), (10.0, 0.68)]:
        trial = optuna.trial.create_trial(
            params={"C": c},
            distributions={"C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)},
            values=[ll],
            user_attrs={"log_loss": ll, "duration_s": 5.0},
        )
        study.add_trial(trial)
    return study


class TestFormatLeaderboard:
    """Tests for leaderboard formatting."""

    def test_default_sorts_by_holdout_log_loss(self, populated_study):
        """Leaderboard sorts by holdout_log_loss by default (best holdout LL first)."""
        lines = format_leaderboard(populated_study, top_n=3)
        output = "\n".join(lines)
        # Best holdout_log_loss is 0.62 (trial C=1.0) — should appear in first row
        assert "0.6200" in output.split("\n")[2]

    def test_explicit_log_loss_auto_prefixes_to_holdout(self, populated_study):
        """Passing --sort log_loss still sorts by holdout_log_loss."""
        lines = format_leaderboard(populated_study, sort_by=["log_loss"], top_n=3)
        output = "\n".join(lines)
        # Same ranking as default — auto-prefixed to holdout_log_loss
        assert "0.6200" in output.split("\n")[2]

    def test_sort_by_calibration_uses_holdout(self, populated_study):
        """`--sort calibration_error` auto-prefixes and sorts by holdout_calibration_error."""
        lines = format_leaderboard(
            populated_study, sort_by=["calibration_error"], top_n=3
        )
        output = "\n".join(lines)
        # Best holdout cal is 0.012 = 1.20% (trial C=10.0) — should appear first
        assert "1.20%" in output.split("\n")[2]

    def test_top_n_limits_rows(self, populated_study):
        """Leaderboard respects top_n limit."""
        lines = format_leaderboard(populated_study, top_n=2)
        trial_lines = [l for l in lines if l.strip().startswith(("1.", "2.", "3."))]
        assert len(trial_lines) == 2

    def test_shows_all_metrics(self, populated_study):
        """Leaderboard displays both tuning and holdout LL plus cal/err80."""
        lines = format_leaderboard(populated_study, top_n=1)
        output = "\n".join(lines)
        assert "holdout_LL=" in output
        assert "LL=" in output
        assert "cal=" in output
        assert "err80=" in output

    def test_study_without_holdout_errors_clearly(self, study_without_holdout):
        """Studies tuned before holdout support get a clear error, no silent fallback."""
        lines = format_leaderboard(study_without_holdout, top_n=3)
        output = "\n".join(lines)
        assert "No holdout metrics" in output
        # Should NOT silently rank by tuning log_loss
        assert "0.6300" not in output


class TestFormatParamImportance:
    """Tests for param importance formatting."""

    def test_returns_lines(self, populated_study):
        """format_param_importance returns non-empty output."""
        lines = format_param_importance(populated_study)
        assert len(lines) > 0

    def test_handles_insufficient_trials(self, tmp_path):
        """Gracefully handles studies with too few trials for importance."""
        storage = f"sqlite:///{tmp_path / 'empty.db'}"
        study = optuna.create_study(study_name="empty", storage=storage, direction="minimize")
        lines = format_param_importance(study)
        assert len(lines) > 0
