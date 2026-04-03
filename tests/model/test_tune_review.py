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

    trial_data = [
        {"C": 0.1, "ll": 0.65, "cal": 0.02, "scal": 0.01, "err80": 0.12},
        {"C": 1.0, "ll": 0.63, "cal": 0.015, "scal": -0.005, "err80": 0.10},
        {"C": 10.0, "ll": 0.68, "cal": 0.03, "scal": 0.02, "err80": 0.15},
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
                "duration_s": 5.0,
            },
        )
        study.add_trial(trial)

    return study


class TestFormatLeaderboard:
    """Tests for leaderboard formatting."""

    def test_default_sort_by_log_loss(self, populated_study):
        """Leaderboard sorts by log_loss by default."""
        lines = format_leaderboard(populated_study, sort_by=["log_loss"], top_n=3)
        output = "\n".join(lines)
        assert "0.6300" in output.split("\n")[2]

    def test_sort_by_calibration(self, populated_study):
        """Leaderboard sorts by calibration_error when specified."""
        lines = format_leaderboard(populated_study, sort_by=["calibration_error"], top_n=3)
        output = "\n".join(lines)
        # Best cal (0.015 = 1.50%) should appear first
        assert "1.50%" in output.split("\n")[2]

    def test_top_n_limits_rows(self, populated_study):
        """Leaderboard respects top_n limit."""
        lines = format_leaderboard(populated_study, sort_by=["log_loss"], top_n=2)
        trial_lines = [l for l in lines if l.strip().startswith(("1.", "2.", "3."))]
        assert len(trial_lines) == 2

    def test_shows_all_metrics(self, populated_study):
        """Leaderboard displays all tracked metrics."""
        lines = format_leaderboard(populated_study, sort_by=["log_loss"], top_n=1)
        output = "\n".join(lines)
        assert "LL=" in output
        assert "cal=" in output
        assert "err80=" in output


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
