"""tune-review rendering for IID/projection studies.

Regression: IID trials carry bare in-fold metrics only — the tuner never gives
IIDProjectionRunner an outer block — but the sort keys were holdout-prefixed
regardless, so every IID leaderboard fell through to the "No holdout metrics
found" bail-out and the IID column block was unreachable.
"""

import optuna
import pytest

from mvp.model.tune_review import format_leaderboard


def _iid_trial(max_depth: int, crps_total: float, crps_spread: float,
               mae: float, ll: float, folds: list[float] | None = None):
    fold_metrics = [
        {"iid_crps_total_games": v, "mae": mae, "log_loss": ll}
        for v in (folds or [])
    ]
    return optuna.trial.create_trial(
        params={"max_depth": max_depth},
        distributions={"max_depth": optuna.distributions.IntDistribution(3, 8)},
        values=[crps_total],
        user_attrs={
            "_tuning_mode": "raw",
            "iid_crps_total_games": crps_total,
            "iid_crps_spread": crps_spread,
            "mae": mae,
            "log_loss": ll,
            "fold_metrics": fold_metrics,
            "duration_s": 900.0,
        },
    )


@pytest.fixture
def iid_study(tmp_path):
    """Three IID trials. Best CRPS_total is max_depth=5; best MAE is depth=8,
    so an explicit --sort must actually change the ordering."""
    storage = f"sqlite:///{tmp_path / 'iid.db'}"
    study = optuna.create_study(
        study_name="iid_review", storage=storage, direction="minimize",
    )
    study.add_trial(_iid_trial(3, 2.910, 3.44, 3.10, 0.641, [2.88, 2.94]))
    study.add_trial(_iid_trial(5, 2.880, 3.41, 3.08, 0.638, [2.80, 2.96]))
    study.add_trial(_iid_trial(8, 2.905, 3.46, 3.02, 0.644, [2.90, 2.91]))
    return study


class TestIIDLeaderboard:
    def test_renders_instead_of_bailing_out(self, iid_study):
        out = "\n".join(format_leaderboard(iid_study))
        assert "No holdout metrics found" not in out
        assert "CRPS_total" in out

    def test_default_sort_is_crps_total(self, iid_study):
        lines = format_leaderboard(iid_study)
        assert any("sorted by iid_crps_total_games" in ln for ln in lines)
        first_row = next(ln for ln in lines if ln.strip().startswith("1."))
        assert "CRPS_total=2.8800" in first_row

    def test_explicit_bare_sort_is_not_holdout_prefixed(self, iid_study):
        lines = format_leaderboard(iid_study, sort_by=["mae"])
        assert any("sorted by mae" in ln for ln in lines)
        assert not any("holdout_mae" in ln for ln in lines)
        out = "\n".join(lines)
        assert "No holdout metrics found" not in out

    def test_explicit_sort_reorders(self, iid_study):
        """Best MAE is a different trial than best CRPS — the sort must bite."""
        lines = format_leaderboard(iid_study, sort_by=["mae"])
        first_row = next(ln for ln in lines if ln.strip().startswith("1."))
        assert "MAE=3.0200" in first_row

    def test_shows_per_fold_spread(self, iid_study):
        """The per-fold breakdown is persisted per trial; on a flat plateau it is
        often wider than the gap between adjacent trials."""
        out = "\n".join(format_leaderboard(iid_study))
        assert "per fold:" in out
        assert "over 2 folds" in out

    def test_params_are_shown_for_copying(self, iid_study):
        out = "\n".join(format_leaderboard(iid_study))
        assert "max_depth: 5" in out

    def test_top_n_limits_rows(self, iid_study):
        lines = format_leaderboard(iid_study, top_n=2)
        rows = [ln for ln in lines if ln.strip()[:2] in ("1.", "2.", "3.")]
        assert len(rows) == 2
