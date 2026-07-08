"""Tests for tuning review output."""

import optuna
import pytest

from mvp.model.tune_review import (
    _to_ranked,
    format_best_trial,
    format_leaderboard,
    format_param_importance,
)


@pytest.fixture
def populated_study(tmp_path):
    """Create a study with a few completed trials."""
    storage = f"sqlite:///{tmp_path / 'test.db'}"
    study = optuna.create_study(
        study_name="test_review",
        storage=storage,
        direction="minimize",
    )

    # Each trial has the full classification metric set (7 metrics) for both
    # in-fold and holdout. Values are arranged so that different holdout
    # metrics pick different winners, which lets us verify explicit `--sort`
    # routes to the corresponding holdout metric:
    #   holdout_log_loss best -> C=1.0 (0.62)
    #   holdout_calibration_error best -> C=10.0 (0.012)
    #   holdout_brier_score best -> C=1.0 (0.21)
    trial_data = [
        {
            "C": 0.1,
            "ll": 0.65, "brier": 0.23, "auc": 0.70, "acc": 0.63,
            "cal": 0.02, "scal": 0.01, "err80": 0.12,
            "h_ll": 0.66, "h_brier": 0.24, "h_auc": 0.69, "h_acc": 0.62,
            "h_cal": 0.025, "h_scal": 0.018, "h_err80": 0.13,
        },
        {
            "C": 1.0,
            "ll": 0.63, "brier": 0.22, "auc": 0.74, "acc": 0.68,
            "cal": 0.015, "scal": -0.005, "err80": 0.10,
            "h_ll": 0.62, "h_brier": 0.21, "h_auc": 0.75, "h_acc": 0.69,
            "h_cal": 0.018, "h_scal": -0.004, "h_err80": 0.11,
        },
        {
            "C": 10.0,
            "ll": 0.61, "brier": 0.225, "auc": 0.72, "acc": 0.66,
            "cal": 0.03, "scal": 0.02, "err80": 0.15,
            "h_ll": 0.67, "h_brier": 0.23, "h_auc": 0.71, "h_acc": 0.65,
            "h_cal": 0.012, "h_scal": 0.011, "h_err80": 0.14,
        },
    ]

    for td in trial_data:
        trial = optuna.trial.create_trial(
            params={"C": td["C"]},
            distributions={"C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)},
            values=[td["ll"]],
            user_attrs={
                "_tuning_mode": "raw",
                "log_loss": td["ll"],
                "brier_score": td["brier"],
                "roc_auc": td["auc"],
                "accuracy": td["acc"],
                "calibration_error": td["cal"],
                "signed_calibration": td["scal"],
                "error_rate_80plus": td["err80"],
                "holdout_log_loss": td["h_ll"],
                "holdout_brier_score": td["h_brier"],
                "holdout_roc_auc": td["h_auc"],
                "holdout_accuracy": td["h_acc"],
                "holdout_calibration_error": td["h_cal"],
                "holdout_signed_calibration": td["h_scal"],
                "holdout_error_rate_80plus": td["h_err80"],
                "duration_s": 5.0,
            },
        )
        study.add_trial(trial)

    return study


@pytest.fixture
def populated_cal_study(tmp_path):
    """Phase-2 classification study: trials carry both raw `holdout_*` and
    deployment-frame `holdout_cal_*` metrics, plus per-fold calibrated metrics.
    Calibrated log_loss picks a DIFFERENT winner (C=1.0) than raw log_loss
    (C=0.1), so the default sort must be ranking on the calibrated value."""
    storage = f"sqlite:///{tmp_path / 'cal.db'}"
    study = optuna.create_study(
        study_name="cal_review", storage=storage, direction="minimize"
    )
    # (C, raw holdout LL, cal holdout LL, cal per-fold LLs)
    trial_data = [
        {"C": 0.1, "h_ll": 0.62, "hc_ll": 0.64, "folds": [0.63, 0.65]},
        {"C": 1.0, "h_ll": 0.66, "hc_ll": 0.60, "folds": [0.58, 0.62]},
        {"C": 10.0, "h_ll": 0.65, "hc_ll": 0.63, "folds": [0.61, 0.65]},
    ]
    for td in trial_data:
        raw = {
            "holdout_log_loss": td["h_ll"], "holdout_brier_score": 0.22,
            "holdout_roc_auc": 0.73, "holdout_accuracy": 0.67,
            "holdout_calibration_error": 0.03,
            "holdout_calibration_error_max": 0.06,
            "holdout_overconfidence_max": 0.05,
            "holdout_signed_calibration": 0.01,
            "holdout_error_rate_80plus": 0.12,
        }
        cal = {
            "holdout_cal_log_loss": td["hc_ll"], "holdout_cal_brier_score": 0.21,
            "holdout_cal_roc_auc": 0.73, "holdout_cal_accuracy": 0.68,
            "holdout_cal_calibration_error": 0.012,
            "holdout_cal_calibration_error_max": 0.03,
            "holdout_cal_overconfidence_max": 0.02,
            "holdout_cal_signed_calibration": -0.004,
            "holdout_cal_error_rate_80plus": 0.10,
        }
        trial = optuna.trial.create_trial(
            params={"C": td["C"]},
            distributions={
                "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
            },
            values=[td["h_ll"]],
            user_attrs={
                "_tuning_mode": "raw",
                "log_loss": td["h_ll"],
                "duration_s": 5.0,
                "holdout_fold_metrics_calibrated": [
                    {"log_loss": f} for f in td["folds"]
                ],
                **raw,
                **cal,
            },
        )
        study.add_trial(trial)
    return study


@pytest.fixture
def legacy_study(tmp_path):
    """Pre-decoupling-refactor study — no `_tuning_mode` attr.

    Metrics on these trials were Platt-calibrated during tuning. tune-review
    refuses to display them (rather than silently ranking apples vs oranges).
    """
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
            user_attrs={
                "log_loss": ll,
                "holdout_log_loss": ll,
                "duration_s": 5.0,
            },
        )
        study.add_trial(trial)
    return study


class TestFormatLeaderboard:
    """Tests for leaderboard formatting."""

    def test_default_sorts_by_holdout_log_loss(self, populated_study):
        """Leaderboard sorts by holdout_log_loss by default (best holdout LL first)."""
        lines = format_leaderboard(populated_study, top_n=3)
        output = "\n".join(lines)
        # Best holdout_log_loss is 0.62 (trial C=1.0) — should appear in first row.
        # Header now spans 3 lines (title + raw-mode note + separator). First
        # trial row is at index 3.
        assert "LL=0.6200" in output.split("\n")[3]

    def test_bare_sort_auto_prefixes_to_holdout(self, populated_study):
        """`--sort log_loss` is auto-prefixed to `holdout_log_loss`.

        The user picks a metric NAME; in-fold vs holdout is an implementation
        detail. Ranking is always by the holdout measurement of that metric.
        With the fixture, best holdout_log_loss is C=1.0 (0.62), so that
        trial leads regardless of the in-fold ordering.
        """
        lines = format_leaderboard(populated_study, sort_by=["log_loss"], top_n=3)
        output = "\n".join(lines)
        assert "LL=0.6200" in output.split("\n")[3]

    def test_bare_sort_by_calibration_auto_prefixes(self, populated_study):
        """`--sort calibration_error` ranks by holdout_calibration_error."""
        lines = format_leaderboard(
            populated_study, sort_by=["calibration_error"], top_n=3
        )
        output = "\n".join(lines)
        # Best holdout cal is 0.012 = 1.20% (trial C=10.0) — should lead.
        assert "cal=1.20%" in output.split("\n")[3]

    def test_already_holdout_prefixed_sort_passes_through(self, populated_study):
        """`--sort holdout_brier_score` works literally (no double-prefix)."""
        lines = format_leaderboard(
            populated_study, sort_by=["holdout_brier_score"], top_n=3
        )
        output = "\n".join(lines)
        # Best holdout brier is 0.21 (trial C=1.0) — should lead.
        assert "brier=0.2100" in output.split("\n")[3]

    def test_top_n_limits_rows(self, populated_study):
        """Leaderboard respects top_n limit."""
        lines = format_leaderboard(populated_study, top_n=2)
        trial_lines = [l for l in lines if l.strip().startswith(("1.", "2.", "3."))]
        assert len(trial_lines) == 2

    def test_shows_all_holdout_metrics(self, populated_study):
        """Each row surfaces every standard classification metric (holdout)."""
        lines = format_leaderboard(populated_study, top_n=1)
        output = "\n".join(lines)
        # All 7 metrics should appear with their bare display labels.
        for label in ("LL=", "brier=", "AUC=", "acc=", "cal=", "scal=", "err80="):
            assert label in output

    def test_rows_show_optuna_number_and_seq(self, populated_study):
        """Each row leads with the crash-immune `seq` position and keeps the
        canonical Optuna id with the duration at the end. With three clean
        completed trials the two coincide (trial 0 -> seq 1, etc.)."""
        lines = format_leaderboard(populated_study, top_n=3)
        # Best holdout_log_loss is the 2nd-created trial (index 1) -> Optuna id 1,
        # seq 2. `seq` leads the row; `trial 1` sits with the duration.
        winner = " ".join(
            next(l for l in lines if l.strip().startswith("1.")).split()
        )
        assert winner.startswith("1. [seq 2] ")
        assert "trial 1)" in winner

    def test_seq_deinflates_across_incomplete_trials(self, tmp_path):
        """A failed/zombie trial consumes an Optuna number but not a batch slot,
        so `seq` skips it while `trial.number` does not. Insert a FAILED trial
        between two completed ones and assert the later completed trial reports
        Optuna id 2 but seq 2 (not 3)."""
        storage = f"sqlite:///{tmp_path / 'gap.db'}"
        study = optuna.create_study(
            study_name="gap_review", storage=storage, direction="minimize"
        )

        def _complete(c, ll):
            return optuna.trial.create_trial(
                params={"C": c},
                distributions={
                    "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                },
                values=[ll],
                user_attrs={
                    "_tuning_mode": "raw",
                    "log_loss": ll,
                    "holdout_log_loss": ll,
                    "duration_s": 5.0,
                },
            )

        # Optuna id 0: completed. id 1: failed (consumes a number, no batch slot).
        # id 2: completed -> should be seq 2.
        study.add_trial(_complete(0.1, 0.65))
        study.add_trial(
            optuna.trial.create_trial(
                params={"C": 1.0},
                distributions={
                    "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                },
                state=optuna.trial.TrialState.FAIL,
            )
        )
        study.add_trial(_complete(10.0, 0.62))

        lines = format_leaderboard(study, sort_by=["log_loss"], top_n=3)
        output = "\n".join(lines)
        # Winner is the last completed trial (holdout LL 0.62): Optuna id 2, seq 2.
        winner = " ".join(
            next(l for l in lines if l.strip().startswith("1.")).split()
        )
        assert winner.startswith("1. [seq 2] ")
        assert "trial 2)" in winner
        # Only two terminal trials exist, so no seq 3 is ever assigned.
        assert "[seq 3]" not in output

    def test_legacy_study_is_refused(self, legacy_study):
        """Pre-refactor studies (no `_tuning_mode`) are refused with clear guidance."""
        lines = format_leaderboard(legacy_study, top_n=3)
        output = "\n".join(lines)
        assert "before the calibration-decoupling refactor" in output
        assert "Delete the study DB" in output
        # Should NOT silently rank trials from the legacy study
        assert "0.6300" not in output

    def test_cal_study_sorts_by_calibrated_log_loss(self, populated_cal_study):
        """Default sort ranks by holdout_cal_log_loss, not raw. The calibrated
        winner (C=1.0, cal LL 0.60) differs from the raw winner (C=0.1, raw LL
        0.62), so the calibrated value must determine the order."""
        first_row = "\n".join(
            format_leaderboard(populated_cal_study, top_n=3)
        ).split("\n")[3]
        assert "LL=0.6000" in first_row  # calibrated LL, not raw 0.6200
        assert "trial 1)" in first_row   # C=1.0 was the 2nd-created trial

    def test_cal_header_and_raw_reference(self, populated_cal_study):
        """Header flags deployment-frame; each trial shows a raw LL reference."""
        output = "\n".join(format_leaderboard(populated_cal_study, top_n=3))
        assert "deployment-frame" in output
        assert "raw LL=0.6600" in output  # winner's raw LL on the reference line

    def test_cal_outer_block_spread(self, populated_cal_study):
        """The reference line shows the outer-block per-fold log_loss spread."""
        output = "\n".join(format_leaderboard(populated_cal_study, top_n=1))
        # Winner (C=1.0) calibrated per-fold LLs are [0.58, 0.62].
        assert "outer LL [0.5800..0.6200] over 2 folds" in output

    def test_cal_bare_sort_routes_to_calibrated(self, populated_cal_study):
        """`--sort log_loss` on a cal study ranks by holdout_cal_log_loss."""
        first_row = "\n".join(
            format_leaderboard(populated_cal_study, sort_by=["log_loss"], top_n=1)
        ).split("\n")[3]
        assert "LL=0.6000" in first_row

    def test_cal_sort_by_auc_stays_raw(self, populated_cal_study):
        """AUC is calibration-invariant, so `--sort roc_auc` routes to the raw
        holdout key (not holdout_cal_)."""
        lines = format_leaderboard(populated_cal_study, sort_by=["roc_auc"], top_n=1)
        assert "holdout_roc_auc" in lines[0]
        assert "holdout_cal_roc_auc" not in lines[0]

    def test_mixed_vintage_study_falls_back_to_raw(self, tmp_path):
        """A study where only some trials carry deployment-frame metrics shows the
        raw view with a note — it must never bury the cal-missing trials at ±inf
        (the raw-only trial here has the best raw LL and must still lead)."""
        storage = f"sqlite:///{tmp_path / 'mixed.db'}"
        study = optuna.create_study(
            study_name="mixed", storage=storage, direction="minimize"
        )

        def _trial(c, ll, with_cal):
            ua = {
                "_tuning_mode": "raw", "log_loss": ll, "duration_s": 5.0,
                "holdout_log_loss": ll, "holdout_roc_auc": 0.73,
                "holdout_brier_score": 0.22, "holdout_accuracy": 0.67,
                "holdout_calibration_error": 0.03,
                "holdout_calibration_error_max": 0.06,
                "holdout_overconfidence_max": 0.05,
                "holdout_signed_calibration": 0.01,
                "holdout_error_rate_80plus": 0.12,
            }
            if with_cal:
                ua["holdout_cal_log_loss"] = ll - 0.02
            return optuna.trial.create_trial(
                params={"C": c},
                distributions={
                    "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                },
                values=[ll],
                user_attrs=ua,
            )

        study.add_trial(_trial(0.1, 0.66, with_cal=True))
        study.add_trial(_trial(1.0, 0.62, with_cal=False))  # raw-only, best raw LL
        study.add_trial(_trial(10.0, 0.64, with_cal=True))
        output = "\n".join(format_leaderboard(study, top_n=3))
        # Mixed → raw view (not deployment-frame) with an explanatory note.
        assert "deployment-frame" in output  # only in the mixed-note text
        assert "2/3 trials carry deployment-frame metrics" in output
        assert "Probability metrics are deployment-frame" not in output  # not the cal header
        # The raw-only trial (best raw LL 0.62) is ranked and shown, not buried.
        assert "LL=0.6200" in output
        assert "nan" not in output


class TestToRanked:
    """Metric-name → ranked user-attr key routing (calibrated vs raw)."""

    def test_prob_scale_routes_to_calibrated_when_use_cal(self):
        assert _to_ranked("log_loss", True) == "holdout_cal_log_loss"
        # weighted_concordance is NOT exactly Platt-invariant → calibrated key.
        assert (
            _to_ranked("weighted_concordance", True)
            == "holdout_cal_weighted_concordance"
        )

    def test_invariant_metrics_stay_raw_even_when_use_cal(self):
        assert _to_ranked("roc_auc", True) == "holdout_roc_auc"
        assert _to_ranked("partial_auc_tail", True) == "holdout_partial_auc_tail"

    def test_no_cal_routes_to_raw(self):
        assert _to_ranked("log_loss", False) == "holdout_log_loss"

    def test_already_prefixed_passes_through(self):
        assert _to_ranked("holdout_log_loss", True) == "holdout_log_loss"
        assert _to_ranked("holdout_cal_log_loss", True) == "holdout_cal_log_loss"


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


@pytest.fixture
def nn_study(tmp_path):
    """Study with a single NN trial carrying encoded search-space params."""
    storage = f"sqlite:///{tmp_path / 'nn.db'}"
    study = optuna.create_study(
        study_name="nn_review", storage=storage, direction="minimize"
    )
    trial = optuna.trial.create_trial(
        params={
            "hidden_layers": "256-128",
            "normalization": "layer",
            "grad_clip_norm": None,
            "lr_scheduler": None,
            "dropout": 0.19,
        },
        distributions={
            "hidden_layers": optuna.distributions.CategoricalDistribution(
                ["256-128", "64-32"]
            ),
            "normalization": optuna.distributions.CategoricalDistribution(
                ["none", "batch", "layer"]
            ),
            "grad_clip_norm": optuna.distributions.CategoricalDistribution(
                [None, 1.0, 5.0]
            ),
            "lr_scheduler": optuna.distributions.CategoricalDistribution(
                [None, "plateau"]
            ),
            "dropout": optuna.distributions.FloatDistribution(0.1, 0.5),
        },
        values=[0.62],
        user_attrs={
            "_tuning_mode": "raw",
            "log_loss": 0.62,
            "holdout_log_loss": 0.62,
            "duration_s": 5.0,
        },
    )
    study.add_trial(trial)
    return study


class TestFormatBestTrial:
    """Best-trial output must be decoded and YAML-paste-safe."""

    def test_decodes_and_renders_yaml_safe_params(self, nn_study):
        lines = format_best_trial(nn_study)
        text = "\n".join(lines)
        # hidden_layers as a list, not the "256-128" string
        assert "hidden_layers: [256, 128]" in text
        # normalization expanded to the two booleans the model reads
        assert "batch_norm: false" in text
        assert "layer_norm: true" in text
        assert "normalization:" not in text
        # None rendered as YAML null, not the string "None"
        assert "grad_clip_norm: null" in text
        assert "lr_scheduler: null" in text

    def test_leaderboard_params_are_decoded(self, nn_study):
        """The per-trial param block in the leaderboard is paste-safe too."""
        text = "\n".join(format_leaderboard(nn_study))
        assert "hidden_layers: [256, 128]" in text
        assert "batch_norm: false" in text
        assert "layer_norm: true" in text
        assert "normalization:" not in text
        assert "grad_clip_norm: null" in text
        assert "lr_scheduler: null" in text
