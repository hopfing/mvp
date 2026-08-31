"""Tests for tuning review output."""

import optuna
import pytest

from mvp.model.tuning import _OBJECTIVE_FRAME, _OBJECTIVE_FRAME_CAL
from mvp.model.tune_review import (
    _is_new_pipeline_study,
    _objective_key,
    _robust_pick,
    _to_ranked,
    format_best_trial,
    format_leaderboard,
    format_param_importance,
    resolve_sort_keys,
    sort_trials,
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
    # rll: a third ordering (C=10 best) so a restricted_logloss sort is
    # distinguishable from both log-loss orderings.
    trial_data = [
        {"C": 0.1, "h_ll": 0.62, "hc_ll": 0.64, "folds": [0.63, 0.65], "rll": 0.52},
        {"C": 1.0, "h_ll": 0.66, "hc_ll": 0.60, "folds": [0.58, 0.62], "rll": 0.54},
        {"C": 10.0, "h_ll": 0.65, "hc_ll": 0.63, "folds": [0.61, 0.65], "rll": 0.50},
    ]
    for td in trial_data:
        raw = {
            "holdout_restricted_logloss": td["rll"] + 0.01,
            "holdout_log_loss": td["h_ll"], "holdout_brier_score": 0.22,
            "holdout_roc_auc": 0.73, "holdout_accuracy": 0.67,
            "holdout_calibration_error": 0.03,
            "holdout_calibration_error_max": 0.06,
            "holdout_overconfidence_max": 0.05,
            "holdout_signed_calibration": 0.01,
            "holdout_error_rate_80plus": 0.12,
        }
        cal = {
            "holdout_cal_restricted_logloss": td["rll"],
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


def _first_row(lines: list[str]) -> str:
    """The top-ranked trial's row. Found by its rank prefix, not a line index —
    the header is a variable number of one-line bullets."""
    return next(l for l in lines if l.strip().startswith("1."))


class TestFormatLeaderboard:
    """Tests for leaderboard formatting."""

    def test_default_sorts_by_holdout_log_loss(self, populated_study):
        """Leaderboard sorts by holdout_log_loss by default (best holdout LL first)."""
        lines = format_leaderboard(populated_study, top_n=3)
        # Best holdout_log_loss is 0.62 (trial C=1.0) — should lead.
        assert "LL=0.6200" in _first_row(lines)

    def test_bare_sort_auto_prefixes_to_holdout(self, populated_study):
        """`--sort log_loss` is auto-prefixed to `holdout_log_loss`.

        The user picks a metric NAME; in-fold vs holdout is an implementation
        detail. Ranking is always by the holdout measurement of that metric.
        With the fixture, best holdout_log_loss is C=1.0 (0.62), so that
        trial leads regardless of the in-fold ordering.
        """
        lines = format_leaderboard(populated_study, sort_by=["log_loss"], top_n=3)
        assert "LL=0.6200" in _first_row(lines)

    def test_bare_sort_by_calibration_auto_prefixes(self, populated_study):
        """`--sort calibration_error` ranks by holdout_calibration_error."""
        lines = format_leaderboard(
            populated_study, sort_by=["calibration_error"], top_n=3
        )
        # Best holdout cal is 0.012 = 1.20% (trial C=10.0) — should lead.
        assert "cal=1.20%" in _first_row(lines)

    def test_already_holdout_prefixed_sort_passes_through(self, populated_study):
        """`--sort holdout_brier_score` works literally (no double-prefix)."""
        lines = format_leaderboard(
            populated_study, sort_by=["holdout_brier_score"], top_n=3
        )
        # Best holdout brier is 0.21 (trial C=1.0) — should lead.
        assert "brier=0.2100" in _first_row(lines)

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
        first_row = _first_row(format_leaderboard(populated_cal_study, top_n=3))
        assert "LL=0.6000" in first_row  # calibrated LL, not raw 0.6200
        assert "trial 1)" in first_row   # C=1.0 was the 2nd-created trial

    def test_ranked_metric_outside_the_columns_leads_the_row(
        self, populated_cal_study
    ):
        """Sorting by a metric that has no fixed column (restricted_logloss)
        prints it first on the row, so the sort key is visible where the eye
        lands rather than only as a raw value + delta on the second line."""
        lines = format_leaderboard(
            populated_cal_study, top_n=3, sort_by=["restricted_logloss"]
        )
        first_row = _first_row(lines)
        assert "restricted_logloss=0.50000  LL=" in first_row
        assert "C: 10.0" in "\n".join(lines[lines.index(first_row):])
        # a headline metric as the sort key adds no lead
        assert "log_loss=" not in _first_row(
            format_leaderboard(populated_cal_study, top_n=3, sort_by=["log_loss"])
        )

    def test_raw_reference_on_each_row(self, populated_cal_study):
        """Each trial shows its uncalibrated LL next to the ranked one. The frame
        needs no header note — the sort line names the key and the field labels
        itself."""
        output = "\n".join(format_leaderboard(populated_cal_study, top_n=3))
        assert "raw LL=0.6600" in output  # winner's raw LL on the reference line
        assert output.split("\n")[1].startswith("=")  # no bullets, straight to rows

    def test_cal_outer_block_spread(self, populated_cal_study):
        """The reference line shows the outer-block per-fold log_loss spread."""
        output = "\n".join(format_leaderboard(populated_cal_study, top_n=1))
        # Winner (C=1.0) calibrated per-fold LLs are [0.58, 0.62].
        assert "outer LL [0.5800..0.6200] over 2 folds" in output

    def test_cal_bare_sort_routes_to_calibrated(self, populated_cal_study):
        """`--sort log_loss` on a cal study ranks by holdout_cal_log_loss."""
        first_row = _first_row(
            format_leaderboard(populated_cal_study, sort_by=["log_loss"], top_n=1)
        )
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
        # Mixed → raw view, flagged: an inconsistent study is worth a bullet.
        assert "only 2/3 trials have deployment-frame metrics" in output
        assert "holdout_log_loss" in output  # ranked raw, not holdout_cal_
        # The raw-only trial (best raw LL 0.62) is ranked and shown, not buried.
        assert "LL=0.6200" in output
        assert "nan" not in output


class TestNewPipelineGate:
    """The legacy-refusal gate accepts both raw- and calibrated-frame studies."""

    @staticmethod
    def _trial(mode):
        ua = {"duration_s": 5.0}
        if mode is not None:
            ua["_tuning_mode"] = mode
        return optuna.trial.create_trial(
            params={}, distributions={}, values=[0.6], user_attrs=ua
        )

    def test_raw_frame_accepted(self):
        assert _is_new_pipeline_study([self._trial("raw")]) is True

    def test_calibrated_frame_accepted(self):
        # A calibrated-frame study is new-pipeline, not legacy — must not be refused.
        assert _is_new_pipeline_study([self._trial("calibrated")]) is True

    def test_legacy_missing_attr_refused(self):
        assert _is_new_pipeline_study([self._trial(None)]) is False


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


class TestRobustPick:
    """1-SE robust pick: stability over a within-noise argmax."""

    @staticmethod
    def _cal_trial(number_seed, fold_lls):
        return optuna.trial.create_trial(
            params={"C": number_seed},
            distributions={
                "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
            },
            values=[fold_lls[0]],
            user_attrs={
                "holdout_fold_metrics_calibrated": [
                    {"log_loss": v} for v in fold_lls
                ]
            },
        )

    def test_picks_lowest_variance_within_1se(self, tmp_path):
        """Trial 0 has the best mean but high fold-to-fold variance; trial 1 is
        slightly worse on mean but zero-variance and inside 1 SE of the best — the
        stable one wins. (Trials are added to a study so they carry real numbers.)"""
        storage = f"sqlite:///{tmp_path / 'rp1.db'}"
        study = optuna.create_study(
            study_name="rp1", storage=storage, direction="minimize"
        )
        study.add_trial(self._cal_trial(0.1, [0.55, 0.64, 0.55, 0.63]))  # 0: hi var
        study.add_trial(self._cal_trial(1.0, [0.60, 0.60, 0.60, 0.60]))  # 1: stable
        number, band, note = _robust_pick(study.trials, "holdout_cal_log_loss")
        assert number == 1
        assert band == 2
        assert note == ""

    def test_none_without_per_fold_metrics(self):
        t = optuna.trial.create_trial(
            params={}, distributions={}, values=[0.6], user_attrs={}
        )
        assert _robust_pick([t], "holdout_cal_log_loss") == (None, 0, "")

    def test_maximize_metric_direction(self, tmp_path):
        """roc_auc is maximize: the stable trial within 1 SE *below* the best mean
        is the robust pick (direction handled correctly)."""
        storage = f"sqlite:///{tmp_path / 'rp2.db'}"
        study = optuna.create_study(
            study_name="rp2", storage=storage, direction="maximize"
        )

        def _t(seed, folds):
            return optuna.trial.create_trial(
                params={"C": seed},
                distributions={
                    "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                },
                values=[folds[0]],
                user_attrs={"holdout_fold_metrics": [{"roc_auc": v} for v in folds]},
            )

        study.add_trial(_t(0.1, [0.70, 0.80, 0.70, 0.80]))  # 0: mean .75, high var
        study.add_trial(_t(1.0, [0.74, 0.74, 0.74, 0.74]))  # 1: mean .74, var 0
        number, band, note = _robust_pick(study.trials, "holdout_roc_auc")
        assert number == 1
        assert band == 2
        assert note == ""

    def test_robust_pick_annotates_leaderboard(self, tmp_path):
        """format_leaderboard flags the 1-SE robust pick when it differs from the
        argmax and there's a genuine within-1-SE band."""
        storage = f"sqlite:///{tmp_path / 'robust.db'}"
        study = optuna.create_study(
            study_name="robust", storage=storage, direction="minimize"
        )

        def _t(seed, pooled, folds):
            return optuna.trial.create_trial(
                params={"C": seed},
                distributions={
                    "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                },
                values=[pooled],
                user_attrs={
                    "_tuning_mode": "calibrated",
                    "log_loss": pooled,
                    "duration_s": 5.0,
                    "holdout_cal_log_loss": pooled,
                    "holdout_cal_roc_auc": 0.73,
                    "holdout_fold_metrics_calibrated": [
                        {"log_loss": v} for v in folds
                    ],
                },
            )

        study.add_trial(_t(0.1, 0.592, [0.55, 0.64, 0.55, 0.63]))  # argmax, high var
        study.add_trial(_t(1.0, 0.600, [0.60, 0.60, 0.60, 0.60]))  # stable, in band
        output = "\n".join(format_leaderboard(study, top_n=2))
        assert "1-SE pick: trial 1" in output
        assert "◆ 1-SE robust pick" in output

    def test_suppressed_below_three_outer_folds(self, tmp_path):
        """At K=2 the variance tie-break is just "which two numbers sit closest",
        so the pick is suppressed with a reason rather than recommended."""
        storage = f"sqlite:///{tmp_path / 'rp_k2.db'}"
        study = optuna.create_study(
            study_name="rp_k2", storage=storage, direction="minimize"
        )
        study.add_trial(self._cal_trial(0.1, [0.55, 0.65]))  # best mean, wide
        study.add_trial(self._cal_trial(1.0, [0.605, 0.607]))  # in band, narrow
        number, band, note = _robust_pick(
            study.trials, "holdout_cal_log_loss", outer_folds=2
        )
        assert number is None
        assert band == 0
        assert "1-SE pick off: 2 outer folds" in note

    def test_fold_count_falls_back_to_per_fold_attrs(self, tmp_path):
        """Studies predating the `outer_folds` attr still get the K guard, from
        what the per-fold metrics actually carry."""
        storage = f"sqlite:///{tmp_path / 'rp_k2b.db'}"
        study = optuna.create_study(
            study_name="rp_k2b", storage=storage, direction="minimize"
        )
        study.add_trial(self._cal_trial(0.1, [0.55, 0.65]))
        study.add_trial(self._cal_trial(1.0, [0.605, 0.607]))
        number, _, note = _robust_pick(study.trials, "holdout_cal_log_loss")
        assert number is None
        assert "1-SE pick off: 2 outer folds" in note

    def test_suppressed_when_band_swallows_the_leaderboard(self, tmp_path):
        """Enough outer folds, but per-fold noise dwarfs the between-trial spread,
        so nearly every trial lands within 1 SE — naming one implies a distinction
        the data doesn't carry. Fold count alone wouldn't catch this."""
        storage = f"sqlite:///{tmp_path / 'rp_band.db'}"
        study = optuna.create_study(
            study_name="rp_band", storage=storage, direction="minimize"
        )
        # 25 trials, means 0.6000..0.6024 apart, each with a ~0.03 fold spread.
        for i in range(25):
            base = 0.60 + i * 0.0001
            study.add_trial(
                self._cal_trial(0.1 + i, [base - 0.015, base + 0.015, base, base])
            )
        number, band, note = _robust_pick(
            study.trials, "holdout_cal_log_loss", outer_folds=4
        )
        assert number is None
        assert band == 0
        assert "trials within 1 SE" in note
        assert "doesn't separate trials" in note

    def test_band_fraction_guard_needs_enough_trials(self, tmp_path):
        """The band-fraction symptom only means something with enough trials —
        a 2-trial study keeps the pick."""
        storage = f"sqlite:///{tmp_path / 'rp_small.db'}"
        study = optuna.create_study(
            study_name="rp_small", storage=storage, direction="minimize"
        )
        study.add_trial(self._cal_trial(0.1, [0.55, 0.64, 0.55, 0.63]))
        study.add_trial(self._cal_trial(1.0, [0.60, 0.60, 0.60, 0.60]))
        number, band, note = _robust_pick(
            study.trials, "holdout_cal_log_loss", outer_folds=4
        )
        assert number == 1  # band is 2/2 but the study is too small to judge
        assert band == 2
        assert note == ""


class TestDefaultSort:
    """With no `--sort`, rank on the study's own objective — not a hardcoded
    metric the study may never have optimized."""

    @staticmethod
    def _study(tmp_path, name, objective, rows):
        """rows: (C, holdout_cal_log_loss, holdout_cal_brier_score)."""
        storage = f"sqlite:///{tmp_path / f'{name}.db'}"
        study = optuna.create_study(
            study_name=name, storage=storage, direction="minimize"
        )
        if objective:
            study.set_user_attr("objective_metrics", objective)
        for c, ll, brier in rows:
            study.add_trial(
                optuna.trial.create_trial(
                    params={"C": c},
                    distributions={
                        "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                    },
                    values=[ll],
                    user_attrs={
                        "_tuning_mode": "calibrated",
                        "log_loss": ll, "duration_s": 5.0,
                        "holdout_cal_log_loss": ll,
                        "holdout_cal_brier_score": brier,
                        "holdout_log_loss": ll + 0.001,
                        "holdout_roc_auc": 0.73,
                    },
                )
            )
        return study

    # LL and brier disagree: C=0.1 wins LL, C=1.0 wins brier.
    _ROWS = [(0.1, 0.60, 0.23), (1.0, 0.62, 0.21)]

    def test_brier_objective_ranks_by_brier(self, tmp_path):
        study = self._study(tmp_path, "obj_brier", ["brier_score"], self._ROWS)
        assert resolve_sort_keys(study, study.trials) == ["holdout_cal_brier_score"]
        lines = format_leaderboard(study, top_n=2)
        assert "sorted by holdout_cal_brier_score" in lines[0]
        assert "brier=0.2100" in _first_row(lines)  # C=1.0 leads, not the LL winner

    def test_log_loss_objective_ranks_by_log_loss(self, tmp_path):
        study = self._study(tmp_path, "obj_ll", ["log_loss"], self._ROWS)
        assert resolve_sort_keys(study, study.trials) == ["holdout_cal_log_loss"]
        assert "LL=0.6000" in _first_row(format_leaderboard(study, top_n=2))

    def test_explicit_sort_overrides_the_objective(self, tmp_path):
        study = self._study(tmp_path, "obj_override", ["brier_score"], self._ROWS)
        keys = resolve_sort_keys(study, study.trials, ["log_loss"])
        assert keys == ["holdout_cal_log_loss"]

    def test_unstamped_study_falls_back(self, tmp_path):
        """No objective_metrics and an ambiguous value → the old log_loss default,
        so studies tuned before the stamp keep rendering."""
        study = self._study(tmp_path, "obj_none", None, self._ROWS)
        assert resolve_sort_keys(study, study.trials) == ["holdout_cal_log_loss"]

    def test_sweep_and_leaderboard_agree(self, tmp_path):
        """The invariant the sweep depends on: top-N off the shared helpers is the
        same set, in the same order, as the leaderboard's rows. Both `tune-review`
        and `--select topn` go through resolve_sort_keys + sort_trials."""
        rows = [(0.1, 0.60, 0.23), (1.0, 0.62, 0.21), (10.0, 0.61, 0.22)]
        study = self._study(tmp_path, "obj_agree", ["brier_score"], rows)
        keys = resolve_sort_keys(study, study.trials)
        picked = [t.number for t in sort_trials(study.trials, keys)[:2]]
        shown = [
            int(line.split("trial ")[1].rstrip(")"))
            for line in format_leaderboard(study, top_n=2)
            if line.strip().startswith(("1.", "2."))
        ]
        assert picked == shown


class TestObjectiveKey:
    """Naming the value Optuna optimized, so the tuner's `best=` reconciles with
    the leaderboard (which ranks a different fold set)."""

    @staticmethod
    def _study(tmp_path, name, attrs, trial_attrs, value):
        storage = f"sqlite:///{tmp_path / f'{name}.db'}"
        study = optuna.create_study(
            study_name=name, storage=storage, direction="minimize"
        )
        for k, v in attrs.items():
            study.set_user_attr(k, v)
        for i in range(2):
            study.add_trial(
                optuna.trial.create_trial(
                    params={"C": 0.1 * (i + 1)},
                    distributions={
                        "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                    },
                    values=[value + i * 0.01],
                    user_attrs={
                        k: (v + i * 0.01 if isinstance(v, float) else v)
                        for k, v in trial_attrs.items()
                    },
                )
            )
        return study

    def test_stamped_calibrated_study_resolves_to_cal_key(self, tmp_path):
        """A calibrated-frame study optimizes `cal_<metric>`, not the raw metric."""
        study = self._study(
            tmp_path, "stamped",
            {"objective_metrics": ["log_loss"], "objective_frame": _OBJECTIVE_FRAME_CAL},
            {"log_loss": 0.61, "cal_log_loss": 0.60},
            0.60,
        )
        assert _objective_key(study, study.trials) == "cal_log_loss"

    def test_stamped_raw_study_resolves_to_bare_key(self, tmp_path):
        study = self._study(
            tmp_path, "stamped_raw",
            {"objective_metrics": ["log_loss"], "objective_frame": _OBJECTIVE_FRAME},
            {"log_loss": 0.60, "cal_log_loss": 0.59},
            0.60,
        )
        assert _objective_key(study, study.trials) == "log_loss"

    def test_unstamped_study_recovers_by_value_match(self, tmp_path):
        """Studies tuned before the stamp existed still get the column: the
        objective attr is the one equal to `values[0]` on every trial."""
        study = self._study(
            tmp_path, "unstamped", {},
            {"log_loss": 0.61, "cal_log_loss": 0.60},
            0.60,
        )
        assert _objective_key(study, study.trials) == "cal_log_loss"

    def test_ambiguous_recovery_returns_none(self, tmp_path):
        """Two attrs tying on every trial is ambiguous — no column beats a guess."""
        study = self._study(
            tmp_path, "ambiguous", {},
            {"log_loss": 0.60, "holdout_log_loss": 0.60},
            0.60,
        )
        assert _objective_key(study, study.trials) is None

    def test_multi_objective_returns_none(self, tmp_path):
        storage = f"sqlite:///{tmp_path / 'multi.db'}"
        study = optuna.create_study(
            study_name="multi", storage=storage, directions=["minimize", "maximize"]
        )
        assert _objective_key(study, []) is None

    def test_leaderboard_never_shows_the_objective(self, tmp_path):
        """The search objective appears nowhere in the leaderboard — not as a
        header claim, not as a per-row field. Every trial was optimized against
        it, so it can't rank them, and showing it only invites the reader to
        mistake it for the sort key."""
        storage = f"sqlite:///{tmp_path / 'obj.db'}"
        study = optuna.create_study(
            study_name="obj", storage=storage, direction="minimize"
        )
        study.set_user_attr("objective_metrics", ["log_loss"])
        study.set_user_attr("objective_frame", _OBJECTIVE_FRAME_CAL)
        # Trial 0 wins the objective; trial 1 wins the held-out block.
        for cal_ll, hc_ll in ((0.5966, 0.5997), (0.5972, 0.5991)):
            study.add_trial(
                optuna.trial.create_trial(
                    params={"C": cal_ll},
                    distributions={
                        "C": optuna.distributions.FloatDistribution(0.01, 100.0, log=True)
                    },
                    values=[cal_ll],
                    user_attrs={
                        "_tuning_mode": "calibrated",
                        "cal_log_loss": cal_ll,
                        "log_loss": cal_ll + 0.001,
                        "holdout_log_loss": hc_ll + 0.001,
                        "holdout_cal_log_loss": hc_ll,
                        "holdout_roc_auc": 0.73,
                        "duration_s": 5.0,
                    },
                )
            )
        lines = format_leaderboard(study, top_n=2)
        output = "\n".join(lines)
        # Trial 0 won the objective; trial 1 wins the held-out block and leads.
        assert "trial 1)" in _first_row(lines)
        assert "cal_log_loss=" not in output  # no `obj cal_log_loss=` field
        assert "0.59660" not in output        # nor the objective's best value
        assert "tuner's best" not in output   # nor a header claim about it


class TestFormatParamImportance:
    """Tests for param importance formatting."""

    def test_returns_lines(self, populated_study):
        """format_param_importance returns non-empty output."""
        lines = format_param_importance(populated_study)
        assert len(lines) > 0

    def test_names_its_target(self, populated_study):
        """The block is computed against the tuning objective while the
        leaderboard above it ranks on `--sort` — say so."""
        output = "\n".join(format_param_importance(populated_study))
        assert "· target: log_loss (tuning objective, not the sort metric)" in output

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
