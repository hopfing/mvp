"""Paired family comparison (model/compare.py): pairing, orientation collapse,
block bootstrap, refusal semantics. Plan
mvp-docs/plans/2026-09-01-paired-family-comparison.md rev 3."""

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from mvp.model import compare


def _write_eval(
    root, fp, probs, *, days=None, y=None, uids=None, column="y_prob_cal",
    mirrored=True, circuit=None,
):
    """A minimal fold_predictions.parquet: mirrored rows by default, with
    complementary probabilities (the symmetry invariant)."""
    n = len(probs)
    if days is None:
        days = [date(2024, 1, 3) + timedelta(weeks=i % 20) for i in range(n)]
    if y is None:
        y = [i % 2 for i in range(n)]
    if uids is None:
        uids = [f"M{i}" for i in range(n)]
    rows = {
        "match_uid": list(uids), "player_id": ["A"] * n,
        "effective_match_date": list(days),
        "fold_idx": [1] * n, "y_test": list(y), column: list(probs),
    }
    if circuit is not None:
        rows["circuit"] = list(circuit)
    df = pl.DataFrame(rows)
    if mirrored:
        mirror = df.with_columns(
            pl.lit("B").alias("player_id"),
            (1 - pl.col(column)).alias(column),
            (1 - pl.col("y_test")).alias("y_test"),
        )
        df = pl.concat([df, mirror])
    d = root / fp
    d.mkdir(parents=True, exist_ok=True)
    df.write_parquet(d / "fold_predictions.parquet")
    return d


@pytest.fixture
def root(tmp_path, monkeypatch):
    r = tmp_path / "evals"
    r.mkdir()
    monkeypatch.setattr(compare, "EVALUATIONS_ROOT", r)
    return r


def _break_symmetry(d):
    p = pl.read_parquet(d / "fold_predictions.parquet")
    p = p.with_columns(
        pl.when(pl.col("player_id") == "B")
        .then(pl.col("y_prob_cal") + 0.05)
        .otherwise(pl.col("y_prob_cal"))
        .alias("y_prob_cal")
    )
    p.write_parquet(d / "fold_predictions.parquet")


class TestLoadAndCollapse:
    def test_collapses_mirrored_rows_to_match_grain(self, root):
        d = _write_eval(root, "a" * 12, [0.6, 0.7, 0.55, 0.8])
        df, grain = compare.load_eval_predictions(d, "y_prob_cal")
        assert grain == "match"
        assert df.height == 4  # matches, not 8 rows
        assert df["match_uid"].n_unique() == 4

    def test_asymmetric_frame_warns_and_keeps_rows(self, root, caplog):
        d = _write_eval(root, "a" * 12, [0.6, 0.7], mirrored=True)
        _break_symmetry(d)
        with caplog.at_level("WARNING"):
            df, grain = compare.load_eval_predictions(d, "y_prob_cal")
        assert grain == "row"
        assert df.height == 4  # both rows kept
        assert "orientation" in caplog.text

    def test_missing_column_is_an_error_not_a_fallback(self, root):
        d = _write_eval(root, "a" * 12, [0.6], column="y_prob")
        with pytest.raises(ValueError, match="y_prob_cal"):
            compare.load_eval_predictions(d, "y_prob_cal")


class TestPair:
    def test_identical_predictions_delta_zero_interval_zero(self, root):
        probs = list(np.linspace(0.3, 0.8, 40))
        da = _write_eval(root, "a" * 12, probs)
        db = _write_eval(root, "b" * 12, probs)
        fa, _ = compare.load_eval_predictions(da, "y_prob_cal")
        fb, _ = compare.load_eval_predictions(db, "y_prob_cal")
        r = compare.compare_pair(fa, fb, "a" * 12, "b" * 12, reps=200)
        assert r.delta_ll == 0.0
        assert r.ci == (0.0, 0.0)

    def test_known_delta_recovered_and_ci_covers(self, root):
        rng = np.random.default_rng(1)
        n = 400
        y = rng.integers(0, 2, n)
        # B is closer to the truth than A by a consistent margin
        pb = np.clip(0.5 + (y - 0.5) * 0.3 + rng.normal(0, 0.05, n), 0.05, 0.95)
        pa = np.clip(pb - (y - 0.5) * 0.1, 0.05, 0.95)
        days = [date(2024, 1, 2) + timedelta(weeks=i % 40) for i in range(n)]
        da = _write_eval(root, "a" * 12, list(pa), y=list(y), days=days)
        db = _write_eval(root, "b" * 12, list(pb), y=list(y), days=days)
        fa, _ = compare.load_eval_predictions(da, "y_prob_cal")
        fb, _ = compare.load_eval_predictions(db, "y_prob_cal")
        r = compare.compare_pair(fa, fb, "a" * 12, "b" * 12, reps=500)
        assert r.delta_ll > 0  # A worse (higher loss)
        assert r.ci[0] > 0  # separation resolves at this effect size
        assert r.n_matches == n

    def test_disjoint_populations_refused(self, root):
        da = _write_eval(root, "a" * 12, [0.6] * 10, uids=[f"M{i}" for i in range(10)])
        db = _write_eval(root, "b" * 12, [0.6] * 10, uids=[f"N{i}" for i in range(10)])
        fa, _ = compare.load_eval_predictions(da, "y_prob_cal")
        fb, _ = compare.load_eval_predictions(db, "y_prob_cal")
        with pytest.raises(ValueError, match="min_overlap"):
            compare.compare_pair(fa, fb, "a", "b")

    def test_fold_schemes_are_irrelevant_to_pairing(self, root):
        probs = list(np.linspace(0.3, 0.8, 12))
        da = _write_eval(root, "a" * 12, probs)
        db = _write_eval(root, "b" * 12, probs)
        p = pl.read_parquet(db / "fold_predictions.parquet")
        p.with_columns(pl.lit(7).alias("fold_idx")).write_parquet(
            db / "fold_predictions.parquet"
        )
        fa, _ = compare.load_eval_predictions(da, "y_prob_cal")
        fb, _ = compare.load_eval_predictions(db, "y_prob_cal")
        r = compare.compare_pair(fa, fb, "a", "b", reps=100)
        assert r.n_matches == 12 and r.delta_ll == 0.0

    def test_single_week_flags_low_blocks(self, root):
        days = [date(2024, 1, 2)] * 8
        da = _write_eval(root, "a" * 12, [0.6] * 8, days=days)
        db = _write_eval(root, "b" * 12, [0.7] * 8, days=days)
        fa, _ = compare.load_eval_predictions(da, "y_prob_cal")
        fb, _ = compare.load_eval_predictions(db, "y_prob_cal")
        r = compare.compare_pair(fa, fb, "a", "b", reps=100)
        assert r.n_blocks == 1
        assert r.n_blocks < compare.LOW_BLOCKS  # the CLI flags this


class TestFamilies:
    def test_matrix_envelope_and_family_cuts(self, root):
        base = list(np.linspace(0.3, 0.8, 30))
        dirs_a = [
            _write_eval(root, "a" * 11 + str(i), [p + 0.01 * i for p in base])
            for i in range(2)
        ]
        dirs_b = [_write_eval(root, "b" * 12, base)]
        rep = compare.compare_families(dirs_a, dirs_b, reps=100)
        assert rep["n_trials"] == (2, 1)
        assert len(rep["matrix"]) == 2
        lo, hi = rep["envelope"]
        assert lo <= rep["family_mean"] <= hi
        # segment cuts exist at FAMILY level, each with an interval
        assert rep["family_cuts"]
        for _lab, (dv, clo, chi, _nb) in rep["family_cuts"].items():
            assert clo <= dv <= chi

    def test_one_v_one_is_the_head_to_head_form(self, root):
        """A 1x1 matrix's family verdict IS the pair verdict — the
        head-to-head usage needs no separate machinery."""
        probs = list(np.linspace(0.3, 0.8, 30))
        da = _write_eval(root, "a" * 12, probs)
        db = _write_eval(root, "b" * 12, [p + 0.02 for p in probs])
        rep = compare.compare_families([da], [db], reps=100)
        pair = rep["matrix"][("a" * 12, "b" * 12)]
        assert rep["family_mean"] == pytest.approx(pair.delta_ll)
        assert rep["family_cuts"]  # cuts present in the 1v1 form too

    def test_mixed_grain_is_refused(self, root):
        da = _write_eval(root, "a" * 12, [0.6, 0.7])
        db = _write_eval(root, "b" * 12, [0.6, 0.7])
        _break_symmetry(db)
        fa, ga = compare.load_eval_predictions(da, "y_prob_cal")
        fb, gb = compare.load_eval_predictions(db, "y_prob_cal")
        with pytest.raises(ValueError, match="mixed grain"):
            compare.compare_pair(fa, fb, "a", "b", grain_a=ga, grain_b=gb)

    def test_one_refused_cell_does_not_abort_the_matrix(self, root):
        base = list(np.linspace(0.3, 0.8, 20))
        good = _write_eval(root, "a" * 12, base)
        other_pop = _write_eval(
            root, "e" * 12, base, uids=[f"X{i}" for i in range(20)]
        )
        db = _write_eval(root, "b" * 12, base)
        rep = compare.compare_families([good, other_pop], [db], reps=100)
        assert len(rep["matrix"]) == 1
        assert len(rep["refused"]) == 1
        assert ("e" * 12, "b" * 12) in rep["refused"]

    def test_tag_top_subset_by_tune_ranking(self, root, tmp_path, monkeypatch):
        """--top on a tag side: subset by the tune study's own ordering
        (holdout key), mapped via the sweep's __hNN_tNN tags — no
        fingerprint pasting."""
        import optuna

        optuna.logging.set_verbosity(optuna.logging.WARNING)
        monkeypatch.setattr(compare, "get_data_root", lambda: tmp_path)
        (tmp_path / "tuning").mkdir()
        study = optuna.create_study(
            study_name="fam",
            storage=f"sqlite:///{tmp_path / 'tuning' / 'fam.db'}",
            direction="minimize",
        )
        # trial numbers 0..2; holdout ranking: t1 best, t2, t0
        for hold in (0.62, 0.60, 0.61):
            study.add_trial(optuna.trial.create_trial(
                params={}, distributions={}, value=hold,
                user_attrs={
                    "holdout_cal_log_loss": hold, "_tuning_mode": "calibrated",
                },
            ))
        for i, fp in enumerate(("1" * 12, "2" * 12, "3" * 12)):
            d = _write_eval(root, fp, [0.6])
            (d / "source.txt").write_text(
                f"fam__h0{i + 1}_t{i}\trun{i}\t2026-01-01\n"
                f"fam\tfam__h0{i + 1}_t{i}\t2026-01-01\n"
            )
        assert [p.name for p in compare.resolve_side("fam", top=2)] == [
            "2" * 12, "3" * 12,  # t1 then t2 by holdout
        ]
        with pytest.raises(ValueError, match="fingerprint list IS the subset"):
            compare.resolve_side("1" * 12, top=2)
        with pytest.raises(ValueError, match="mixed side forms"):
            compare.resolve_sides("fam", "1" * 12)
        d = _write_eval(root, "4" * 12, [0.6])
        (d / "source.txt").write_text("famx_no_db\tx\t2026-01-01\n")
        with pytest.raises(FileNotFoundError, match="tune study"):
            compare.resolve_side("famx_no_db", top=2)

    def test_resolve_side_fingerprints_and_tag(self, root):
        d1 = _write_eval(root, "c" * 12, [0.6])
        (d1 / "source.txt").write_text("famx\trun1\t2026-01-01\n")
        _write_eval(root, "d" * 12, [0.6])
        assert [p.name for p in compare.resolve_side("c" * 12 + "," + "d" * 12)] == [
            "c" * 12, "d" * 12,
        ]
        assert [p.name for p in compare.resolve_side("famx")] == ["c" * 12]
        with pytest.raises(FileNotFoundError, match="matched no"):
            compare.resolve_side("nosuchtag")


class TestCLI:
    def _run(self, root, capsys, days_a, days_b, **kw):
        import argparse

        from mvp.cli import cmd_compare

        _write_eval(root, "a" * 12, [0.6] * len(days_a), days=days_a)
        _write_eval(root, "b" * 12, [0.7] * len(days_b), days=days_b)
        args = argparse.Namespace(
            a="a" * 12, b="b" * 12, top=None,
            column="y_prob_cal", min_overlap=0.5, reps=100, seed=0, **kw,
        )
        assert cmd_compare(args) == 0
        return capsys.readouterr().out

    def test_low_blocks_reaches_the_headline_line(self, root, capsys):
        """A thin-window comparison must flag the pooled interval, not only
        the segment cuts (the silent narrow CI is the exact failure this
        tool exists to prevent)."""
        days = [date(2024, 1, 2)] * 8
        out = self._run(root, capsys, days, days)
        pooled_line = next(ln for ln in out.splitlines() if ln.startswith("pooled"))
        assert "LOW-BLOCKS" in pooled_line
        assert "matches" in out

    def test_family_cuts_printed_with_intervals(self, root, capsys):
        days = [date(2024, 1, 2) + timedelta(weeks=i) for i in range(40)]
        out = self._run(root, capsys, days, days)
        assert "selection" not in out or "not corrected" in out
        # half-year cut lines carry brackets (an interval each)
        cut_lines = [ln for ln in out.splitlines() if ln.strip().startswith("H")]
        assert cut_lines and all("[" in ln for ln in cut_lines)
