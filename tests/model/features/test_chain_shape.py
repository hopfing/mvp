"""chain_shape: the projection's distribution moments as winner-side features.

Covers the scalar math (against hand-computed pmfs), the mirror semantics
(symmetric carried, antisymmetric negated), the splice honesty (per-fold train
ends, leak refusal), and the refusals (model-kind stems, artifacts predating
the shape columns)."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest

from mvp.common.chain_shape import (
    SHAPE_ANTISYMMETRIC,
    SHAPE_COLUMNS,
    SHAPE_SYMMETRIC,
    shape_scalars,
)
from mvp.model.features import prior


def _fake_out() -> SimpleNamespace:
    """Two matches with tiny hand-checkable distributions."""
    # total games pmf over support {0,1,2}: match 0 -> all mass at 2;
    # match 1 -> half at 0, half at 2 (mean 1, var 1).
    total = np.array([[0.0, 0.0, 1.0], [0.5, 0.0, 0.5]])
    # spread pmf with offset 1, support {-1, 0, +1}: match 0 -> all at +1;
    # match 1 -> half at -1, half at +1 (mean 0, var 1).
    spread = np.array([[0.0, 0.0, 1.0], [0.5, 0.0, 0.5]])
    dist = SimpleNamespace(
        p_match_win_a=np.array([0.8, 0.5]),
        set_outcome_probs={
            (2, 0): np.array([0.5, 0.1]),
            (0, 2): np.array([0.1, 0.1]),
            (2, 1): np.array([0.3, 0.4]),
            (1, 2): np.array([0.1, 0.4]),
            # no bo5 keys: a bo3-only batch, the defensive-lookup case
        },
        total_games_pmf=total,
        spread_pmf=spread,
        spread_offset=1,
        expected_total_games=np.array([2.0, 1.0]),
        expected_spread=np.array([1.0, 0.0]),
    )
    return SimpleNamespace(
        distribution=dist,
        h_a=np.array([0.9, 0.7]),
        h_b=np.array([0.6, 0.7]),
        t_ab=np.array([0.7, 0.5]),
        p_a_serve_win=np.array([0.68, 0.61]),
        p_b_serve_win=np.array([0.60, 0.61]),
    )


class TestShapeScalars:
    def test_hand_computed_values(self):
        s = shape_scalars(_fake_out())
        assert set(s) == set(SHAPE_COLUMNS)
        np.testing.assert_allclose(s["chain_egames"], [2.0, 1.0])
        # match 0: degenerate pmf -> std 0; match 1: values {0,2} evenly -> std 1
        np.testing.assert_allclose(s["chain_gstd"], [0.0, 1.0])
        np.testing.assert_allclose(s["chain_spread_std"], [0.0, 1.0])
        np.testing.assert_allclose(s["chain_hold_sum"], [1.5, 1.4])
        np.testing.assert_allclose(s["chain_hold_asym"], [0.3, 0.0])
        np.testing.assert_allclose(s["chain_p_straight"], [0.6, 0.2])
        np.testing.assert_allclose(s["chain_p_decider"], [0.4, 0.8])
        # bo3-only batch: no (3,1)/(1,3) keys -> zeros, not KeyError
        np.testing.assert_allclose(s["chain_p_4set"], [0.0, 0.0])
        np.testing.assert_allclose(s["chain_serve_level"], [1.28, 1.22])
        np.testing.assert_allclose(s["chain_serve_gap"], [0.08, 0.0])
        np.testing.assert_allclose(s["chain_tb_edge"], [0.2, 0.0])
        np.testing.assert_allclose(s["chain_espread"], [1.0, 0.0])

    def test_rows_align_with_the_output(self):
        s = shape_scalars(_fake_out())
        assert all(len(v) == 2 for v in s.values())


def _shape_fold_frame(days: list[date], folds: list[int]) -> pl.DataFrame:
    n = len(days)
    base = pl.DataFrame({
        "match_uid": [f"M{i}" for i in range(n)],
        "player_id": [f"A{i}" for i in range(n)],
        "opp_id": [f"B{i}" for i in range(n)],
        "effective_match_date": days,
        "fold_idx": pl.Series(folds, dtype=pl.Int32),
        "p_match_win_a": [0.6] * n,
        "won_a": pl.Series([i % 2 for i in range(n)], dtype=pl.Int8),
    })
    for j, c in enumerate(SHAPE_COLUMNS):
        base = base.with_columns(
            pl.Series(c, [float(j + 1) + 0.1 * i for i in range(n)])
        )
    return base


def _source(tmp_path: Path) -> prior.PriorSource:
    return prior.PriorSource(
        model="proj_base", config_path=tmp_path / "proj_base.yaml",
        fp="abc123abc123", eval_dir=tmp_path / "pe" / "abc123abc123",
        kind="projection", forward_train_end=date(2025, 12, 31),
    )


_DAYS = [date(2025, 1, 5), date(2025, 1, 9), date(2025, 7, 2), date(2025, 7, 8)]
_FOLDS = [1, 1, 2, 2]


class TestShapeFrame:
    def test_mirror_semantics(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _shape_fold_frame(_DAYS, _FOLDS).write_parquet(src.fold_match_win)
        rows = prior._shape_fold_rows(src)
        a = rows.filter(pl.col("player_id") == "A0")
        b = rows.filter(pl.col("player_id") == "B0")
        assert len(a) == 1 and len(b) == 1
        for c in SHAPE_SYMMETRIC:
            assert a[c][0] == b[c][0]
        for c in SHAPE_ANTISYMMETRIC:
            assert a[c][0] == -b[c][0]

    def test_fold_train_ends_precede_fold_days(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _shape_fold_frame(_DAYS, _FOLDS).write_parquet(src.fold_match_win)
        rows = prior._shape_fold_rows(src)
        assert (rows["day"] > rows["shape_train_end"]).all()
        # fold 2's train end = its own min day - 1, not fold 1's
        f2 = rows.filter(pl.col("match_uid") == "M2")
        assert f2["shape_train_end"][0] == date(2025, 7, 1)

    def test_missing_shape_columns_refused_with_command(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        legacy = _shape_fold_frame(_DAYS, _FOLDS).drop(SHAPE_COLUMNS)
        legacy.write_parquet(src.fold_match_win)
        with pytest.raises(ValueError, match="iid-project"):
            prior._shape_fold_rows(src)

    def test_model_kind_stem_refused(self, tmp_path, monkeypatch):
        prior._cached_shape_frame.cache_clear()
        model_src = prior.PriorSource(
            model="some_model", config_path=tmp_path / "some_model.yaml",
            fp="def456def456", eval_dir=tmp_path / "me" / "def456def456",
            kind="model", forward_train_end=date(2025, 12, 31),
        )
        monkeypatch.setattr(prior, "resolve_prior", lambda m: model_src)
        with pytest.raises(ValueError, match="projection stems"):
            prior._cached_shape_frame("some_model")
        prior._cached_shape_frame.cache_clear()

    def test_forward_rows_join_and_leak_refusal(self, tmp_path, monkeypatch):
        prior._cached_shape_frame.cache_clear()
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _shape_fold_frame(_DAYS, _FOLDS).write_parquet(src.fold_match_win)
        fwd = _shape_fold_frame([date(2026, 2, 1)], [9]).with_columns(
            pl.Series("match_uid", ["F0"])
        )
        fwd.write_parquet(src.pmf_parquet)
        monkeypatch.setattr(prior, "resolve_prior", lambda m: src)
        monkeypatch.setattr(prior, "ensure_prior_artifacts", lambda s, regenerate: None)
        frame = prior._cached_shape_frame("proj_base")
        # 4 fold matches + 1 forward match, both orientations
        assert len(frame) == 10
        f0 = frame.filter(pl.col("match_uid") == "F0")
        assert (f0["shape_train_end"] == date(2025, 12, 31)).all()
        prior._cached_shape_frame.cache_clear()

        # a forward row dated inside the training window must refuse
        fwd_bad = _shape_fold_frame([date(2025, 6, 1)], [9]).with_columns(
            pl.Series("match_uid", ["F1"])
        )
        fwd_bad.write_parquet(src.pmf_parquet)
        with pytest.raises(ValueError, match="on/before their"):
            prior._cached_shape_frame("proj_base")
        prior._cached_shape_frame.cache_clear()
