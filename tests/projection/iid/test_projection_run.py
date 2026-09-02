"""Tests for `projection_run` — the repo's only pmf producer.

Three, per the cutover plan's next-steps list. All pure frame-in / frame-out;
none of them train a model or touch the feature engine.

Why these three specifically: `projection_run.py` holds the only pmf producer
and the only projector-training path and had no tests, while `backtest.py`
still writes the same `total_games_pmf.parquet`. Until the cutover deletes it,
which module ran determines whether a given pmf was alignment-checked — so the
alignment assert is the one guard standing between a reordered frame and every
downstream bet being priced against another match's distribution.
"""

from datetime import date

import joblib
import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.chain import (
    match_distribution,
    p_service_game_win,
    p_tiebreak_game_win,
    set_score_distribution,
)
from mvp.projection.iid.projection_run import (
    build_pmf_frame,
    build_spread_pmf_frame,
    resolve_targets,
)
from mvp.projection.iid.projector import ProjectionOutput


def _output_for(match_uids: list[str], serve_prob: float = 0.64) -> ProjectionOutput:
    n = len(match_uids)
    p_a = np.full(n, serve_prob)
    p_b = np.full(n, serve_prob - 0.03)
    h_a = p_service_game_win(p_a)
    h_b = p_service_game_win(p_b)
    t_ab = p_tiebreak_game_win(p_a, p_b)
    best_of = np.full(n, 3, dtype=np.int64)
    return ProjectionOutput(
        distribution=match_distribution(h_a, h_b, t_ab, best_of),
        match_uid=np.array(match_uids),
        best_of=best_of,
        p_a_serve_win=p_a,
        p_b_serve_win=p_b,
        h_a=h_a,
        h_b=h_b,
        t_ab=t_ab,
        set_score_pmf=set_score_distribution(h_a, h_b, t_ab),
    )


def _test_df(match_uids: list[str]) -> pl.DataFrame:
    n = len(match_uids)
    return pl.DataFrame(
        {
            "match_uid": match_uids,
            "player_id": [f"AA{i:02d}" for i in range(n)],
            "opp_id": [f"ZZ{i:02d}" for i in range(n)],
            "effective_match_date": [date(2026, 1, 2 + i) for i in range(n)],
            "circuit": ["tour"] * n,
            "surface": ["Hard"] * n,
            "round": ["R32"] * n,
            "best_of": [3] * n,
            "_target_games_a": [12.0 + i for i in range(n)],
            "_target_games_b": [10.0] * n,
        }
    )


class TestBuildPmfFrame:
    """(a) the alignment assert — the guard the cutover plan calls load-bearing."""

    def test_aligned_pair_carries_each_match_its_own_pmf(self):
        uids = ["m0", "m1", "m2"]
        out = _output_for(uids)
        pmf = build_pmf_frame(_test_df(uids), out)

        assert pmf["match_uid"].to_list() == uids
        for i in range(len(uids)):
            assert (
                pmf["total_games_pmf"][i].to_list()
                == out.distribution.total_games_pmf[i].tolist()
            )

    def test_permuted_output_raises_rather_than_writing_a_silent_mismatch(self):
        uids = ["m0", "m1", "m2"]
        out = _output_for(uids)
        out.match_uid = np.array(["m2", "m0", "m1"])
        with pytest.raises(ValueError, match="not row-aligned"):
            build_pmf_frame(_test_df(uids), out)

    def test_equal_length_but_different_matches_raises(self):
        """Same shape, disjoint identities — the case where nothing else would
        notice: the frame and the distribution agree on length and on nothing
        else."""
        out = _output_for(["x0", "x1", "x2"])
        with pytest.raises(ValueError, match="not row-aligned"):
            build_pmf_frame(_test_df(["m0", "m1", "m2"]), out)

    def test_actual_total_is_the_settlement_source(self):
        uids = ["m0", "m1"]
        pmf = build_pmf_frame(_test_df(uids), _output_for(uids))
        # _target_games_a + _target_games_b, per row.
        assert pmf["actual_total"].to_list() == [22.0, 23.0]


class TestResolveTargets:
    """(c) the RET / W-O / DEF / UNP exclusion — settlement source of truth."""

    def _frame(self, reasons: list[str | None], set1=None, set2=None) -> pl.DataFrame:
        n = len(reasons)
        data: dict = {
            "match_uid": [f"m{i}" for i in range(n)],
            "reason": reasons,
        }
        for i in range(1, 6):
            data[f"player_set{i}_games"] = [6.0 if i <= 2 else None] * n
            data[f"opp_set{i}_games"] = [4.0 if i <= 2 else None] * n
        if set1 is not None:
            data["player_set1_games"] = set1
        if set2 is not None:
            data["player_set2_games"] = set2
        return pl.DataFrame(data)

    @pytest.mark.parametrize("reason", ["W/O", "RET", "DEF", "UNP"])
    def test_unfinished_matches_are_dropped(self, reason):
        """A retirement produces fewer games than the match would have, so
        scoring it against a total records an under no book would settle."""
        df = resolve_targets(self._frame([reason, None]))
        assert df["match_uid"].to_list() == ["m1"]

    def test_completed_match_survives_with_targets(self):
        df = resolve_targets(self._frame([None]))
        assert df.height == 1
        assert df["_target_games_a"][0] == 12.0
        assert df["_target_games_b"][0] == 8.0

    def test_missing_first_two_sets_are_dropped(self):
        """A match without two completed sets has no meaningful total."""
        df = resolve_targets(self._frame([None, None], set1=[6.0, None]))
        assert df["match_uid"].to_list() == ["m0"]
        df = resolve_targets(self._frame([None, None], set2=[None, 6.0]))
        assert df["match_uid"].to_list() == ["m1"]

    def test_null_reason_is_treated_as_completed(self):
        """`reason` is null for ordinary matches; fill_null must not exclude
        them."""
        assert resolve_targets(self._frame([None, None])).height == 2


class TestLoadArtifact:
    """(b) config-text mismatch → None, or a sweep scores the OLD model."""

    def _config_and_path(self, tmp_path, text: str):
        from mvp.projection.iid.config import IIDProjectionConfig

        path = tmp_path / "cfg.yaml"
        path.write_text(text, encoding="utf-8")
        return IIDProjectionConfig.from_file(str(path)), path

    _YAML = """\
description: test
data:
  date_range:
    start: 2024-01-01
    end: 2025-12-31
features:
  include:
    - player_elo
serve_model:
  type: identity
  window: 90
"""

    def test_stale_config_text_returns_none(self, tmp_path, monkeypatch):
        from mvp.projection.iid import projection_run

        config, path = self._config_and_path(tmp_path, self._YAML)
        artifact = tmp_path / "serve_model.joblib"
        joblib.dump(
            {
                "serve_model": object(),
                "config_path": str(path),
                "config_yaml": self._YAML + "\n# edited since training\n",
                "n_train": 10,
            },
            artifact,
        )
        monkeypatch.setattr(
            projection_run, "artifact_path", lambda cfg, cfg_path: artifact
        )
        assert projection_run._load_artifact(config, path) is None

    def test_matching_config_text_loads(self, tmp_path, monkeypatch):
        from mvp.projection.iid import projection_run
        from mvp.projection.iid.serve_model import IdentityServeModel

        config, path = self._config_and_path(tmp_path, self._YAML)
        artifact = tmp_path / "serve_model.joblib"
        joblib.dump(
            {
                "serve_model": IdentityServeModel(window=90),
                "config_path": str(path),
                "config_yaml": self._YAML,
                "n_train": 10,
            },
            artifact,
        )
        monkeypatch.setattr(
            projection_run, "artifact_path", lambda cfg, cfg_path: artifact
        )
        loaded = projection_run._load_artifact(config, path)
        assert loaded is not None
        assert isinstance(loaded.serve_model, IdentityServeModel)

    def test_absent_artifact_returns_none(self, tmp_path, monkeypatch):
        from mvp.projection.iid import projection_run

        config, path = self._config_and_path(tmp_path, self._YAML)
        monkeypatch.setattr(
            projection_run,
            "artifact_path",
            lambda cfg, cfg_path: tmp_path / "nope.joblib",
        )
        assert projection_run._load_artifact(config, path) is None


def _spread_test_df(pairs: list[tuple[str, str]]) -> pl.DataFrame:
    """`pairs` is (kept-row player_id, other id); the uid sorts them."""
    n = len(pairs)
    uids = [f"2026_540_SGL_R32_{min(a, b)}_{max(a, b)}" for a, b in pairs]
    return pl.DataFrame(
        {
            "match_uid": uids,
            "player_id": [a for a, _ in pairs],
            "opp_id": [b for _, b in pairs],
            "effective_match_date": [date(2026, 1, 2 + i) for i in range(n)],
            "circuit": ["tour"] * n,
            "surface": ["Hard"] * n,
            "round": ["R32"] * n,
            "best_of": [3] * n,
            "_target_games_a": [12.0 + i for i in range(n)],
            "_target_games_b": [10.0] * n,
        }
    )


class TestBuildSpreadPmfFrame:
    """The spread pmf is the totals one in a different frame: signed margin with
    an offset, and an orientation flag pricing asserts against."""

    def test_actual_spread_is_a_minus_b_signed(self):
        df = _spread_test_df([("AA01", "ZZ99"), ("BB02", "YY88")])
        out = _output_for(df["match_uid"].to_list())
        pmf = build_spread_pmf_frame(df, out)
        assert pmf["actual_spread"].to_list() == [2.0, 3.0]

    def test_offset_is_written_not_inferred(self):
        """A reader assuming a 0-based index lands every lookup `offset` places
        out with nothing raising, so the constant travels with the data."""
        df = _spread_test_df([("AA01", "ZZ99")])
        out = _output_for(df["match_uid"].to_list())
        pmf = build_spread_pmf_frame(df, out)
        offset = pmf["spread_offset"][0]
        assert offset == out.distribution.spread_offset
        assert offset > 0, "a 0 offset would make the signed index unrecoverable"
        assert len(pmf["spread_pmf"][0]) == 2 * offset + 1

    def test_a_is_uid_min_true_when_kept_row_is_the_lower_id(self):
        df = _spread_test_df([("AA01", "ZZ99")])
        pmf = build_spread_pmf_frame(df, _output_for(df["match_uid"].to_list()))
        assert pmf["a_is_uid_min"].to_list() == [True]

    def test_a_is_uid_min_false_when_the_kept_row_is_the_higher_id(self):
        """Reachable: `_collapse_to_match_rows` keeps the lowest SURVIVING
        player_id, so a match arriving with one perspective row keeps that row
        whichever id it holds. This flag is what makes that visible downstream."""
        df = _spread_test_df([("ZZ99", "AA01")])
        pmf = build_spread_pmf_frame(df, _output_for(df["match_uid"].to_list()))
        assert pmf["a_is_uid_min"].to_list() == [False]

    def test_alignment_is_asserted(self):
        """Same guard as the totals frame — an equal-length but reordered pair
        would give every match another match's distribution silently."""
        df = _spread_test_df([("AA01", "ZZ99"), ("BB02", "YY88")])
        out = _output_for(list(reversed(df["match_uid"].to_list())))
        with pytest.raises(ValueError, match="not row-aligned"):
            build_spread_pmf_frame(df, out)

    def test_the_two_frames_agree_on_identity_and_disagree_on_outcome(self):
        """Both pmfs describe the same matches in the same order; only the
        outcome column and the distribution differ."""
        df = _spread_test_df([("AA01", "ZZ99"), ("BB02", "YY88")])
        out = _output_for(df["match_uid"].to_list())
        tot, spr = build_pmf_frame(df, out), build_spread_pmf_frame(df, out)
        assert tot["match_uid"].to_list() == spr["match_uid"].to_list()
        assert "actual_total" in tot.columns and "actual_total" not in spr.columns
        assert "actual_spread" in spr.columns and "actual_spread" not in tot.columns


class TestBuildFoldMatchFrame:
    """The runner-side fold frame for fold_match_win.parquet."""

    def test_columns_alignment_and_probability(self):
        from mvp.projection.iid.runner import build_fold_match_frame

        uids = ["m0", "m1"]
        df, out = _test_df(uids), _output_for(uids)
        from mvp.projection.iid.artifacts import SHAPE_COLUMNS

        frame = build_fold_match_frame(
            df, out, fold_idx=3, y_won=np.array([1, 0])
        )
        assert frame.columns == [
            "match_uid", "player_id", "opp_id", "effective_match_date",
            "fold_idx", "p_match_win_a", "won_a",
            *SHAPE_COLUMNS,
        ]
        # shape columns are reductions of the SAME output object
        np.testing.assert_allclose(
            frame["chain_hold_sum"].to_numpy(), out.h_a + out.h_b
        )
        np.testing.assert_allclose(
            frame["chain_egames"].to_numpy(),
            out.distribution.expected_total_games,
        )
        assert frame["won_a"].to_list() == [1, 0]
        assert frame["fold_idx"].to_list() == [3, 3]
        assert frame["player_id"].to_list() == df["player_id"].to_list()
        assert frame["opp_id"].to_list() == df["opp_id"].to_list()
        np.testing.assert_allclose(
            frame["p_match_win_a"].to_numpy(), out.distribution.p_match_win_a
        )

    def test_misaligned_output_raises(self):
        from mvp.projection.iid.runner import build_fold_match_frame

        df = _test_df(["m0", "m1"])
        out = _output_for(["m1", "m0"])
        with pytest.raises(ValueError, match="not row-aligned"):
            build_fold_match_frame(df, out, fold_idx=1, y_won=np.array([1, 0]))

    def test_feeds_the_artifact_writer(self, tmp_path):
        from mvp.projection.iid.artifacts import write_fold_match_win
        from mvp.projection.iid.runner import build_fold_match_frame

        parts = [
            build_fold_match_frame(
                _test_df(["m0"]), _output_for(["m0"]), 1, np.array([1])
            ),
            build_fold_match_frame(
                _test_df(["m1"]), _output_for(["m1"]), 2, np.array([0])
            ),
        ]
        path = write_fold_match_win(tmp_path, pl.concat(parts))
        assert pl.read_parquet(path).height == 2
