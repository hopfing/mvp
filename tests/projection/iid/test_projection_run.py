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


def _test_df(
    match_uids: list[str], scoreable: list[bool] | None = None
) -> pl.DataFrame:
    """A projected frame as `resolve_targets` leaves it.

    Unscoreable rows carry null targets — the masking `resolve_targets` applies,
    without which the score helpers' `fill_null(0)` hands a retirement a real
    partial total.
    """
    n = len(match_uids)
    flags = [True] * n if scoreable is None else list(scoreable)
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
            "_scoreable": flags,
            "_target_games_a": [
                12.0 + i if flags[i] else None for i in range(n)
            ],
            "_target_games_b": [10.0 if flags[i] else None for i in range(n)],
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
        assert pmf["scoreable"].to_list() == [1, 1]

    def test_an_unscoreable_match_is_projected_with_no_outcome(self):
        """Prediction needs no target: the retirement gets a pmf like any other
        match, `scoreable == 0` marks it, and `actual_total` is null so nothing
        settles it."""
        uids = ["m0", "m1"]
        out = _output_for(uids)
        pmf = build_pmf_frame(_test_df(uids, scoreable=[True, False]), out)

        assert pmf.height == 2
        assert pmf["scoreable"].to_list() == [1, 0]
        assert pmf["scoreable"].dtype == pl.Int8
        assert pmf["actual_total"].to_list() == [22.0, None]
        assert pmf["p_match_win_a"][1] == out.distribution.p_match_win_a[1]


class TestResolveTargets:
    """Every match with a result is kept; only the TARGET is withheld.

    Prediction needs no target, so a retirement stays in the frame (and reaches
    the artifacts) with `_scoreable` False and null targets. Only a walkover —
    a non-match — is dropped.
    """

    def _frame(
        self, reasons: list[str | None], set1=None, set2=None, won=None
    ) -> pl.DataFrame:
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
        if won is not None:
            data["won"] = won
        return pl.DataFrame(data)

    @pytest.mark.parametrize("reason", ["RET", "DEF", "UNP"])
    def test_unfinished_matches_are_kept_unscoreable(self, reason):
        """A retirement produces fewer games than the match would have, so it
        has no total to score — but it is still a match that was played, and the
        chain predicted it pre-match. Kept, flagged, target withheld."""
        df = resolve_targets(self._frame([reason, None]))
        assert df["match_uid"].to_list() == ["m0", "m1"]
        assert df["_scoreable"].to_list() == [False, True]
        # Masked, not summed: the score helpers fill_null(0) and never
        # null-propagate, so an unmasked RET row would carry a real partial
        # total (6-4 6-4 here = 12.0) that a book would never have settled.
        assert df["_target_games_a"][0] is None
        assert df["_target_games_b"][0] is None
        assert df["_target_games_a"][1] == 12.0

    def test_walkovers_are_dropped(self):
        """A walkover is not a match: no play, nothing to predict."""
        df = resolve_targets(self._frame(["W/O", None]))
        assert df["match_uid"].to_list() == ["m1"]

    def test_completed_match_survives_with_targets(self):
        df = resolve_targets(self._frame([None]))
        assert df.height == 1
        assert df["_scoreable"].to_list() == [True]
        assert df["_target_games_a"][0] == 12.0
        assert df["_target_games_b"][0] == 8.0

    def test_missing_first_two_sets_are_kept_unscoreable(self):
        """A match without two completed sets has no meaningful total, but it
        still has a result and a pre-match prediction."""
        df = resolve_targets(self._frame([None, None], set1=[6.0, None]))
        assert df["match_uid"].to_list() == ["m0", "m1"]
        assert df["_scoreable"].to_list() == [True, False]
        assert df["_target_games_a"][1] is None
        df = resolve_targets(self._frame([None, None], set2=[None, 6.0]))
        assert df["_scoreable"].to_list() == [False, True]

    def test_null_reason_is_treated_as_completed(self):
        """`reason` is null for ordinary matches; fill_null must not exclude
        them."""
        out = resolve_targets(self._frame([None, None]))
        assert out.height == 2
        assert out["_scoreable"].to_list() == [True, True]

    def test_null_won_is_dropped(self):
        """The classification stack's target is `won`; a row without one is not
        a row either stack can use."""
        df = resolve_targets(self._frame([None, None], won=[True, None]))
        assert df["match_uid"].to_list() == ["m0"]

    def test_won_is_optional(self):
        """The helper runs on frames that never carried `won` (the pmf-side
        tests build them); its absence must not raise."""
        assert resolve_targets(self._frame([None])).height == 1


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


def _spread_test_df(
    pairs: list[tuple[str, str]], scoreable: list[bool] | None = None
) -> pl.DataFrame:
    """`pairs` is (kept-row player_id, other id); the uid sorts them."""
    n = len(pairs)
    flags = [True] * n if scoreable is None else list(scoreable)
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
            "_scoreable": flags,
            "_target_games_a": [
                12.0 + i if flags[i] else None for i in range(n)
            ],
            "_target_games_b": [10.0 if flags[i] else None for i in range(n)],
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
        assert pmf["scoreable"].to_list() == [1, 1]

    def test_an_unscoreable_match_is_projected_with_no_margin(self):
        """Same rule as the totals frame: the row is priced-out, not dropped."""
        df = _spread_test_df([("AA01", "ZZ99"), ("BB02", "YY88")],
                             scoreable=[True, False])
        out = _output_for(df["match_uid"].to_list())
        pmf = build_spread_pmf_frame(df, out)
        assert pmf.height == 2
        assert pmf["scoreable"].to_list() == [1, 0]
        assert pmf["scoreable"].dtype == pl.Int8
        assert pmf["actual_spread"].to_list() == [2.0, None]
        assert pmf["p_match_win_a"][1] == out.distribution.p_match_win_a[1]

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
            df, out, fold_idx=3, y_won=np.array([1, 0]),
            scoreable=df["_scoreable"],
        )
        assert frame.columns == [
            "match_uid", "player_id", "opp_id", "effective_match_date",
            "fold_idx", "p_match_win_a", "won_a", "scoreable",
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

    def test_scoreable_rides_along_as_int8_and_won_a_is_cast_not_inferred(self):
        """The OOF store's readers split on `scoreable`; `won_a` is defined on
        every written row (a retirement has a winner) and is cast rather than
        left to whatever dtype `won` arrives in."""
        from mvp.projection.iid.runner import build_fold_match_frame

        uids = ["m0", "m1"]
        df = _test_df(uids, scoreable=[True, False])
        frame = build_fold_match_frame(
            df, _output_for(uids), fold_idx=1,
            y_won=pl.Series("won", [True, False]),
            scoreable=df["_scoreable"],
        )
        assert frame["scoreable"].to_list() == [1, 0]
        assert frame["scoreable"].dtype == pl.Int8
        assert frame["won_a"].to_list() == [1, 0]
        assert frame["won_a"].dtype == pl.Int8

    def test_misaligned_output_raises(self):
        from mvp.projection.iid.runner import build_fold_match_frame

        df = _test_df(["m0", "m1"])
        out = _output_for(["m1", "m0"])
        with pytest.raises(ValueError, match="not row-aligned"):
            build_fold_match_frame(
                df, out, fold_idx=1, y_won=np.array([1, 0]),
                scoreable=df["_scoreable"],
            )

    def test_feeds_the_artifact_writer(self, tmp_path):
        from mvp.projection.iid.artifacts import write_fold_match_win
        from mvp.projection.iid.runner import build_fold_match_frame

        parts = [
            build_fold_match_frame(
                _test_df(["m0"]), _output_for(["m0"]), 1, np.array([1]),
                scoreable=pl.Series([True]),
            ),
            build_fold_match_frame(
                _test_df(["m1"], scoreable=[False]), _output_for(["m1"]), 2,
                np.array([0]), scoreable=pl.Series([False]),
            ),
        ]
        path = write_fold_match_win(tmp_path, pl.concat(parts))
        written = pl.read_parquet(path)
        assert written.height == 2
        assert written["scoreable"].to_list() == [1, 0]


_FIT_YAML = """\
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


def _engine_frame(
    reasons: list[str | None], days: list[date] | None = None
) -> pl.DataFrame:
    """Mirrored per-player rows, the shape the feature engine hands the pair of
    population builders (`_train_projector` and `build_test_set`)."""
    n = len(reasons)
    days = days or [date(2025, 1, 2 + i) for i in range(n)]
    data: dict = {
        "match_uid": [f"m{i}" for i in range(n) for _ in (0, 1)],
        "player_id": [f"{p}{i:02d}" for i in range(n) for p in ("A", "B")],
        "opp_id": [f"{p}{i:02d}" for i in range(n) for p in ("B", "A")],
        "won": [p == "A" for _ in range(n) for p in ("A", "B")],
        "reason": [r for r in reasons for _ in (0, 1)],
        "best_of": [3] * (2 * n),
        "effective_match_date": [d for d in days for _ in (0, 1)],
    }
    for j in range(1, 6):
        data[f"player_set{j}_games"] = [6.0 if j <= 2 else None] * (2 * n)
        data[f"opp_set{j}_games"] = [4.0 if j <= 2 else None] * (2 * n)
    return pl.DataFrame(data)


def _fit_config(tmp_path):
    from mvp.projection.iid.config import IIDProjectionConfig

    path = tmp_path / "fit.yaml"
    path.write_text(_FIT_YAML, encoding="utf-8")
    return IIDProjectionConfig.from_file(str(path))


class TestTrainProjector:
    """The fit population is unchanged by the widening: scoreable rows only.

    Whether the serve models should learn from points played in a retired match
    is a modelling question with its own gate, not a consequence of this one.
    """

    def test_fit_sees_only_scoreable_rows(self, tmp_path, monkeypatch):
        from mvp.projection.iid import projection_run
        from mvp.projection.iid.projector import TennisProjector

        seen: dict = {}
        monkeypatch.setattr(
            TennisProjector, "fit", lambda self, df: seen.__setitem__("df", df)
        )
        projection_run._train_projector(
            _fit_config(tmp_path), _engine_frame([None, "RET", None])
        )
        assert seen["df"]["match_uid"].to_list() == ["m0", "m2"]
        assert seen["df"]["_scoreable"].to_list() == [True, True]


class TestBuildTestSet:
    """The forward set predicts everything settled; only the target is withheld."""

    def test_an_unscoreable_settled_match_is_projected(self, tmp_path):
        from mvp.projection.iid import projection_run

        test = projection_run.build_test_set(
            _fit_config(tmp_path),
            _engine_frame(
                [None, "RET", "W/O"],
                days=[date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4)],
            ),
        )
        assert test["match_uid"].to_list() == ["m0", "m1"]
        assert test["_scoreable"].to_list() == [True, False]
        assert test["_target_games_a"][1] is None
