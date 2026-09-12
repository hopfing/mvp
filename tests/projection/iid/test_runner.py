"""Tests for IIDProjectionRunner — config loading and helper methods.

Full end-to-end runner integration is exercised by running the CLI against a
real parquet (`poetry run py -m mvp iid-project iid_projection_identity`); the
tests here cover the runner's deterministic helpers (target resolution and
match-row collapse) plus IIDProjectionConfig parsing.
"""

import textwrap
from datetime import date

import numpy as np
import polars as pl
import pytest

from mvp.model.splitters import (
    DateExpandingWindowSplitter,
    DateSlidingWindowSplitter,
    ExpandingWindowSplitter,
)
from mvp.projection.iid.config import IIDProjectionConfig
from mvp.projection.iid.runner import IIDProjectionRunner


class TestIIDProjectionConfig:
    def test_parse_minimal_yaml(self):
        yaml_str = textwrap.dedent(
            """
            description: "Test config"
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
              filters:
                draw_type: singles
                circuit: [tour, chal]
            features:
              include:
                - pts_service_won_pct(days=90)
            serve_model:
              type: identity
              window: 90
            validation:
              type: expanding_window
              initial_train_size: 1000
              step_size: 1000
            metrics:
              total_lines: [21.5, 22.5]
              spread_lines: [-2.5, 2.5]
            """
        )
        cfg = IIDProjectionConfig.from_yaml(yaml_str)
        assert cfg.description == "Test config"
        assert cfg.serve_model.type == "identity"
        assert cfg.serve_model.window == 90
        assert cfg.metrics.total_lines == [21.5, 22.5]
        assert cfg.metrics.spread_lines == [-2.5, 2.5]
        assert cfg.metrics.include_classification is True
        assert cfg.metrics.include_regression is True

    def test_default_serve_model(self):
        yaml_str = textwrap.dedent(
            """
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
            features:
              include:
                - pts_service_won_pct(days=90)
            """
        )
        cfg = IIDProjectionConfig.from_yaml(yaml_str)
        assert cfg.serve_model.type == "identity"
        assert cfg.serve_model.window == 90
        assert cfg.serve_model.clip_min == 0.30
        assert cfg.serve_model.clip_max == 0.90

    def test_invalid_serve_model_type(self):
        yaml_str = textwrap.dedent(
            """
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
            features:
              include:
                - pts_service_won_pct(days=90)
            serve_model:
              type: nonsense
            """
        )
        with pytest.raises(Exception):  # pydantic ValidationError
            IIDProjectionConfig.from_yaml(yaml_str)


class TestRunnerHelpers:
    """Test the runner's helper methods on hand-crafted DataFrames."""

    def _make_runner(self, tmp_path, validation: str | None = None):
        # Build a minimal config file so the runner can be instantiated.
        validation = validation or textwrap.dedent(
            """
            validation:
              type: expanding_window
              initial_train_size: 100
              step_size: 50
            """
        )
        config_path = tmp_path / "test.yaml"
        config_path.write_text(
            textwrap.dedent(
                """
                data:
                  date_range:
                    start: "2024-01-01"
                    end: "2025-12-31"
                features:
                  include:
                    - pts_service_won_pct(days=90)
                serve_model:
                  type: identity
                  window: 90
                """
            )
            + validation
        )
        return IIDProjectionRunner(
            config_path=config_path,
            log_to_mlflow=False,
        )

    def _build_match_df(self):
        """Two players A and B in a 2-set 6-4 6-3 match (A wins).

        Mirrored: 2 rows per match, one with player=A, opp=B and the reverse.
        """
        return pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "won": [True, False],
                "reason": [None, None],
                "best_of": [3, 3],
                "circuit": ["tour", "tour"],
                "surface": ["Hard", "Hard"],
                "round": ["R32", "R32"],
                "player_set1_games": [6, 4],
                "player_set2_games": [6, 3],
                "player_set3_games": [None, None],
                "player_set4_games": [None, None],
                "player_set5_games": [None, None],
                "opp_set1_games": [4, 6],
                "opp_set2_games": [3, 6],
                "opp_set3_games": [None, None],
                "opp_set4_games": [None, None],
                "opp_set5_games": [None, None],
            }
        )

    def test_resolve_targets_adds_target_columns(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df()
        result = runner._resolve_targets(df)
        assert "_target_games_a" in result.columns
        assert "_target_games_b" in result.columns
        # Row 0: player=A, A won 6+6=12 games
        assert result["_target_games_a"][0] == 12.0
        assert result["_target_games_b"][0] == 7.0
        # Row 1: player=B, B won 4+3=7 games
        assert result["_target_games_a"][1] == 7.0
        assert result["_target_games_b"][1] == 12.0

    def test_resolve_targets_filters_walkovers(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df().with_columns(
            pl.lit("W/O").alias("reason"),
        )
        result = runner._resolve_targets(df)
        assert len(result) == 0

    def test_resolve_targets_keeps_retirements_unscoreable(self, tmp_path):
        """The runner delegates to `projection_run.resolve_targets`, so the
        fold split sees every match that was played. A retirement reaches a
        test set — and the fold artifact — with no target."""
        runner = self._make_runner(tmp_path)
        df = self._build_match_df().with_columns(pl.lit("RET").alias("reason"))
        result = runner._resolve_targets(df)
        assert len(result) == 2
        assert result["_scoreable"].to_list() == [False, False]
        assert result["_target_games_a"].to_list() == [None, None]

    def test_resolve_targets_filters_missing_set_scores(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df().with_columns(
            pl.lit(None).cast(pl.Int64).alias("player_set1_games"),
        )
        result = runner._resolve_targets(df)
        assert len(result) == 2
        assert result["_scoreable"].to_list() == [False, False]

    def test_resolve_targets_is_the_projection_run_implementation(self, tmp_path):
        """One implementation, not two: the fold artifact and the forward pmf
        must agree on which matches exist and which are scoreable."""
        from mvp.projection.iid import projection_run

        runner = self._make_runner(tmp_path)
        df = self._build_match_df()
        assert runner._resolve_targets(df).equals(projection_run.resolve_targets(df))

    def test_collapse_to_match_rows_one_per_match(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df()
        collapsed = runner._collapse_to_match_rows(df)
        assert len(collapsed) == 1
        # Lower player_id "A" should be the kept row
        assert collapsed["player_id"][0] == "A"

    def test_make_splitter_forwards_calendar_month_params(self, tmp_path):
        """A promoted serve-FS config inherits its match-grain validation block
        verbatim, so date_expanding has to survive the trip into make_splitter."""
        runner = self._make_runner(tmp_path, textwrap.dedent(
            """
            validation:
              type: date_expanding
              initial_train_months: 12
              test_months: 6
            """
        ))
        splitter = runner._make_splitter()
        assert isinstance(splitter, DateExpandingWindowSplitter)
        assert splitter.initial_train_months == 12
        assert splitter.test_months == 6

    def test_make_splitter_date_sliding(self, tmp_path):
        runner = self._make_runner(tmp_path, textwrap.dedent(
            """
            validation:
              type: date_sliding
              train_months: 24
              test_months: 3
            """
        ))
        splitter = runner._make_splitter()
        assert isinstance(splitter, DateSlidingWindowSplitter)
        assert splitter.train_months == 24
        assert splitter.test_months == 3

    def test_make_splitter_count_based_modes_still_work(self, tmp_path):
        splitter = self._make_runner(tmp_path)._make_splitter()
        assert isinstance(splitter, ExpandingWindowSplitter)

    def test_make_splitter_date_windows_over_the_match_frame(self, tmp_path):
        """The date splitter reads effective_match_date off the collapsed frame."""
        runner = self._make_runner(tmp_path, textwrap.dedent(
            """
            validation:
              type: date_expanding
              initial_train_months: 6
              test_months: 3
            """
        ))
        df = pl.DataFrame({
            "match_uid": [f"m{i}" for i in range(24)],
            "effective_match_date": [
                date(2024, 1 + i // 2, 1 + (i % 2) * 10) for i in range(24)
            ],
        })
        folds = list(runner._make_splitter().split(df))
        assert len(folds) == 2
        for train_idx, test_idx in folds:
            assert train_idx and test_idx
            assert max(df["effective_match_date"][i] for i in train_idx) < min(
                df["effective_match_date"][i] for i in test_idx
            )

    def test_collapse_picks_lower_id(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1", "m2", "m2"],
                "player_id": ["zoe", "anna", "ben", "ada"],
                "best_of": [3, 3, 3, 3],
            }
        )
        collapsed = runner._collapse_to_match_rows(df)
        assert len(collapsed) == 2
        # m1 → "anna" (lex smaller than "zoe")
        # m2 → "ada" (lex smaller than "ben")
        kept = sorted(zip(collapsed["match_uid"].to_list(), collapsed["player_id"].to_list()))
        assert kept == [("m1", "anna"), ("m2", "ada")]


class TestPreloadMatchSpecs:
    """Which match features the runner materializes once for the whole run.

    Two-level configs were excluded from the preload entirely, so each of their
    three branches re-read points and re-ran `engine.compute` per fold — three
    times the reload a single-level config pays, multiplied again by trial count
    under `mvp tune`.
    """

    @staticmethod
    def _cfg(**kw):
        from mvp.projection.iid.config import ServeModelConfig

        return ServeModelConfig(**kw)

    def test_single_level_uses_its_own_field(self):
        from mvp.projection.iid.runner import preload_match_specs

        cfg = self._cfg(type="score_state", match_level_features=["player_glicko_rd"])
        assert preload_match_specs(cfg) == ["player_glicko_rd"]

    def test_two_level_unions_all_three_components(self):
        from mvp.projection.iid.runner import preload_match_specs

        cfg = self._cfg(
            type="two_level",
            first_in_match_features=["player_a"],
            win_first_match_features=["player_b"],
            win_second_match_features=["player_c"],
        )
        assert preload_match_specs(cfg) == ["player_a", "player_b", "player_c"]

    def test_two_level_ignores_the_single_level_field(self):
        """`match_level_features` is inert under type=two_level. Reading it
        would preload nothing and leave every branch recomputing per fold."""
        from mvp.projection.iid.runner import preload_match_specs

        cfg = self._cfg(
            type="two_level",
            match_level_features=["player_never_used"],
            win_first_match_features=["player_b"],
        )
        assert preload_match_specs(cfg) == ["player_b"]

    def test_shared_specs_are_deduped_once(self):
        """Components overlap heavily in practice — base_fi and base_w2 both
        select player_glicko_rd_diff. Computing it twice is wasted work."""
        from mvp.projection.iid.runner import preload_match_specs

        cfg = self._cfg(
            type="two_level",
            first_in_match_features=["player_a", "player_shared"],
            win_first_match_features=["player_shared"],
            win_second_match_features=["player_shared", "player_b"],
        )
        assert preload_match_specs(cfg) == [
            "player_a", "player_shared", "player_b",
        ]

    def test_no_swap_side_partners_are_added(self):
        """`engine.compute` returns two mirrored ROWS per match, so fit-time
        joins get the returner's values from the row. Partners are a
        predict-time need met by `features.include`; requesting them here only
        triggers mirror self-joins nothing at fit time selects."""
        from mvp.projection.iid.runner import preload_match_specs

        cfg = self._cfg(
            type="two_level", win_first_match_features=["player_glicko_rd"],
        )
        assert preload_match_specs(cfg) == ["player_glicko_rd"]

    def test_a_two_level_config_with_no_features_preloads_nothing(self):
        from mvp.projection.iid.runner import preload_match_specs

        assert preload_match_specs(self._cfg(type="two_level")) == []


class _StubEngine:
    """FeatureEngine stand-in: the runner's one `compute` call returns a frame
    built here, so the end-to-end test needs no parquet and no cache."""

    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame

    def compute(self, feature_specs=None, extra_columns=None, **_):
        return self._frame


_SET_COLS = [f"player_set{i}_games" for i in range(1, 6)] + [
    f"opp_set{i}_games" for i in range(1, 6)
] + [f"player_set{i}_tiebreak" for i in range(1, 6)] + [
    f"opp_set{i}_tiebreak" for i in range(1, 6)
]

_E2E_YAML = textwrap.dedent(
    """
    description: e2e
    data:
      date_range:
        start: "2024-01-01"
        end: "2024-12-31"
    features:
      include:
        - pts_service_won_pct(days=90)
    serve_model:
      type: identity
      window: 90
    validation:
      type: date_expanding
      initial_train_months: 6
      test_months: 3
    metrics:
      total_lines: [21.5, 22.5]
      spread_lines: [-2.5, 2.5]
    """
)

# The retired match, inside fold 1's test window (2024-08-28).
_RET_IDX = 40
_RET_UID = f"m{_RET_IDX:03d}"


def _e2e_frame(*, with_ret: bool) -> pl.DataFrame:
    """Mirrored per-player rows for 60 matches across 2024, one of them a
    retirement whose first two sets ARE present — the case the mask exists for:
    unmasked, `total_games_won`'s fill_null(0) would hand it a real partial
    total no book would have settled."""
    from datetime import timedelta

    rows: list[dict] = []
    for i in range(60):
        if i == _RET_IDX and not with_ret:
            continue
        reason = "RET" if i == _RET_IDX else None
        day = date(2024, 1, 1) + timedelta(days=6 * i)
        three_set = i % 4 == 3
        # Seeded per match index, so removing one match leaves every other
        # match's outcome identical -- the two runs differ in one row, nothing
        # else.
        a_wins = bool(np.random.default_rng(1000 + i).random() < 0.5)
        a_sets = [6.0, 4.0 if three_set else 6.0, 6.0 if three_set else None]
        b_sets = [3.0 + (i % 3), 6.0 if three_set else 3.0,
                  4.0 if three_set else None]
        a_serve = 0.60 + 0.002 * (i % 11)
        b_serve = 0.58 + 0.002 * (i % 7)
        for side in ("a", "b"):
            mine, theirs = (a_sets, b_sets) if side == "a" else (b_sets, a_sets)
            my_serve, their_serve = (
                (a_serve, b_serve) if side == "a" else (b_serve, a_serve)
            )
            row = {
                "match_uid": f"m{i:03d}",
                "player_id": f"{'A' if side == 'a' else 'B'}{i:03d}",
                "opp_id": f"{'B' if side == 'a' else 'A'}{i:03d}",
                "won": a_wins if side == "a" else not a_wins,
                "reason": reason,
                "best_of": 3,
                "circuit": "tour",
                # Decorrelated from the winner: a segment that lines up with
                # the outcome is single-class and cannot be scored.
                "surface": "Hard" if (i // 3) % 2 else "Clay",
                "round": "R32",
                "effective_match_date": day,
                "player_pts_service_won_pct_90d": my_serve,
                "opp_pts_service_won_pct_90d": their_serve,
                "pts_service_pts_won": 40.0 + i % 5,
                "pts_service_pts_played": 70.0,
                "opp_pts_service_pts_won": 38.0 + i % 4,
                "opp_pts_service_pts_played": 70.0,
                "svc_games_played": 10.0,
                "svc_bp_saved": 2.0,
                "svc_bp_faced": 3.0,
                "opp_svc_games_played": 10.0,
                "opp_svc_bp_saved": 1.0,
                "opp_svc_bp_faced": 3.0,
            }
            for s in range(1, 6):
                row[f"player_set{s}_games"] = (
                    mine[s - 1] if s <= len(mine) else None
                )
                row[f"opp_set{s}_games"] = (
                    theirs[s - 1] if s <= len(theirs) else None
                )
                row[f"player_set{s}_tiebreak"] = (
                    7.0 if (s == 1 and i % 5 == 0) else None
                )
                row[f"opp_set{s}_tiebreak"] = (
                    5.0 if (s == 1 and i % 5 == 0) else None
                )
            rows.append(row)
    return pl.DataFrame(rows).with_columns(
        [pl.col(c).cast(pl.Float64) for c in _SET_COLS]
    )


class TestRunWritesEveryMatch:
    """End-to-end: the projection predicts every match in a fold's test window
    and writes it; only fitting and scoring use the scoreable subset."""

    def _run(self, tmp_path, monkeypatch, *, with_ret: bool):
        from mvp.projection.iid.artifacts import fp_dir_for

        monkeypatch.setenv("MVP_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
        config_path = tmp_path / "e2e.yaml"
        config_path.write_text(_E2E_YAML, encoding="utf-8")
        runner = IIDProjectionRunner(
            config_path=config_path,
            matches_path=tmp_path / "matches.parquet",
            cache_dir=tmp_path / "cache",
            log_to_mlflow=False,
        )
        runner.engine = _StubEngine(_e2e_frame(with_ret=with_ret))
        result = runner.run()
        return result, fp_dir_for(runner.config, config_path)

    def test_the_retirement_is_in_the_oof_store_with_a_real_probability(
        self, tmp_path, monkeypatch
    ):
        """The null-in-feature that encoded "this match ended early" is gone:
        the row exists, carries a probability, and says it is not scoreable."""
        result, fp_dir = self._run(tmp_path, monkeypatch, with_ret=True)
        art = pl.read_parquet(fp_dir / "fold_match_win.parquet")

        ret = art.filter(pl.col("match_uid") == _RET_UID)
        assert ret.height == 1, "the retirement never reached the fold artifact"
        assert ret["scoreable"][0] == 0
        assert 0.0 < ret["p_match_win_a"][0] < 1.0
        assert ret["won_a"][0] in (0, 1)
        assert (
            art.filter(pl.col("match_uid") != _RET_UID)["scoreable"] == 1
        ).all()
        assert result["n_scoreable"] == result["n_matches"] - 1

    def test_scoring_is_unchanged_by_the_unscoreable_row(
        self, tmp_path, monkeypatch
    ):
        """Fit and metric populations are exactly today's: the run with the
        retirement scores identically to the run without it."""
        with_ret, _ = self._run(tmp_path, monkeypatch, with_ret=True)
        without, _ = self._run(tmp_path, monkeypatch, with_ret=False)

        assert with_ret["n_matches"] == without["n_matches"] + 1
        assert with_ret["n_scoreable"] == without["n_scoreable"]
        assert with_ret["n_folds"] == without["n_folds"]
        for mine, theirs in zip(with_ret["fold_metrics"], without["fold_metrics"]):
            assert mine.keys() == theirs.keys()
            for k in mine:
                assert mine[k] == pytest.approx(theirs[k], nan_ok=True), k
        for k in with_ret["metrics"]:
            assert with_ret["metrics"][k] == pytest.approx(
                without["metrics"][k], nan_ok=True
            ), k
