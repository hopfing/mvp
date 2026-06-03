"""Derivation-correctness tests for H68 MTL aux variants (#1-#6).

Cross-site parity (runner / predictor / fast_selection identical) was verified
during stage-by-stage code review; here we focus on derivation values, null
edge cases, and the secondary drop_nulls gate behavior.
"""

import importlib
from pathlib import Path

import polars as pl
import pytest

from mvp.model.runner import ExperimentRunner


# All 14 supported aux names (3 original + 11 H68) for the most-permissive test.
ALL_AUX = [
    "game_margin",
    "set_margin",
    "set_count",
    "total_pts_won_diff",
    "service_pts_won_pct_diff",
    "return_pts_won_pct_diff",
    "first_serve_won_pct_diff",
    "bp_save_pct_diff",
    "svc_serve_rating_diff",
    "ret_return_rating_diff",
    "set1_games_diff",
    "set2_games_diff",
    "duration_seconds",
    "wl_continuous_proxy",
]


class TestH68AuxDerivations:
    @pytest.fixture(autouse=True)
    def ensure_features_registered(self, isolated_registry):
        import mvp.model.features.h2h
        import mvp.model.features.ranking
        import mvp.model.features.serve
        import mvp.model.features.win_rate

        importlib.reload(mvp.model.features.h2h)
        importlib.reload(mvp.model.features.ranking)
        importlib.reload(mvp.model.features.serve)
        importlib.reload(mvp.model.features.win_rate)

    @pytest.fixture
    def empty_matches(self, tmp_path: Path) -> Path:
        df = pl.DataFrame(
            {
                "match_uid": ["M0"],
                "player_id": ["P0"],
                "opp_id": ["P1"],
                "effective_match_date": ["2024-01-01"],
                "won": [True],
                "circuit": ["tour"],
            }
        ).with_columns(pl.col("effective_match_date").str.to_datetime())
        path = tmp_path / "matches.parquet"
        df.write_parquet(path)
        return path

    def _make_config(self, tmp_path: Path, aux: list[str]) -> Path:
        yaml_aux = "\n".join(f"    - {t}" for t in aux)
        cfg_str = f"""
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: xgboost
  params:
    n_estimators: 10
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 50
  test_size: 25
mtl:
  auxiliary_targets:
{yaml_aux}
"""
        path = tmp_path / "config.yaml"
        path.write_text(cfg_str)
        return path

    def _h68_fixture(self) -> pl.DataFrame:
        """4 rows + 1 pathological row covering normal/edge cases.

        Hand-computed expected aux values per row:

        M0 — normal completed match, all aux defined:
          total_pts_won_diff = 45 - 30 = 15
          service_pts_won_pct_diff = 30/40 - 20/35 = 0.75 - 0.571428... = 0.178571...
          return_pts_won_pct_diff = 15/35 - 10/40 = 0.428571... - 0.25 = 0.178571...
          first_serve_won_pct_diff = 22/30 - 14/22 = 0.733... - 0.636... ≈ 0.0970
          bp_save_pct_diff = 3/4 - 2/5 = 0.35
          svc_serve_rating_diff = 75.5 - 60.2 = 15.3
          ret_return_rating_diff = 32.1 - 25.5 = 6.6
          set1_games_diff = 6 - 4 = 2; set2_games_diff = 6 - 2 = 4
          duration_seconds = 5400
          wl_continuous_proxy = 2*1 - 1 = +1

        M1 — server held all games (svc_bp_faced=0). bp_save_pct_diff aux
        becomes null on the player side and the row drops if that aux is
        configured. All other aux still well-defined.

        M2 — loss with full data. wl_continuous_proxy = 2*0 - 1 = -1.

        M3 — 5-set win. set_count = 5; per-set games for sets 3-5 defined
        but variant #4 (set1/set2_games_diff) ignores them.

        M4 — pathological partial set: reason=None and sets_played=2 (passes
        gates), but player_set2_games=None. Variant #4 aux must null this
        row via the is_not_null guard.
        """
        return pl.DataFrame(
            {
                "match_uid": ["M0", "M1", "M2", "M3", "M4"],
                "won": [True, True, False, True, True],
                "reason": [None, None, None, None, None],
                "sets_played": [2, 2, 2, 5, 2],
                "best_of": [3, 3, 3, 5, 3],
                "player_set1_games": [6, 6, 4, 6, 6],
                "opp_set1_games": [4, 4, 6, 4, 4],
                "player_set2_games": [6, 6, 2, 6, None],
                "opp_set2_games": [2, 0, 6, 4, 4],
                "player_set3_games": [None, None, None, 4, None],
                "opp_set3_games": [None, None, None, 6, None],
                "player_set4_games": [None, None, None, 4, None],
                "opp_set4_games": [None, None, None, 6, None],
                "player_set5_games": [None, None, None, 6, None],
                "opp_set5_games": [None, None, None, 3, None],
                "pts_total_pts_won": [45, 30, 30, 100, 40],
                "opp_pts_total_pts_won": [30, 25, 45, 95, 25],
                "pts_service_pts_won": [30, 25, 20, 60, 25],
                "pts_service_pts_played": [40, 30, 35, 80, 30],
                "opp_pts_service_pts_won": [20, 15, 30, 55, 15],
                "opp_pts_service_pts_played": [35, 25, 40, 75, 25],
                "pts_return_pts_won": [15, 5, 10, 40, 15],
                "pts_return_pts_played": [35, 25, 40, 75, 25],
                "opp_pts_return_pts_won": [10, 5, 15, 35, 10],
                "opp_pts_return_pts_played": [40, 30, 35, 80, 30],
                "svc_first_serve_pts_won": [22, 18, 15, 45, 18],
                "svc_first_serve_pts_played": [30, 22, 25, 60, 22],
                "opp_svc_first_serve_pts_won": [14, 12, 22, 40, 12],
                "opp_svc_first_serve_pts_played": [22, 16, 30, 55, 16],
                "svc_bp_saved": [3, 0, 2, 5, 1],
                "svc_bp_faced": [4, 0, 5, 8, 2],
                "opp_svc_bp_saved": [2, 1, 3, 6, 2],
                "opp_svc_bp_faced": [5, 3, 4, 10, 4],
                "svc_serve_rating": [75.5, 80.0, 50.0, 70.0, 72.0],
                "opp_svc_serve_rating": [60.2, 65.0, 65.0, 68.0, 60.0],
                "ret_return_rating": [32.1, 25.0, 20.0, 35.0, 28.0],
                "opp_ret_return_rating": [25.5, 28.0, 30.0, 32.0, 26.0],
                "duration_seconds": [5400.0, 4800.0, 6000.0, 14400.0, 5200.0],
                "effective_match_date": ["2024-01-01"] * 5,
            }
        ).with_columns(pl.col("effective_match_date").str.to_datetime())

    def _run(self, tmp_path: Path, empty_matches: Path, aux: list[str]):
        cfg = self._make_config(tmp_path, aux)
        runner = ExperimentRunner(config_path=cfg, matches_path=empty_matches)
        return runner._resolve_target(self._h68_fixture())

    def test_total_pts_won_diff_values(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["total_pts_won_diff"])
        keyed = dict(zip(df_out["match_uid"].to_list(), df_out["_aux_total_pts_won_diff"].to_list()))
        assert keyed["M0"] == pytest.approx(15.0)
        assert keyed["M1"] == pytest.approx(5.0)
        assert keyed["M2"] == pytest.approx(-15.0)
        assert keyed["M3"] == pytest.approx(5.0)
        assert keyed["M4"] == pytest.approx(15.0)

    def test_service_pts_won_pct_diff_values(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["service_pts_won_pct_diff"])
        m0 = (
            df_out.filter(pl.col("match_uid") == "M0")["_aux_service_pts_won_pct_diff"]
            .to_list()[0]
        )
        assert m0 == pytest.approx(30 / 40 - 20 / 35)

    def test_return_pts_won_pct_diff_values(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["return_pts_won_pct_diff"])
        m0 = (
            df_out.filter(pl.col("match_uid") == "M0")["_aux_return_pts_won_pct_diff"]
            .to_list()[0]
        )
        assert m0 == pytest.approx(15 / 35 - 10 / 40)

    def test_first_serve_won_pct_diff_values(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["first_serve_won_pct_diff"])
        m0 = (
            df_out.filter(pl.col("match_uid") == "M0")["_aux_first_serve_won_pct_diff"]
            .to_list()[0]
        )
        assert m0 == pytest.approx(22 / 30 - 14 / 22)

    def test_bp_save_pct_diff_zero_denom_drops_row(
        self, tmp_path: Path, empty_matches: Path
    ):
        """svc_bp_faced=0 on player side yields null aux; row drops via drop_nulls."""
        df_out, _ = self._run(tmp_path, empty_matches, ["bp_save_pct_diff"])
        uids = df_out["match_uid"].to_list()
        assert "M1" not in uids
        assert "M0" in uids
        m0 = (
            df_out.filter(pl.col("match_uid") == "M0")["_aux_bp_save_pct_diff"]
            .to_list()[0]
        )
        assert m0 == pytest.approx(3 / 4 - 2 / 5)

    def test_svc_serve_rating_diff_values(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["svc_serve_rating_diff"])
        keyed = dict(
            zip(df_out["match_uid"].to_list(), df_out["_aux_svc_serve_rating_diff"].to_list())
        )
        assert keyed["M0"] == pytest.approx(15.3)
        assert keyed["M2"] == pytest.approx(-15.0)

    def test_ret_return_rating_diff_values(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["ret_return_rating_diff"])
        m0 = (
            df_out.filter(pl.col("match_uid") == "M0")["_aux_ret_return_rating_diff"]
            .to_list()[0]
        )
        assert m0 == pytest.approx(6.6)

    def test_set1_games_diff_values(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["set1_games_diff"])
        keyed = dict(zip(df_out["match_uid"].to_list(), df_out["_aux_set1_games_diff"].to_list()))
        assert keyed["M0"] == pytest.approx(2.0)
        assert keyed["M2"] == pytest.approx(-2.0)
        assert keyed["M3"] == pytest.approx(2.0)

    def test_set2_games_diff_drops_partial_set_row(
        self, tmp_path: Path, empty_matches: Path
    ):
        """M4 has player_set2_games=None despite reason=None and sets_played=2;
        the is_not_null guard nulls the aux which drop_nulls then drops."""
        df_out, _ = self._run(tmp_path, empty_matches, ["set2_games_diff"])
        uids = df_out["match_uid"].to_list()
        assert "M4" not in uids
        keyed = dict(zip(df_out["match_uid"].to_list(), df_out["_aux_set2_games_diff"].to_list()))
        assert keyed["M0"] == pytest.approx(4.0)
        assert keyed["M1"] == pytest.approx(6.0)
        assert keyed["M2"] == pytest.approx(-4.0)

    def test_set_shape_does_not_use_fill_null_helpers(
        self, tmp_path: Path, empty_matches: Path
    ):
        """Regression guard: if someone wires set-shape aux through
        _total_games_won() helpers (which fill_null(0)), a partial-set row
        like M4 would silently produce a wrong nonzero value instead of
        being dropped."""
        df_out, _ = self._run(
            tmp_path, empty_matches, ["set1_games_diff", "set2_games_diff"]
        )
        assert "M4" not in df_out["match_uid"].to_list()

    def test_duration_seconds_passthrough(self, tmp_path: Path, empty_matches: Path):
        df_out, _ = self._run(tmp_path, empty_matches, ["duration_seconds"])
        keyed = dict(zip(df_out["match_uid"].to_list(), df_out["_aux_duration_seconds"].to_list()))
        assert keyed["M0"] == pytest.approx(5400.0)
        assert keyed["M2"] == pytest.approx(6000.0)
        assert keyed["M3"] == pytest.approx(14400.0)

    def test_wl_continuous_proxy_is_plus_minus_one(
        self, tmp_path: Path, empty_matches: Path
    ):
        """Variant #6: 2*y - 1 → exactly ±1 for the binary `won` target."""
        df_out, _ = self._run(tmp_path, empty_matches, ["wl_continuous_proxy"])
        keyed = dict(
            zip(df_out["match_uid"].to_list(), df_out["_aux_wl_continuous_proxy"].to_list())
        )
        assert keyed["M0"] == pytest.approx(1.0)
        assert keyed["M1"] == pytest.approx(1.0)
        assert keyed["M2"] == pytest.approx(-1.0)
        assert keyed["M3"] == pytest.approx(1.0)
        assert keyed["M4"] == pytest.approx(1.0)

    def test_wl_continuous_proxy_with_deciding_set_target(
        self, tmp_path: Path, empty_matches: Path
    ):
        """Variant #6 referenced via deciding_set primary target.

        For deciding_set, the primary col is `_target_deciding_set` = 1 iff
        sets_played == best_of (M3 only among kept rows since M0/M1/M2 have
        sets_played=2 < best_of=3, M3 has sets_played=5 == best_of=5).
        wl_continuous_proxy = 2*y - 1 against that target.
        """
        cfg_str = """
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
target: deciding_set
model:
  type: xgboost
  params:
    n_estimators: 10
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 50
  test_size: 25
mtl:
  auxiliary_targets:
    - wl_continuous_proxy
"""
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(cfg_str)
        runner = ExperimentRunner(config_path=cfg_path, matches_path=empty_matches)
        df_out, target_cols = runner._resolve_target(self._h68_fixture())
        assert target_cols[0] == "_target_deciding_set"
        keyed = dict(
            zip(df_out["match_uid"].to_list(), df_out["_aux_wl_continuous_proxy"].to_list())
        )
        # M0, M1, M2: sets_played=2 < best_of=3 → deciding_set=0 → aux = -1
        # M3: sets_played=5 == best_of=5 → deciding_set=1 → aux = +1
        # M4 drops because partial-set issue isn't relevant here (no set-shape aux),
        # but sets_played=2 < best_of=3 → deciding_set=0 → aux = -1
        assert keyed["M0"] == pytest.approx(-1.0)
        assert keyed["M1"] == pytest.approx(-1.0)
        assert keyed["M2"] == pytest.approx(-1.0)
        assert keyed["M3"] == pytest.approx(1.0)
        assert keyed["M4"] == pytest.approx(-1.0)

    def test_all_aux_at_once_drops_only_rows_with_any_null(
        self, tmp_path: Path, empty_matches: Path
    ):
        """When ALL aux are configured, M1 drops (bp null) and M4 drops
        (set2 null). M0/M2/M3 survive."""
        df_out, target_cols = self._run(tmp_path, empty_matches, ALL_AUX)
        kept = sorted(df_out["match_uid"].to_list())
        assert kept == ["M0", "M2", "M3"]
        # target_cols includes primary + all 14 aux
        assert target_cols[0] == "won"
        assert len(target_cols) == 1 + len(ALL_AUX)

    def test_aux_column_types_are_float64_for_ratios(
        self, tmp_path: Path, empty_matches: Path
    ):
        """Ratio aux must be Float64 (XGBoost custom obj requires numeric targets)."""
        df_out, _ = self._run(
            tmp_path,
            empty_matches,
            [
                "service_pts_won_pct_diff",
                "first_serve_won_pct_diff",
                "bp_save_pct_diff",
                "set1_games_diff",
                "duration_seconds",
                "wl_continuous_proxy",
            ],
        )
        for col in [
            "_aux_service_pts_won_pct_diff",
            "_aux_first_serve_won_pct_diff",
            "_aux_bp_save_pct_diff",
            "_aux_set1_games_diff",
            "_aux_duration_seconds",
            "_aux_wl_continuous_proxy",
        ]:
            assert df_out.schema[col] == pl.Float64, f"{col} is not Float64"
