"""Tests for MTL auxiliary-target derivation in ExperimentRunner._resolve_target."""

import importlib
from pathlib import Path

import polars as pl
import pytest

from mvp.model.runner import ExperimentRunner


class TestResolveTargetMTL:
    """Tests for _resolve_target's MTL path (auxiliary target derivation +
    completeness filter)."""

    @pytest.fixture(autouse=True)
    def ensure_features_registered(self, isolated_registry):
        """Reload feature modules so the registry is populated for runner init."""
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
        """Minimal matches.parquet used only to satisfy runner __init__."""
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

    def _make_config(self, tmp_path: Path, with_mtl: bool, aux: list[str] | None = None) -> Path:
        """Write a config file with or without an MTL block.

        Uses `model.type: xgboost` even though these tests only exercise the
        data-layer `_resolve_target` path — the Step C config validator
        rejects MTL with non-xgboost model.type, and we want both with_mtl
        and without_mtl variants to share the same model.type so the only
        diff is the mtl block.
        """
        mtl_block = ""
        if with_mtl:
            aux_targets = aux if aux is not None else ["game_margin", "set_margin", "set_count"]
            yaml_aux = "\n".join(f"    - {t}" for t in aux_targets)
            mtl_block = f"\nmtl:\n  auxiliary_targets:\n{yaml_aux}"
        config_str = f"""
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
  test_size: 25{mtl_block}
"""
        path = tmp_path / "config.yaml"
        path.write_text(config_str)
        return path

    def _synthetic_matches_df(self) -> pl.DataFrame:
        """Construct a small df spanning the cases we want to test.

        Rows:
          0: 6-4 6-2 win (BO3, completed) — game_margin=+6, set_margin=+2, count=2
          1: 4-6 6-4 7-5 win (BO3, completed) — game_margin=+3, set_margin=+1, count=3
          2: 6-7 4-6 loss (BO3, completed) — game_margin=-5, set_margin=-2, count=2
          3: W/O — should be filtered in both single-task and MTL
          4: RET, partial scores — kept in single-task `won` path, dropped in MTL
          5: DEF — kept in single-task `won` path, dropped in MTL
          6: completed but sets_played null — has scores but no top-level
             sets_played; MTL filters it via the sets_played not-null gate
          7: BO5 win 6-4 6-4 6-4 — game_margin=+6, set_margin=+3, count=3
        """
        return pl.DataFrame(
            {
                "match_uid": [f"M{i}" for i in range(8)],
                "won": [True, True, False, True, True, False, True, True],
                "reason": [None, None, None, "W/O", "RET", "DEF", None, None],
                "sets_played": [2, 3, 2, None, 1, None, None, 3],
                "best_of": [3, 3, 3, 3, 3, 3, 3, 5],
                "player_set1_games": [6, 4, 6, None, 6, None, 6, 6],
                "player_set2_games": [6, 6, 4, None, None, None, 6, 6],
                "player_set3_games": [None, 7, None, None, None, None, None, 6],
                "player_set4_games": [None, None, None, None, None, None, None, None],
                "player_set5_games": [None, None, None, None, None, None, None, None],
                "opp_set1_games": [4, 6, 7, None, 4, None, 4, 4],
                "opp_set2_games": [2, 4, 6, None, None, None, 2, 4],
                "opp_set3_games": [None, 5, None, None, None, None, None, 4],
                "opp_set4_games": [None, None, None, None, None, None, None, None],
                "opp_set5_games": [None, None, None, None, None, None, None, None],
                "effective_match_date": ["2024-01-01"] * 8,
            }
        ).with_columns(pl.col("effective_match_date").str.to_datetime())

    def test_non_mtl_returns_single_element_list_unchanged_behavior(
        self, empty_matches: Path, tmp_path: Path
    ):
        """Without an mtl block, _resolve_target returns [primary_col]; only
        walkovers are excluded (today's behavior preserved)."""
        cfg = self._make_config(tmp_path, with_mtl=False)
        runner = ExperimentRunner(config_path=cfg, matches_path=empty_matches)
        df_out, target_cols = runner._resolve_target(self._synthetic_matches_df())

        assert target_cols == ["won"]
        # Only walkover (row 3) excluded; everything else (incl. RET/DEF/null
        # sets_played) kept under today's single-target `won` behavior.
        assert df_out.height == 7

    def test_mtl_appends_aux_columns_and_target_names(
        self, empty_matches: Path, tmp_path: Path
    ):
        """MTL path derives _aux_* columns and appends them to target_cols."""
        cfg = self._make_config(tmp_path, with_mtl=True)
        runner = ExperimentRunner(config_path=cfg, matches_path=empty_matches)
        df_out, target_cols = runner._resolve_target(self._synthetic_matches_df())

        assert target_cols == [
            "won",
            "_aux_game_margin",
            "_aux_set_margin",
            "_aux_set_count",
        ]
        for col in ["_aux_game_margin", "_aux_set_margin", "_aux_set_count"]:
            assert col in df_out.columns

    def test_mtl_aux_values_correct(
        self, empty_matches: Path, tmp_path: Path
    ):
        """Aux target derivations produce expected values on completed rows."""
        cfg = self._make_config(tmp_path, with_mtl=True)
        runner = ExperimentRunner(config_path=cfg, matches_path=empty_matches)
        df_out, _ = runner._resolve_target(self._synthetic_matches_df())

        # After MTL filtering: rows 0, 1, 2, 7 survive (W/O, RET, DEF, null
        # sets_played all dropped).
        kept_uids = df_out["match_uid"].to_list()
        assert kept_uids == ["M0", "M1", "M2", "M7"]

        # Verify aux values
        gm = df_out["_aux_game_margin"].to_list()
        sm = df_out["_aux_set_margin"].to_list()
        sc = df_out["_aux_set_count"].to_list()
        assert gm == [+6, +2, -3, +6]  # 6-4 6-2; 4-6 6-4 7-5; 6-7 4-6; 6-4 6-4 6-4
        assert sm == [+2, +1, -2, +3]
        assert sc == [2, 3, 2, 3]

    def test_mtl_filters_all_invalid_reasons(
        self, empty_matches: Path, tmp_path: Path
    ):
        """MTL path excludes W/O, RET, DEF, UNP rows (vs single-task only W/O)."""
        cfg = self._make_config(tmp_path, with_mtl=True)
        runner = ExperimentRunner(config_path=cfg, matches_path=empty_matches)
        df_out, _ = runner._resolve_target(self._synthetic_matches_df())

        # No row with any of W/O, RET, DEF should survive.
        kept_reasons = df_out["reason"].drop_nulls().to_list()
        for r in {"W/O", "RET", "DEF", "UNP"}:
            assert r not in kept_reasons

    def test_mtl_drops_rows_with_null_sets_played(
        self, empty_matches: Path, tmp_path: Path
    ):
        """Even when reason is null/unrecognized, sets_played-null rows are
        excluded under MTL — the explicit sets_played-not-null gate catches
        them."""
        cfg = self._make_config(tmp_path, with_mtl=True)
        runner = ExperimentRunner(config_path=cfg, matches_path=empty_matches)
        df_out, _ = runner._resolve_target(self._synthetic_matches_df())

        # Row M6 has reason=None and sets_played=None — must be excluded.
        assert "M6" not in df_out["match_uid"].to_list()

    def test_mtl_subset_aux_targets(
        self, empty_matches: Path, tmp_path: Path
    ):
        """If only a subset of aux targets is configured, only those are
        materialized and only those appear in target_cols."""
        cfg = self._make_config(
            tmp_path, with_mtl=True, aux=["set_margin"]
        )
        runner = ExperimentRunner(config_path=cfg, matches_path=empty_matches)
        df_out, target_cols = runner._resolve_target(self._synthetic_matches_df())

        assert target_cols == ["won", "_aux_set_margin"]
        assert "_aux_set_margin" in df_out.columns
        # Other aux columns should NOT be on the df.
        assert "_aux_game_margin" not in df_out.columns
        assert "_aux_set_count" not in df_out.columns

    def test_mtl_config_rejects_empty_aux_list(self, tmp_path: Path):
        """MTLConfig validator requires at least one auxiliary target."""
        from mvp.model.config import ExperimentConfig

        config_str = """
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: logistic
mtl:
  auxiliary_targets: []
"""
        with pytest.raises(ValueError, match="at least one target"):
            ExperimentConfig.from_yaml(config_str)

    def test_mtl_config_rejects_duplicate_aux(self, tmp_path: Path):
        """MTLConfig validator rejects duplicates in auxiliary_targets."""
        from mvp.model.config import ExperimentConfig

        config_str = """
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: logistic
mtl:
  auxiliary_targets:
    - game_margin
    - game_margin
"""
        with pytest.raises(ValueError, match="duplicates"):
            ExperimentConfig.from_yaml(config_str)

    def test_mtl_with_deciding_set_target(
        self, empty_matches: Path, tmp_path: Path
    ):
        """MTL combined with `target: deciding_set` derives both the primary
        deciding_set target AND the auxiliary regression targets. The
        deciding_set extra-RET-filter branch is dead under MTL (already filtered
        upstream), so the primary derivation should still match the simple
        `sets_played == best_of` rule on the surviving rows."""
        config_str = """
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
target: deciding_set
mtl:
  auxiliary_targets:
    - game_margin
    - set_margin
    - set_count
"""
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(config_str)

        runner = ExperimentRunner(config_path=cfg_path, matches_path=empty_matches)
        df_out, target_cols = runner._resolve_target(self._synthetic_matches_df())

        assert target_cols == [
            "_target_deciding_set",
            "_aux_game_margin",
            "_aux_set_margin",
            "_aux_set_count",
        ]
        # Same rows survive as in the `won` + MTL case: M0, M1, M2, M7.
        kept_uids = df_out["match_uid"].to_list()
        assert kept_uids == ["M0", "M1", "M2", "M7"]
        # _target_deciding_set is 1 iff sets_played == best_of:
        #   M0 sets=2 bo=3 → 0; M1 sets=3 bo=3 → 1; M2 sets=2 bo=3 → 0;
        #   M7 sets=3 bo=5 → 0
        assert df_out["_target_deciding_set"].to_list() == [0, 1, 0, 0]
        # Aux values unchanged from the `won` + MTL case (same surviving rows).
        assert df_out["_aux_game_margin"].to_list() == [+6, +2, -3, +6]
        assert df_out["_aux_set_margin"].to_list() == [+2, +1, -2, +3]
        assert df_out["_aux_set_count"].to_list() == [2, 3, 2, 3]
