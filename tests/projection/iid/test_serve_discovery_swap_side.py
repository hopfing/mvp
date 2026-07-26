"""Swap-side (B-serves) column materialization for the chain-metric serve FS.

`ScoreStateChainServeModel.predict_state_fn` reads `opp_X` for mirror features and
negates `player_X` for diff features. The FS selector builds the match-grain frame
those reads happen on, so it has to materialize the `opp_X` columns — and it can
only do so on a two-sided frame, because the engine derives `opp_X` by self-joining
the partner row.
"""

from datetime import date, timedelta
from textwrap import dedent

import polars as pl
import pytest

from mvp.projection.iid.serve_discovery import ServeDiscoverySelector
from mvp.projection.iid.serve_model import (
    resolve_match_feature_cols,
    swap_side_opp_specs,
)

# Registered features standing in for each swap mechanism:
MIRROR_SPEC = "player_tourn_pts_service_won_per_game"      # @feature mirror=True
MATCHUP_SPEC = "player_svc_elo_matchup"                    # register_matchup -> mirror=True
DIFF_SPEC = "player_tourn_pts_service_won_per_game_diff"   # register_diff -> mirror=False
SUM_SPEC = "total_games_sum(days=365)"                     # register_sum -> match-level


class TestSwapSideClassification:
    def test_mirror_and_matchup_need_opp_columns(self):
        assert swap_side_opp_specs([MIRROR_SPEC]) == [
            "opp_tourn_pts_service_won_per_game"
        ]
        # A cross-domain matchup is opp_svc_elo - player_ret_elo at the swap side,
        # which is NOT the negation of the player_ value.
        assert swap_side_opp_specs([MATCHUP_SPEC]) == ["opp_svc_elo_matchup"]

    def test_diff_and_match_level_need_no_opp_column(self):
        assert swap_side_opp_specs([DIFF_SPEC, SUM_SPEC]) == []

    def test_params_are_carried_onto_the_opp_spec(self):
        assert swap_side_opp_specs(["player_surface_games_margin_per_set(days=730)"]) == [
            "opp_surface_games_margin_per_set(days=730)"
        ]

    def test_flags_align_with_resolved_columns(self):
        specs = [MIRROR_SPEC, DIFF_SPEC, SUM_SPEC, MATCHUP_SPEC]
        cols, is_diff = resolve_match_feature_cols(specs)
        assert cols == [
            "server_tourn_pts_service_won_per_game",
            "server_tourn_pts_service_won_per_game_diff",
            "total_games_sum_365d",
            "server_svc_elo_matchup",
        ]
        assert is_diff == [False, True, False, False]


class MirroringFakeEngine:
    """FeatureEngine stand-in reproducing the real opp_ derivation.

    `player_X` comes from cache (always available). `opp_X` is derived by joining
    the partner row *within the frame passed in* — mirroring
    `FeatureEngine._mirror_features`, so a deduplicated frame yields all-null
    opp_ columns exactly as it does in production.
    """

    def __init__(self) -> None:
        self.requested: list[list[str]] = []

    def load_features_numpy(
        self, feature_specs: list[str], base_df: pl.DataFrame, cache_key: str,
    ) -> pl.DataFrame:
        self.requested.append(list(feature_specs))
        out = base_df
        for spec in feature_specs:
            if not spec.startswith("player_"):
                continue
            if spec not in out.columns:
                out = out.with_columns(
                    (pl.col("player_id").cast(pl.Float64) / 100.0).alias(spec)
                )
        for spec in feature_specs:
            if not spec.startswith("opp_"):
                continue
            player_col = "player_" + spec[len("opp_"):]
            if player_col not in out.columns:
                out = out.with_columns(
                    (pl.col("player_id").cast(pl.Float64) / 100.0).alias(player_col)
                )
            lookup = out.select(["match_uid", "player_id", player_col]).rename(
                {"player_id": "_lookup_id", player_col: spec}
            )
            out = out.join(
                lookup,
                left_on=["match_uid", "opp_id"],
                right_on=["match_uid", "_lookup_id"],
                how="left",
            )
            if "_lookup_id" in out.columns:
                out = out.drop("_lookup_id")
        return out


@pytest.fixture
def selector(tmp_path):
    """Selector wired to tiny two-sided matches / points parquets."""
    n_matches = 40
    rows = []
    for i in range(n_matches):
        a, b = 1000 + i, 2000 + i
        for player_id, opp_id in ((a, b), (b, a)):
            rows.append({
                "match_uid": f"m{i:03d}",
                "player_id": player_id,
                "opp_id": opp_id,
                "best_of": 3,
                "won": player_id == a,
                "effective_match_date": date(2024, 1, 1) + timedelta(days=i),
                "reason": None,
                "surface": "hard",
                "circuit": "tour",
                "draw_type": "singles",
                "player_set1_games": 6, "player_set2_games": 4, "player_set3_games": 6,
                "player_set4_games": None, "player_set5_games": None,
                "opp_set1_games": 4, "opp_set2_games": 6, "opp_set3_games": 3,
                "opp_set4_games": None, "opp_set5_games": None,
            })
    matches_path = tmp_path / "matches.parquet"
    pl.DataFrame(rows).write_parquet(matches_path)

    points_path = tmp_path / "points.parquet"
    pl.DataFrame({
        "match_uid": [f"m{i:03d}" for i in range(n_matches)],
        "point_won_by_server": [1] * n_matches,
    }).write_parquet(points_path)

    config_path = tmp_path / "fs.yaml"
    config_path.write_text(dedent("""
        data:
          date_range:
            start: 2024-01-01
            end: 2024-12-31
          filters:
            circuit: [tour]
            draw_type: singles
        validation:
          type: walk_forward
          n_splits: 2
          min_train_size: 10
          test_size: 5
        metric: iid_crps_total_games
        features:
          candidate_point_level_features: [is_break_point]
    """))

    return ServeDiscoverySelector(
        config_path=config_path,
        matches_path=matches_path,
        points_path=points_path,
        cache_dir=tmp_path / "cache",
    )


class TestPrepareMatchData:
    def test_opp_columns_are_populated_on_the_match_grain_frame(self, selector):
        pool = [MIRROR_SPEC, MATCHUP_SPEC, DIFF_SPEC]
        engine = MirroringFakeEngine()

        selector._prepare_match_data(match_pool=pool, engine=engine, cache_key="k")

        match_df = selector._match_df
        assert match_df is not None
        # One row per match — this is the frame predict_state_fn runs on.
        assert match_df["match_uid"].n_unique() == len(match_df)
        for opp_col in ("opp_tourn_pts_service_won_per_game", "opp_svc_elo_matchup"):
            assert opp_col in match_df.columns
            assert match_df[opp_col].null_count() == 0
        # The diff's swap side is the negated player_ value; no opp_ read involved.
        assert "opp_tourn_pts_service_won_per_game_diff" not in match_df.columns

    def test_opp_value_is_the_opponents_value(self, selector):
        engine = MirroringFakeEngine()
        selector._prepare_match_data(
            match_pool=[MIRROR_SPEC], engine=engine, cache_key="k",
        )
        match_df = selector._match_df
        # The fake engine sets player_X = player_id / 100, so opp_X must equal
        # opp_id / 100 — i.e. it came from the partner row, not the same row.
        expected = match_df["opp_id"].cast(pl.Float64) / 100.0
        assert match_df["opp_tourn_pts_service_won_per_game"].to_list() == expected.to_list()

    def test_features_are_loaded_on_the_two_sided_frame(self, selector):
        engine = MirroringFakeEngine()
        selector._prepare_match_data(
            match_pool=[MIRROR_SPEC], engine=engine, cache_key="k",
        )
        both = selector._match_features_both_sides
        assert both is not None
        # Two rows per match: the partner row is what makes the opp_ join resolve.
        assert len(both) == 2 * len(selector._match_df)
        assert "opp_tourn_pts_service_won_per_game" in engine.requested[-1]
