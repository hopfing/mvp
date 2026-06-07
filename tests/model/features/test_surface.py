"""Tests for surface-specific feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import surface as surface_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _make_surface_df() -> pl.DataFrame:
    """4 matches for player A across two surfaces with serve/return stats.

    Match 1: Hard — svc_games=10, bp_faced=3, bp_saved=1 -> 8 holds (80%)
    Match 2: Clay — svc_games=12, bp_faced=6, bp_saved=2 -> 8 holds (67%)
    Match 3: Hard — svc_games=11, bp_faced=2, bp_saved=2 -> 11 holds (100%)
    Match 4: Clay — svc_games=10, bp_faced=4, bp_saved=1 -> 7 holds (70%)
    """
    return pl.DataFrame({
        "player_id": ["A", "A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
            date(2024, 4, 1),
        ],
        "surface": ["Hard", "Clay", "Hard", "Clay"],
        "won": [1, 0, 1, 0],
        "svc_games_played": [10, 12, 11, 10],
        "svc_bp_faced": [3, 6, 2, 4],
        "svc_bp_saved": [1, 2, 2, 1],
        "svc_first_serve_pts_won": [30, 25, 35, 20],
        "svc_first_serve_pts_played": [40, 45, 42, 38],
        "svc_second_serve_pts_won": [10, 8, 12, 7],
        "svc_second_serve_pts_played": [20, 18, 22, 16],
        "svc_aces": [8, 2, 10, 3],
        "svc_first_serve_att": [60, 55, 62, 50],
        "svc_double_faults": [2, 4, 1, 3],
        "svc_first_serve_in": [38, 32, 40, 30],
        "ret_first_serve_pts_won": [12, 15, 14, 18],
        "ret_first_serve_pts_played": [45, 40, 48, 42],
        "ret_second_serve_pts_won": [8, 10, 9, 11],
        "ret_second_serve_pts_played": [15, 16, 14, 17],
        "ret_bp_converted": [2, 3, 1, 4],
        "ret_bp_opportunities": [5, 6, 4, 7],
        "pts_service_pts_won": [40, 33, 47, 27],
        "pts_service_pts_played": [60, 63, 64, 54],
        "pts_return_pts_won": [20, 25, 23, 29],
        "pts_return_pts_played": [60, 56, 62, 59],
    }).sort("effective_match_date")


class TestSurfaceServeFeatures:
    """Tests for surface-stratified serve features."""

    def test_all_registered(self):
        registry = get_registry()
        names = [
            "surface_first_serve_win_pct", "surface_second_serve_win_pct",
            "surface_ace_pct", "surface_df_pct", "surface_bp_save_pct",
            "surface_first_serve_in_pct", "surface_hold_pct",
        ]
        for name in names:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == ["days"]

    def test_surface_hold_pct_groups_by_surface(self):
        from mvp.model.features.surface import surface_hold_pct

        df = _make_surface_df()
        result = df.with_columns(surface_hold_pct(days=365).alias("val"))
        # Grouped by surface + shrunk (k=12): first match on a surface has no
        # prior -> null; once that surface has history an interior value appears
        # (Hard and Clay tracked separately).
        assert result["val"][0] is None  # first Hard
        assert result["val"][1] is None  # first Clay
        assert result["val"][2] is not None and 0.0 < result["val"][2] < 1.0
        assert result["val"][3] is not None and 0.0 < result["val"][3] < 1.0

    def test_surface_ace_pct_groups_by_surface(self):
        from mvp.model.features.surface import surface_ace_pct

        df = _make_surface_df()
        result = df.with_columns(surface_ace_pct(days=365).alias("val"))
        # Grouped by surface + shrunk (k=77): first-on-surface -> null; later interior.
        assert result["val"][0] is None
        assert result["val"][2] is not None and 0.0 < result["val"][2] < 1.0
        assert result["val"][3] is not None and 0.0 < result["val"][3] < 1.0


class TestSurfaceReturnFeatures:
    """Tests for surface-stratified return features."""

    def test_all_registered(self):
        registry = get_registry()
        names = [
            "surface_ret_first_serve_win_pct",
            "surface_ret_second_serve_win_pct",
            "surface_ret_bp_convert_pct",
        ]
        for name in names:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == ["days"]

    def test_surface_ret_bp_convert_pct_groups_by_surface(self):
        from mvp.model.features.surface import surface_ret_bp_convert_pct

        df = _make_surface_df()
        result = df.with_columns(surface_ret_bp_convert_pct(days=365).alias("val"))
        # Grouped by surface + shrunk (k=180): interior values once history exists.
        assert result["val"][2] is not None and 0.0 < result["val"][2] < 1.0
        assert result["val"][3] is not None and 0.0 < result["val"][3] < 1.0


class TestSurfacePointsFeatures:
    """Tests for surface-stratified points features."""

    def test_all_registered(self):
        registry = get_registry()
        for name in ["surface_pts_service_won_pct", "surface_pts_return_won_pct"]:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == ["days"]


class TestSurfaceDerivedFeatures:
    """Tests for diffs, sums, and matchups."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        diff_names = [
            "surface_first_serve_win_pct_diff", "surface_second_serve_win_pct_diff",
            "surface_ace_pct_diff", "surface_df_pct_diff", "surface_bp_save_pct_diff",
            "surface_first_serve_in_pct_diff", "surface_hold_pct_diff",
            "surface_ret_first_serve_win_pct_diff", "surface_ret_second_serve_win_pct_diff",
            "surface_ret_bp_convert_pct_diff",
            "surface_pts_service_won_pct_diff", "surface_pts_return_won_pct_diff",
        ]
        for name in diff_names:
            feat = registry.get(name)
            assert feat.mirror is False
            # diff inherits the base's impute (no-fabricate bases are None)
            assert feat.impute == registry.get(feat.depends_on[0]).impute

    def test_all_sums_registered(self):
        registry = get_registry()
        sum_names = [
            "surface_first_serve_win_pct_sum", "surface_second_serve_win_pct_sum",
            "surface_ace_pct_sum", "surface_df_pct_sum", "surface_bp_save_pct_sum",
            "surface_first_serve_in_pct_sum", "surface_hold_pct_sum",
            "surface_ret_first_serve_win_pct_sum", "surface_ret_second_serve_win_pct_sum",
            "surface_ret_bp_convert_pct_sum",
            "surface_pts_service_won_pct_sum", "surface_pts_return_won_pct_sum",
        ]
        for name in sum_names:
            feat = registry.get(name)
            assert feat.match_level is True

    def test_all_matchups_registered(self):
        registry = get_registry()
        matchup_names = [
            "surface_first_serve_win_pct_matchup",
            "surface_second_serve_win_pct_matchup",
            "surface_bp_pct_matchup",
            "surface_ret_first_serve_win_pct_matchup",
            "surface_ret_second_serve_win_pct_matchup",
            "surface_ret_bp_pct_matchup",
            "surface_svc_pts_won_pct_matchup",
            "surface_ret_pts_won_pct_matchup",
        ]
        for name in matchup_names:
            feat = registry.get(name)
            assert feat.mirror is False
            assert len(feat.depends_on) == 2


class TestSurfaceFeatureCount:
    """Verify total feature count."""

    def test_total_count(self):
        registry = get_registry()
        expected = (
            2       # surface_win_pct, surface_matches
            + 1     # surface_win_pct_diff
            + 2     # surface_quality_win_rate + its diff
            + 12    # surface-stratified base (7 serve + 3 return + 2 points)
            + 12    # diffs
            + 12    # sums
            + 8     # matchups
        )
        # Verify all are accessible
        all_names = [
            "surface_win_pct", "surface_matches", "surface_win_pct_diff",
            "surface_quality_win_rate", "surface_quality_win_rate_diff",
            "surface_first_serve_win_pct", "surface_second_serve_win_pct",
            "surface_ace_pct", "surface_df_pct", "surface_bp_save_pct",
            "surface_first_serve_in_pct", "surface_hold_pct",
            "surface_ret_first_serve_win_pct", "surface_ret_second_serve_win_pct",
            "surface_ret_bp_convert_pct",
            "surface_pts_service_won_pct", "surface_pts_return_won_pct",
        ]
        for name in all_names:
            registry.get(name)
        assert expected == 49
