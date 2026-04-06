"""Tests for score-depth feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import score_depth as score_depth_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _make_score_df() -> pl.DataFrame:
    """4 matches for player A with varying scores.

    Match 1: Won 6-3 6-4 (bo3, straight sets, 2 sets, gw=12 gl=7, gw/set=6.0 gl/set=3.5)
    Match 2: Won 7-6 3-6 6-3 (bo3, NOT straight sets, 3 sets, gw=16 gl=15, gw/set=5.33 gl/set=5.0)
    Match 3: Lost 4-6 6-3 3-6 (bo3, NOT straight, 3 sets, gw=13 gl=15, gw/set=4.33 gl/set=5.0)
    Match 4: Won 6-1 6-2 (bo3, straight sets, 2 sets, gw=12 gl=3, gw/set=6.0 gl/set=1.5)
    """
    return pl.DataFrame({
        "player_id": ["A", "A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
            date(2024, 4, 1),
        ],
        "won": [1, 1, 0, 1],
        "sets_played": [2, 3, 3, 2],
        "best_of": [3, 3, 3, 3],
        "player_set1_games": [6, 7, 4, 6],
        "opp_set1_games": [3, 6, 6, 1],
        "player_set2_games": [6, 3, 6, 6],
        "opp_set2_games": [4, 6, 3, 2],
        "player_set3_games": [None, 6, 3, None],
        "opp_set3_games": [None, 3, 6, None],
        "player_set4_games": [None, None, None, None],
        "opp_set4_games": [None, None, None, None],
        "player_set5_games": [None, None, None, None],
        "opp_set5_games": [None, None, None, None],
    }).sort("effective_match_date")


class TestScoreDepthBaseFeatures:
    """Tests for base score-depth features."""

    def test_all_base_registered(self):
        registry = get_registry()
        base_names = [
            "sets_per_match", "straight_sets_win_pct",
            "games_won_per_set", "games_lost_per_set",
            "games_margin_per_set", "games_per_set",
            "total_games_won", "total_games_lost", "total_games",
        ]
        for name in base_names:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == ["days"]

    def test_sets_per_match_rolling(self):
        from mvp.model.features.score_depth import sets_per_match

        df = _make_score_df()
        result = df.with_columns(sets_per_match(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior=[2] -> 2.0
        assert result["val"][1] == pytest.approx(2.0)
        # Row 2: prior=[2, 3] -> 2.5
        assert result["val"][2] == pytest.approx(2.5)
        # Row 3: prior=[2, 3, 3] -> 2.667
        assert result["val"][3] == pytest.approx(8 / 3, abs=0.01)

    def test_straight_sets_win_pct_rolling(self):
        from mvp.model.features.score_depth import straight_sets_win_pct

        df = _make_score_df()
        result = df.with_columns(straight_sets_win_pct(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior wins=[match1 ss_win=1, total_wins=1] -> 1/1 = 1.0
        assert result["val"][1] == pytest.approx(1.0)
        # Row 2: prior wins=[match1 ss=1, match2 ss=0, total_wins=2] -> 1/2 = 0.5
        assert result["val"][2] == pytest.approx(0.5)
        # Row 3: prior wins=[match1 ss=1, match2 ss=0, match3 lost so won=0, total_wins=2] -> 1/2 = 0.5
        assert result["val"][3] == pytest.approx(0.5)

    def test_games_won_per_set_rolling(self):
        from mvp.model.features.score_depth import games_won_per_set

        df = _make_score_df()
        result = df.with_columns(games_won_per_set(days=365).alias("val"))
        # Row 0: null
        assert result["val"][0] is None
        # Row 1: prior=[12/2=6.0] -> 6.0
        assert result["val"][1] == pytest.approx(6.0)
        # Row 2: prior=[6.0, 16/3=5.333] -> mean = 5.667
        assert result["val"][2] == pytest.approx((6.0 + 16 / 3) / 2, abs=0.01)

    def test_games_lost_per_set_rolling(self):
        from mvp.model.features.score_depth import games_lost_per_set

        df = _make_score_df()
        result = df.with_columns(games_lost_per_set(days=365).alias("val"))
        # Row 0: null
        assert result["val"][0] is None
        # Row 1: prior=[7/2=3.5] -> 3.5
        assert result["val"][1] == pytest.approx(3.5)

    def test_games_margin_per_set_rolling(self):
        from mvp.model.features.score_depth import games_margin_per_set

        df = _make_score_df()
        result = df.with_columns(games_margin_per_set(days=365).alias("val"))
        # Row 0: null
        assert result["val"][0] is None
        # Row 1: prior=[(12-7)/2=2.5] -> 2.5
        assert result["val"][1] == pytest.approx(2.5)
        # Row 2: prior=[2.5, (16-15)/3=0.333] -> mean = 1.417
        assert result["val"][2] == pytest.approx((2.5 + 1 / 3) / 2, abs=0.01)

    def test_games_per_set_rolling(self):
        from mvp.model.features.score_depth import games_per_set

        df = _make_score_df()
        result = df.with_columns(games_per_set(days=365).alias("val"))
        # Row 0: null
        assert result["val"][0] is None
        # Row 1: prior=[(12+7)/2=9.5] -> 9.5
        assert result["val"][1] == pytest.approx(9.5)

    def test_alltime_variant(self):
        from mvp.model.features.score_depth import sets_per_match

        df = _make_score_df()
        result = df.with_columns(sets_per_match(days=None).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 3: prior=[2,3,3] -> 8/3
        assert result["val"][3] == pytest.approx(8 / 3, abs=0.01)

    def test_total_games_won_rolling(self):
        from mvp.model.features.score_depth import total_games_won

        df = _make_score_df()
        result = df.with_columns(total_games_won(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior=[12] -> 12.0
        assert result["val"][1] == pytest.approx(12.0)
        # Row 2: prior=[12, 16] -> 14.0
        assert result["val"][2] == pytest.approx(14.0)
        # Row 3: prior=[12, 16, 13] -> 13.667
        assert result["val"][3] == pytest.approx(41 / 3, abs=0.01)

    def test_total_games_lost_rolling(self):
        from mvp.model.features.score_depth import total_games_lost

        df = _make_score_df()
        result = df.with_columns(total_games_lost(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior=[7] -> 7.0
        assert result["val"][1] == pytest.approx(7.0)
        # Row 2: prior=[7, 15] -> 11.0
        assert result["val"][2] == pytest.approx(11.0)

    def test_total_games_rolling(self):
        from mvp.model.features.score_depth import total_games

        df = _make_score_df()
        result = df.with_columns(total_games(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior=[12+7=19] -> 19.0
        assert result["val"][1] == pytest.approx(19.0)
        # Row 2: prior=[19, 16+15=31] -> 25.0
        assert result["val"][2] == pytest.approx(25.0)
        # Row 3: prior=[19, 31, 13+15=28] -> 26.0
        assert result["val"][3] == pytest.approx(78 / 3, abs=0.01)


class TestRecentGamesLoad:
    """Tests for recent_games_load feature."""

    def test_registered(self):
        registry = get_registry()
        feat = registry.get("recent_games_load")
        assert feat.params == ["days"]
        assert feat.mirror is True
        assert feat.impute == 0

    def test_rolling_sum(self):
        from mvp.model.features.score_depth import recent_games_load

        df = _make_score_df()
        result = df.with_columns(recent_games_load(days=365).alias("val"))
        # Row 0: no prior -> 0 (rolling_sum fills null with 0)
        assert result["val"][0] == pytest.approx(0.0)
        # Row 1: prior games = (12+7) = 19
        assert result["val"][1] == pytest.approx(19.0)
        # Row 2: prior games = 19 + (16+15) = 50
        assert result["val"][2] == pytest.approx(50.0)
        # Row 3: prior games = 19 + 31 + (13+15) = 78
        assert result["val"][3] == pytest.approx(78.0)


class TestScoreDepthDiffFeatures:
    """Tests for score-depth diff features."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        diff_names = [
            "sets_per_match_diff", "straight_sets_win_pct_diff",
            "games_won_per_set_diff", "games_lost_per_set_diff",
            "games_margin_per_set_diff", "games_per_set_diff",
            "total_games_won_diff", "total_games_lost_diff",
            "total_games_diff", "recent_games_load_diff",
        ]
        for name in diff_names:
            feat = registry.get(name)
            assert feat.mirror is False
            assert feat.impute == 0
            assert len(feat.depends_on) == 1

    def test_games_won_per_set_diff_computation(self):
        games_won_per_set_diff = get_registry().get("games_won_per_set_diff").func

        df = pl.DataFrame({
            "player_games_won_per_set_365d": [6.0, 5.0],
            "opp_games_won_per_set_365d": [4.5, 5.5],
        })
        result = df.with_columns(games_won_per_set_diff(days=365).alias("diff"))
        assert result["diff"][0] == pytest.approx(1.5)
        assert result["diff"][1] == pytest.approx(-0.5)

    def test_straight_sets_diff_alltime(self):
        straight_sets_win_pct_diff = get_registry().get("straight_sets_win_pct_diff").func

        df = pl.DataFrame({
            "player_straight_sets_win_pct": [0.6, 0.4],
            "opp_straight_sets_win_pct": [0.3, 0.7],
        })
        result = df.with_columns(straight_sets_win_pct_diff(days=None).alias("diff"))
        assert result["diff"][0] == pytest.approx(0.3)
        assert result["diff"][1] == pytest.approx(-0.3)


class TestScoreDepthFeatureCount:
    """Verify total feature count."""

    def test_total_count(self):
        registry = get_registry()
        sd_names = [
            # base (10)
            "sets_per_match", "straight_sets_win_pct",
            "games_won_per_set", "games_lost_per_set",
            "games_margin_per_set", "games_per_set",
            "total_games_won", "total_games_lost", "total_games",
            "recent_games_load",
            # diffs (10)
            "sets_per_match_diff", "straight_sets_win_pct_diff",
            "games_won_per_set_diff", "games_lost_per_set_diff",
            "games_margin_per_set_diff", "games_per_set_diff",
            "total_games_won_diff", "total_games_lost_diff",
            "total_games_diff", "recent_games_load_diff",
            # sums (3)
            "games_per_set_sum", "sets_per_match_sum", "total_games_sum",
        ]
        for name in sd_names:
            registry.get(name)  # Will raise KeyError if missing
        assert len(sd_names) == 23
