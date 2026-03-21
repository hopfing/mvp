"""Tests for opponent-quality-adjusted feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import quality as quality_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _make_quality_df() -> pl.DataFrame:
    """4 matches for player A with known opp_elo values.

    Match 1: Won vs opp_elo=1500
    Match 2: Lost vs opp_elo=1800
    Match 3: Won vs opp_elo=1600
    Match 4: Lost vs opp_elo=1400
    """
    return pl.DataFrame({
        "player_id": ["A", "A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
            date(2024, 4, 1),
        ],
        "won": [1, 0, 1, 0],
        "opp_elo": [1500.0, 1800.0, 1600.0, 1400.0],
    }).sort("effective_match_date")


class TestQualityBaseFeatures:
    """Tests for base quality features."""

    def test_all_base_registered(self):
        registry = get_registry()
        base_names = ["quality_win_rate", "opp_elo_beaten_avg", "opp_elo_faced_avg"]
        for name in base_names:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == ["days"]

    def test_quality_win_rate_rolling(self):
        from mvp.model.features.quality import quality_win_rate

        df = _make_quality_df()
        result = df.with_columns(quality_win_rate(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior=[won*1500/1500] = 1500/1500 = 1.0
        assert result["val"][1] == pytest.approx(1.0)
        # Row 2: prior=[won*1500 + lost*1800] / [1500+1800] = 1500/3300
        assert result["val"][2] == pytest.approx(1500 / 3300, abs=0.001)
        # Row 3: prior=[1500 + 0 + 1600] / [1500+1800+1600] = 3100/4900
        assert result["val"][3] == pytest.approx(3100 / 4900, abs=0.001)

    def test_opp_elo_beaten_avg_rolling(self):
        from mvp.model.features.quality import opp_elo_beaten_avg

        df = _make_quality_df()
        result = df.with_columns(opp_elo_beaten_avg(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior beaten=[1500], count=1 -> 1500/1 = 1500
        assert result["val"][1] == pytest.approx(1500.0)
        # Row 2: prior beaten=[1500, 0(lost)], count=[1,0] -> 1500/1 = 1500
        assert result["val"][2] == pytest.approx(1500.0)
        # Row 3: prior beaten=[1500, 0, 1600], count=[1,0,1] -> 3100/2 = 1550
        assert result["val"][3] == pytest.approx(1550.0)

    def test_opp_elo_faced_avg_rolling(self):
        from mvp.model.features.quality import opp_elo_faced_avg

        df = _make_quality_df()
        result = df.with_columns(opp_elo_faced_avg(days=365).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior=[1500] -> 1500
        assert result["val"][1] == pytest.approx(1500.0)
        # Row 2: prior=[1500, 1800] -> 1650
        assert result["val"][2] == pytest.approx(1650.0)
        # Row 3: prior=[1500, 1800, 1600] -> 1633.33
        assert result["val"][3] == pytest.approx((1500 + 1800 + 1600) / 3, abs=0.1)

    def test_alltime_quality_win_rate(self):
        from mvp.model.features.quality import quality_win_rate

        df = _make_quality_df()
        result = df.with_columns(quality_win_rate(days=None).alias("val"))
        # Row 0: null
        assert result["val"][0] is None
        # Row 3: same as rolling 365 since all within 1 year
        assert result["val"][3] == pytest.approx(3100 / 4900, abs=0.001)

    def test_multiple_players_independent(self):
        from mvp.model.features.quality import opp_elo_faced_avg

        df = pl.DataFrame({
            "player_id": ["A", "A", "B", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 1, 1),
                date(2024, 2, 1),
            ],
            "won": [1, 0, 0, 1],
            "opp_elo": [1500.0, 1800.0, 1200.0, 1300.0],
        }).sort("effective_match_date")

        result = df.with_columns(opp_elo_faced_avg(days=365).alias("val"))
        a_rows = result.filter(pl.col("player_id") == "A")
        b_rows = result.filter(pl.col("player_id") == "B")
        assert a_rows["val"][1] == pytest.approx(1500.0)
        assert b_rows["val"][1] == pytest.approx(1200.0)


class TestQualityDiffFeatures:
    """Tests for quality diff features."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        diff_names = [
            "quality_win_rate_diff",
            "opp_elo_beaten_avg_diff",
            "opp_elo_faced_avg_diff",
        ]
        for name in diff_names:
            feat = registry.get(name)
            assert feat.mirror is False
            assert feat.impute == 0
            assert len(feat.depends_on) == 1

    def test_quality_win_rate_diff_computation(self):
        quality_win_rate_diff = get_registry().get("quality_win_rate_diff").func

        df = pl.DataFrame({
            "player_quality_win_rate_365d": [0.6, 0.4],
            "opp_quality_win_rate_365d": [0.3, 0.5],
        })
        result = df.with_columns(quality_win_rate_diff(days=365).alias("diff"))
        assert result["diff"][0] == pytest.approx(0.3)
        assert result["diff"][1] == pytest.approx(-0.1)

    def test_opp_elo_faced_avg_diff_alltime(self):
        opp_elo_faced_avg_diff = get_registry().get("opp_elo_faced_avg_diff").func

        df = pl.DataFrame({
            "player_opp_elo_faced_avg": [1600.0, 1500.0],
            "opp_opp_elo_faced_avg": [1400.0, 1700.0],
        })
        result = df.with_columns(opp_elo_faced_avg_diff(days=None).alias("diff"))
        assert result["diff"][0] == pytest.approx(200.0)
        assert result["diff"][1] == pytest.approx(-200.0)


class TestQualityFeatureCount:
    """Verify total feature count."""

    def test_total_count(self):
        registry = get_registry()
        q_names = [
            "quality_win_rate", "opp_elo_beaten_avg", "opp_elo_faced_avg",
            "quality_win_rate_diff", "opp_elo_beaten_avg_diff", "opp_elo_faced_avg_diff",
        ]
        for name in q_names:
            registry.get(name)  # Will raise KeyError if missing
        assert len(q_names) == 6
