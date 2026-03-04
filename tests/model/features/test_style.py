"""Tests for playing style feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import style as style_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


def _make_style_df(overrides: dict | None = None) -> pl.LazyFrame:
    """Base test DataFrame with match_beats columns for style features.

    4 matches for player A on different dates, with match_beats data.
    """
    data = {
        "player_id": ["A", "A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 3, 1),
            date(2024, 6, 1),
            date(2024, 9, 1),
        ],
        "mb_player_avg_1st_serve_speed": [200.0, 195.0, 205.0, 198.0],
        "mb_player_max_1st_serve_speed": [220.0, 215.0, 230.0, 225.0],
        "mb_player_avg_2nd_serve_speed": [150.0, 148.0, 155.0, 152.0],
        "mb_player_max_2nd_serve_speed": [165.0, 160.0, 170.0, 168.0],
        "mb_player_std_1st_serve_speed": [8.0, 6.0, 10.0, 7.0],
        "mb_player_winners": [20, 25, 18, 22],
        "mb_player_ues": [15, 10, 12, 14],
        "mb_opp_fes": [8, 12, 6, 10],
        "mb_player_easy_holds": [5, 6, 4, 5],
        "mb_player_difficult_holds": [2, 1, 3, 2],
        "mb_player_service_games": [10, 11, 9, 10],
        "mb_player_crucial_points_won": [8, 10, 7, 9],
        "mb_player_crucial_points_played": [15, 18, 12, 16],
        "mb_player_rally_won_count": [30, 35, 28, 32],
        "mb_player_rally_won_shots": [120, 140, 100, 128],
        "mb_player_rally_lost_count": [25, 20, 22, 24],
        "mb_player_rally_lost_shots": [100, 80, 88, 96],
        "total_points": [100, 110, 90, 105],
    }
    if overrides:
        data.update(overrides)
    return pl.DataFrame(data).lazy()


class TestStyleServeFeatures:
    """Tests for style serve metrics (Layer 1)."""

    def test_avg_1st_serve_speed_registered(self):
        registry = get_registry()
        feat = registry.get("style_avg_1st_serve_speed")
        assert feat.params == []
        assert feat.mirror is True

    def test_avg_1st_serve_speed_computes(self):
        from mvp.model.features.style import style_avg_1st_serve_speed

        df = _make_style_df()
        result = df.with_columns(
            style_avg_1st_serve_speed().alias("style_avg_1st_serve_speed")
        ).collect()
        # Row 0: no prior -> null
        # Row 1: [200] -> 200.0
        # Row 2: [200, 195] -> 197.5
        # Row 3: [200, 195, 205] -> 200.0
        assert result["style_avg_1st_serve_speed"][0] is None
        assert result["style_avg_1st_serve_speed"][1] == 200.0
        assert result["style_avg_1st_serve_speed"][2] == pytest.approx(197.5)
        assert result["style_avg_1st_serve_speed"][3] == pytest.approx(200.0)

    def test_max_1st_serve_speed_computes(self):
        from mvp.model.features.style import style_max_1st_serve_speed

        df = _make_style_df()
        result = df.with_columns(
            style_max_1st_serve_speed().alias("style_max_1st_serve_speed")
        ).collect()
        # Row 0: no prior -> null
        # Row 1: [220] -> 220
        # Row 2: [220, 215] -> 220
        # Row 3: [220, 215, 230] -> 230
        assert result["style_max_1st_serve_speed"][0] is None
        assert result["style_max_1st_serve_speed"][1] == 220.0
        assert result["style_max_1st_serve_speed"][2] == 220.0
        assert result["style_max_1st_serve_speed"][3] == 230.0

    def test_1st_serve_speed_variance_computes(self):
        from mvp.model.features.style import style_1st_serve_speed_variance

        df = _make_style_df()
        result = df.with_columns(
            style_1st_serve_speed_variance().alias("var")
        ).collect()
        # Row 0: no prior -> null
        # Row 1: [8.0] -> 8.0
        # Row 2: [8.0, 6.0] -> 7.0
        # Row 3: [8.0, 6.0, 10.0] -> 8.0
        assert result["var"][0] is None
        assert result["var"][1] == 8.0
        assert result["var"][2] == pytest.approx(7.0)
        assert result["var"][3] == pytest.approx(8.0)


class TestStyleAggressionFeatures:
    """Tests for aggression, game quality, pressure, rally shape metrics."""

    def test_winner_rate_registered(self):
        registry = get_registry()
        feat = registry.get("style_winner_rate")
        assert feat.params == []
        assert feat.mirror is True

    def test_winner_rate_computes(self):
        from mvp.model.features.style import style_winner_rate

        df = _make_style_df()
        result = df.with_columns(style_winner_rate().alias("wr")).collect()
        # Row 0: no prior -> null
        # Row 1: 20/100 = 0.2
        # Row 2: mean(20/100, 25/110) = mean(0.2, 0.2273) ~ 0.2136
        assert result["wr"][0] is None
        assert result["wr"][1] == pytest.approx(0.2)
        assert result["wr"][2] == pytest.approx((20 / 100 + 25 / 110) / 2, abs=0.001)

    def test_winner_ue_ratio_handles_zero_ues(self):
        from mvp.model.features.style import style_winner_ue_ratio

        df = _make_style_df(overrides={"mb_player_ues": [0, 10, 12, 14]})
        result = df.with_columns(style_winner_ue_ratio().alias("wur")).collect()
        # Row 1: prior is match 0 where ues=0 -> ratio is null -> rolling mean of [null] -> null
        assert result["wur"][1] is None
        # Row 2: prior matches: [null, 25/10=2.5] -> mean of non-null = 2.5
        assert result["wur"][2] == pytest.approx(2.5)

    def test_easy_hold_pct_computes(self):
        from mvp.model.features.style import style_easy_hold_pct

        df = _make_style_df()
        result = df.with_columns(style_easy_hold_pct().alias("ehp")).collect()
        # Uses ratio_feature: sum(easy_holds)/sum(service_games) over 365d
        # Row 1: 5/10 = 0.5
        # Row 2: (5+6)/(10+11) = 11/21 ~ 0.5238
        assert result["ehp"][0] is None
        assert result["ehp"][1] == pytest.approx(0.5)
        assert result["ehp"][2] == pytest.approx(11 / 21, abs=0.001)

    def test_crucial_pts_win_pct_computes(self):
        from mvp.model.features.style import style_crucial_pts_win_pct

        df = _make_style_df()
        result = df.with_columns(style_crucial_pts_win_pct().alias("cp")).collect()
        # Row 1: 8/15 ~ 0.5333
        # Row 2: (8+10)/(15+18) = 18/33 ~ 0.5454
        assert result["cp"][0] is None
        assert result["cp"][1] == pytest.approx(8 / 15, abs=0.001)
        assert result["cp"][2] == pytest.approx(18 / 33, abs=0.001)

    def test_rally_won_avg_length_computes(self):
        from mvp.model.features.style import style_rally_won_avg_length

        df = _make_style_df()
        result = df.with_columns(style_rally_won_avg_length().alias("rwl")).collect()
        # Row 1: 120/30 = 4.0
        # Row 2: (120+140)/(30+35) = 260/65 = 4.0
        assert result["rwl"][0] is None
        assert result["rwl"][1] == pytest.approx(4.0)
        assert result["rwl"][2] == pytest.approx(4.0)
