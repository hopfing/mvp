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


def _make_stroke_df() -> pl.LazyFrame:
    """Test DataFrame with stroke_analysis columns."""
    return pl.DataFrame({
        "player_id": ["A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 3, 1),
            date(2024, 6, 1),
        ],
        "player_fh_winners": [10, 12, 8],
        "player_fh_unforced_errors": [5, 4, 6],
        "player_fh_forced_errors": [3, 2, 4],
        "player_bh_winners": [6, 8, 4],
        "player_bh_unforced_errors": [4, 3, 5],
        "player_bh_forced_errors": [2, 1, 3],
    }).lazy()


def _make_shot_variety_df() -> pl.LazyFrame:
    """Test DataFrame with stroke_analysis shot variety columns."""
    return pl.DataFrame({
        "player_id": ["A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 3, 1),
            date(2024, 6, 1),
        ],
        "player_ground_stroke_winners": [15, 18, 12],
        "player_ground_stroke_unforced_errors": [10, 8, 12],
        "player_ground_stroke_forced_errors": [5, 6, 4],
        "player_ground_stroke_others": [70, 68, 72],
        "player_volley_winners": [3, 4, 2],
        "player_volley_forced_errors": [1, 1, 1],
        "player_volley_unforced_errors": [1, 0, 1],
        "player_volley_others": [5, 6, 4],
        "player_approach_winners": [2, 3, 1],
        "player_approach_forced_errors": [1, 0, 1],
        "player_approach_unforced_errors": [0, 1, 0],
        "player_approach_others": [3, 2, 3],
        "player_overhead_winners": [1, 1, 0],
        "player_overhead_forced_errors": [0, 0, 0],
        "player_overhead_unforced_errors": [0, 0, 1],
        "player_overhead_others": [1, 1, 0],
        "player_drop_shot_winners": [2, 3, 1],
        "player_drop_shot_forced_errors": [1, 0, 1],
        "player_drop_shot_unforced_errors": [1, 1, 0],
        "player_drop_shot_others": [2, 2, 2],
        "player_passing_winners": [3, 2, 4],
        "player_passing_forced_errors": [1, 1, 1],
        "player_passing_unforced_errors": [1, 1, 2],
        "player_passing_others": [3, 4, 3],
        "player_lob_winners": [1, 0, 1],
        "player_lob_forced_errors": [0, 1, 0],
        "player_lob_unforced_errors": [1, 0, 1],
        "player_lob_others": [2, 3, 2],
        "pts_total_pts_played": [150, 160, 140],
    }).lazy()


def _make_rally_df() -> pl.LazyFrame:
    """Test DataFrame with rally_analysis columns."""
    return pl.DataFrame({
        "player_id": ["A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 3, 1),
            date(2024, 6, 1),
        ],
        "rally_short_count": [40, 45, 38],
        "rally_medium_count": [30, 28, 32],
        "rally_long_count": [10, 12, 8],
        "rally_points_with_data": [80, 85, 78],
        "player_short_won": [22, 25, 20],
        "player_short_err": [18, 20, 18],
        "player_long_won": [6, 8, 4],
        "player_long_err": [4, 4, 4],
    }).lazy()


class TestStyleWingFeatures:
    """Tests for wing preference metrics."""

    def test_fh_winner_share_registered(self):
        registry = get_registry()
        feat = registry.get("style_fh_winner_share")
        assert feat.mirror is True

    def test_fh_winner_share_computes(self):
        from mvp.model.features.style import style_fh_winner_share

        df = _make_stroke_df()
        result = df.with_columns(
            style_fh_winner_share().alias("fws")
        ).collect()
        # Row 0: no prior -> null
        # Row 1: 10/(10+6) = 0.625
        # Row 2: mean(10/16, 12/20) = mean(0.625, 0.6) = 0.6125
        assert result["fws"][0] is None
        assert result["fws"][1] == pytest.approx(10 / 16)
        assert result["fws"][2] == pytest.approx((10 / 16 + 12 / 20) / 2, abs=0.001)

    def test_fh_winner_rate_computes(self):
        from mvp.model.features.style import style_fh_winner_rate

        df = _make_stroke_df()
        result = df.with_columns(
            style_fh_winner_rate().alias("fwr")
        ).collect()
        # Row 1: 10/(10+3+5) = 10/18 ~ 0.5556
        assert result["fwr"][0] is None
        assert result["fwr"][1] == pytest.approx(10 / 18, abs=0.001)


class TestStyleShotVarietyFeatures:
    """Tests for rally ball-striking and shot variety metrics."""

    def test_ground_stroke_winner_rate_computes(self):
        from mvp.model.features.style import style_ground_stroke_winner_rate

        df = _make_shot_variety_df()
        result = df.with_columns(
            style_ground_stroke_winner_rate().alias("gswr")
        ).collect()
        # Row 1: 15/(15+10+5+70) = 15/100 = 0.15
        assert result["gswr"][0] is None
        assert result["gswr"][1] == pytest.approx(0.15)

    def test_net_approach_frequency_computes(self):
        from mvp.model.features.style import style_net_approach_frequency

        df = _make_shot_variety_df()
        result = df.with_columns(
            style_net_approach_frequency().alias("naf")
        ).collect()
        # Row 1: net = volley(3+1+1+5) + approach(2+1+0+3) + overhead(1+0+0+1) = 10+6+2 = 18
        # 18/150 = 0.12
        assert result["naf"][0] is None
        assert result["naf"][1] == pytest.approx(18 / 150, abs=0.001)

    def test_drop_shot_effectiveness_computes(self):
        from mvp.model.features.style import style_drop_shot_effectiveness

        df = _make_shot_variety_df()
        result = df.with_columns(
            style_drop_shot_effectiveness().alias("dse")
        ).collect()
        # Row 1: 2/(2+1+1+2) = 2/6 ~ 0.333
        assert result["dse"][0] is None
        assert result["dse"][1] == pytest.approx(2 / 6, abs=0.001)


class TestStyleRallyLengthFeatures:
    """Tests for rally length metrics."""

    def test_short_rally_pct_computes(self):
        from mvp.model.features.style import style_short_rally_pct

        df = _make_rally_df()
        result = df.with_columns(
            style_short_rally_pct().alias("srp")
        ).collect()
        # Row 1: 40/80 = 0.5
        assert result["srp"][0] is None
        assert result["srp"][1] == pytest.approx(0.5)

    def test_short_rally_win_pct_computes(self):
        from mvp.model.features.style import style_short_rally_win_pct

        df = _make_rally_df()
        result = df.with_columns(
            style_short_rally_win_pct().alias("srwp")
        ).collect()
        # Row 1: 22/(22+18) = 22/40 = 0.55
        assert result["srwp"][0] is None
        assert result["srwp"][1] == pytest.approx(22 / 40)


class TestStyleDiffFeatures:
    """Tests for diff features (player - opponent)."""

    def test_all_29_diffs_registered(self):
        registry = get_registry()
        single_features = [
            "style_avg_1st_serve_speed", "style_max_1st_serve_speed",
            "style_avg_2nd_serve_speed", "style_max_2nd_serve_speed",
            "style_1st_serve_speed_variance",
            "style_winner_rate", "style_ue_rate",
            "style_winner_ue_ratio", "style_forced_error_rate",
            "style_easy_hold_pct", "style_difficult_hold_pct",
            "style_crucial_pts_win_pct",
            "style_rally_won_avg_length", "style_rally_lost_avg_length",
            "style_fh_winner_share", "style_fh_ue_share",
            "style_fh_winner_rate", "style_bh_winner_rate",
            "style_ground_stroke_winner_rate", "style_ground_stroke_ue_rate",
            "style_net_approach_frequency", "style_drop_shot_frequency",
            "style_drop_shot_effectiveness", "style_passing_frequency",
            "style_lob_frequency",
            "style_short_rally_pct", "style_long_rally_pct",
            "style_short_rally_win_pct", "style_long_rally_win_pct",
        ]
        for name in single_features:
            diff_name = f"{name}_diff"
            feat = registry.get(diff_name)
            assert feat.mirror is False, f"{diff_name} should not mirror"
            assert name in feat.depends_on, f"{diff_name} should depend on {name}"

    def test_winner_rate_diff_computes(self):
        from mvp.model.features.style import style_winner_rate_diff

        df = pl.DataFrame({
            "player_style_winner_rate": [0.20, 0.25, 0.18],
            "opp_style_winner_rate": [0.15, 0.25, 0.22],
        }).lazy()
        result = df.with_columns(
            style_winner_rate_diff().alias("d")
        ).collect()
        assert result["d"][0] == pytest.approx(0.05)
        assert result["d"][1] == pytest.approx(0.0)
        assert result["d"][2] == pytest.approx(-0.04)
