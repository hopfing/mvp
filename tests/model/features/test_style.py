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
        assert feat.params == ["days"]
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
        assert feat.params == ["days"]
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

    def test_all_28_diffs_registered(self):
        registry = get_registry()
        single_features = [
            "style_avg_1st_serve_speed", "style_max_1st_serve_speed",
            "style_avg_2nd_serve_speed", "style_max_2nd_serve_speed",
            "style_1st_serve_speed_variance",
            "style_winner_rate", "style_ue_rate",
            "style_winner_ue_ratio", "style_forced_error_rate",
            "style_easy_hold_pct", "style_difficult_hold_pct",
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
        style_winner_rate_diff = get_registry().get("style_winner_rate_diff").func

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


class TestStyleMatchupFeatures:
    """Tests for cross-domain matchup features."""

    def test_all_matchups_registered(self):
        registry = get_registry()
        matchup_names = [
            "style_winner_rate_matchup",
            "style_fh_winner_rate_matchup",
            "style_bh_winner_rate_matchup",
            "style_net_approach_frequency_matchup",
            "style_lob_frequency_matchup",
            "style_short_rally_pct_matchup",
            "style_long_rally_pct_matchup",
            "style_short_rally_win_pct_matchup",
            "style_long_rally_win_pct_matchup",
            "style_ground_stroke_winner_rate_matchup",
            "style_easy_hold_pct_matchup",
            "style_difficult_hold_pct_matchup",
            "style_rally_won_avg_length_matchup",
            "style_ue_rate_matchup",
            "style_drop_shot_effectiveness_matchup",
        ]
        for name in matchup_names:
            feat = registry.get(name)
            assert feat.mirror is False
            assert len(feat.depends_on) == 2

    def test_fh_winner_rate_matchup_computes(self):
        style_fh_winner_rate_matchup = get_registry().get("style_fh_winner_rate_matchup").func
        df = pl.DataFrame({
            "player_style_fh_winner_rate": [0.55, 0.60],
            "opp_style_bh_winner_rate": [0.30, 0.45],
        }).lazy()
        result = df.with_columns(
            style_fh_winner_rate_matchup().alias("m")
        ).collect()
        assert result["m"][0] == pytest.approx(0.25)
        assert result["m"][1] == pytest.approx(0.15)


class TestStyleMatchupInteractions:
    """Tests for Layer 3 matchup interaction features."""

    def test_power_serve_vs_strong_return_registered(self):
        registry = get_registry()
        feat = registry.get("matchup_power_serve_vs_strong_return")
        assert feat.mirror is False
        assert "is_power_server" in feat.depends_on

    def test_power_serve_vs_strong_return_computes(self):
        """ret_p75 is a rolling-730d quantile; need enough prior data for stable test."""
        from datetime import date as _date, timedelta as _td

        from mvp.model.features.style import matchup_power_serve_vs_strong_return

        # 20 rows spread over a year. Vary opp_ret_first_serve_win_pct and player_is_power_server.
        # Check that the matchup label fires only when player is power server AND opp's
        # return % is above the rolling 75th percentile of the prior population.
        n = 20
        dates = [_date(2024, 1, 1) + _td(days=15 * i) for i in range(n)]
        # Returns decline over time so a late row with high return is "above rolling p75"
        # only if its value exceeds the recent rolling history.
        returns = [0.20 + 0.005 * (i % 5) for i in range(n)]  # bounces around 0.20-0.22
        # Make the last row very high return AND player is power server
        returns[-1] = 0.50
        is_ps = [1] * n  # always power server, to isolate the return-threshold logic
        df = pl.DataFrame({
            "effective_match_date": dates,
            "player_is_power_server": is_ps,
            "opp_ret_first_serve_win_pct": returns,
        }).lazy()
        result = df.with_columns(
            matchup_power_serve_vs_strong_return().alias("m")
        ).collect()
        # Last row: return 0.50 is well above rolling p75 of ~0.21-0.22 → matchup label = 1
        assert result["m"][n - 1] == 1
        # An early "normal" row: return ~0.20 is not above p75 → matchup label = 0
        assert result["m"][10] == 0

    def test_all_7_interactions_registered(self):
        registry = get_registry()
        interactions = [
            "matchup_power_serve_vs_strong_return",
            "matchup_placement_serve_vs_strong_return",
            "matchup_aggressor_vs_counterpuncher",
            "matchup_counterpuncher_vs_aggressor",
            "matchup_both_power_servers",
            "matchup_both_counterpunchers",
            "matchup_net_rusher_vs_passer",
        ]
        for name in interactions:
            feat = registry.get(name)
            assert feat.mirror is False


class TestStyleBoolLabels:
    """Tests for Layer 2 bool style labels."""

    def test_is_power_server_registered(self):
        registry = get_registry()
        feat = registry.get("is_power_server")
        assert feat.mirror is True
        assert feat.depends_on == ["style_avg_1st_serve_speed"]

    def test_is_power_server_computes(self):
        """Top-tertile rolling-730d 1st-serve speed (population threshold, closed-left)."""
        from datetime import date as _date
        from datetime import timedelta as _td

        from mvp.model.features.style import is_power_server
        # 30 history rows spanning a stable speed distribution (~180..240), then two
        # test rows: a clearly-fast one (>= 2/3 quantile -> power) and a clearly-slow
        # one (<= 2/3 quantile -> not power). Threshold uses prior 730d only.
        speeds = [180.0, 195.0, 210.0, 225.0, 240.0] * 6
        dates = [_date(2023, 1, 1) + _td(days=20 * i) for i in range(30)]
        speeds += [245.0, 175.0]
        dates += [_date(2024, 8, 1), _date(2024, 8, 21)]

        df = pl.DataFrame({
            "effective_match_date": dates,
            "player_style_avg_1st_serve_speed": speeds,
        }).lazy()
        result = df.with_columns(
            is_power_server().alias("is_power_server")
        ).collect()
        # 2/3 quantile of the history (~225); 245 is above -> power, 175 below -> not.
        assert result["is_power_server"][30] == 1
        assert result["is_power_server"][31] == 0

    def test_is_placement_server_computes(self):
        """Bottom-tertile rolling-730d 1st-serve speed."""
        from datetime import date as _date
        from datetime import timedelta as _td

        from mvp.model.features.style import is_placement_server
        speeds = [180.0, 195.0, 210.0, 225.0, 240.0] * 6
        dates = [_date(2023, 1, 1) + _td(days=20 * i) for i in range(30)]
        speeds += [175.0, 245.0]   # slow -> placement, fast -> not
        dates += [_date(2024, 8, 1), _date(2024, 8, 21)]

        df = pl.DataFrame({
            "effective_match_date": dates,
            "player_style_avg_1st_serve_speed": speeds,
        }).lazy()
        result = df.with_columns(
            is_placement_server().alias("is_placement_server")
        ).collect()
        assert result["is_placement_server"][30] == 1
        assert result["is_placement_server"][31] == 0

    def test_all_8_labels_registered(self):
        registry = get_registry()
        labels = [
            "is_power_server", "is_placement_server",
            "is_counterpuncher", "is_aggressive_baseliner",
            "is_net_rusher", "is_clay_specialist", "is_hard_specialist",
            "is_clutch_player",
        ]
        for name in labels:
            feat = registry.get(name)
            assert feat.mirror is True, f"{name} should mirror"

    def test_is_hard_specialist_registered(self):
        registry = get_registry()
        feat = registry.get("is_hard_specialist")
        assert feat.mirror is True
        assert feat.params == []
        assert "elo_hard_specialist" in feat.depends_on


class TestStyleFeatureCount:
    """Verify total feature count matches design."""

    _STYLE_BOOL_LABELS = {
        "is_power_server", "is_placement_server", "is_counterpuncher",
        "is_aggressive_baseliner", "is_net_rusher", "is_clay_specialist",
        "is_hard_specialist", "is_clutch_player",
    }

    def _is_style_feature(self, name: str) -> bool:
        return (
            name.startswith("style_")
            or name.startswith("matchup_")
            or name in self._STYLE_BOOL_LABELS
        )

    def test_total_style_features_registered(self):
        registry = get_registry()
        style_features = [
            n for n in registry.list_features()
            if self._is_style_feature(n)
        ]
        # style.py: 29 single + 29 diff + 15 matchup + 8 bool + 7 interaction = 88
        # style_radar.py: 5 signals + 5 z-axes + 5 shrunk + 5 radar + 5 conf = 25
        # style_matchup.py: 5 resid-vs-axis (Form B lookup) = 5
        # style_matchup_retrieval.py: 1 "style_matchup" transform (Form A) = 1
        assert len(style_features) == 119, (
            f"Expected 119 style features, got {len(style_features)}: {sorted(style_features)}"
        )

    def test_no_duplicate_registrations(self):
        """Each feature name is unique."""
        registry = get_registry()
        all_names = registry.list_features()
        style_names = [
            n for n in all_names
            if self._is_style_feature(n)
        ]
        assert len(style_names) == len(set(style_names))


class TestBinaryIndicatorImpute:
    """Nullable binary indicators must stay null (impute=None), not median-filled.

    Median of a ~33%-fire binary is 0, so median-imputation would merge "unknown"
    (no style data) into "not-this-type". These must preserve null.
    """

    _INDICATORS = [
        "is_power_server", "is_placement_server", "is_aggressive_baseliner",
        "is_counterpuncher", "is_net_rusher", "is_clutch_player",
        "is_clay_specialist", "is_hard_specialist",
        "matchup_power_serve_vs_strong_return",
        "matchup_placement_serve_vs_strong_return",
        "matchup_aggressor_vs_counterpuncher",
        "matchup_counterpuncher_vs_aggressor",
        "matchup_both_power_servers",
        "matchup_both_counterpunchers",
        "matchup_net_rusher_vs_passer",
    ]

    def test_impute_none(self):
        reg = get_registry()
        for name in self._INDICATORS:
            assert reg.get(name).impute is None, f"{name} should have impute=None"
