"""Tests for Layer 2 cross-tournament aggregation."""

from datetime import date

import polars as pl

from mvp.atptour.aggregators.matches import ROUND_ORDER
from mvp.common.enums import Round


class TestRoundOrder:
    def test_all_rounds_have_order(self):
        """Every Round enum value must have a ROUND_ORDER entry."""
        for r in Round:
            assert r.value in ROUND_ORDER, f"Missing ROUND_ORDER for {r.value}"

    def test_qualifiers_before_main_draw(self):
        assert ROUND_ORDER["Q1"] < ROUND_ORDER["R128"]

    def test_final_is_last(self):
        assert ROUND_ORDER["F"] == max(ROUND_ORDER.values())

    def test_thirdplace_before_final(self):
        assert ROUND_ORDER["THIRDPLACE"] < ROUND_ORDER["F"]

    def test_round_order_column(self):
        """add_round_order should add an int column."""
        from mvp.atptour.aggregators.matches import add_round_order

        df = pl.DataFrame({"round": ["Q1", "F", "R32", "THIRDPLACE"]})
        result = add_round_order(df)
        assert "round_order" in result.columns
        assert result["round_order"].dtype == pl.Int64
        assert result["round_order"].to_list() == [
            ROUND_ORDER["Q1"],
            ROUND_ORDER["F"],
            ROUND_ORDER["R32"],
            ROUND_ORDER["THIRDPLACE"],
        ]


class TestDCFilter:
    def test_filter_dc_from_layer1(self):
        """DC tournaments should be excluded from Layer 1 stack."""
        from mvp.atptour.aggregators.matches import filter_dc_tournaments

        df = pl.DataFrame({
            "tournament_id": ["339", "8096", "615", "1234"],
            "event_type": ["250", "DCR", None, "CH"],
            "circuit": ["tour", "tour", "team", "chal"],
        })
        result = filter_dc_tournaments(df)
        assert result["tournament_id"].to_list() == ["339", "1234"]

    def test_filter_dc_from_activity(self):
        """DC activity rows should be excluded."""
        from mvp.atptour.aggregators.matches import filter_dc_activity

        df = pl.DataFrame({
            "event_type": ["250", "DC", "CH", "FU", "DC"],
        })
        result = filter_dc_activity(df)
        assert len(result) == 3
        assert "DC" not in result["event_type"].to_list()


class TestActivityMapping:
    def test_map_activity_to_layer2(self):
        """Activity rows should be mapped to Layer 2 schema with correct column names."""
        from mvp.atptour.aggregators.matches import map_activity_to_layer2

        act = pl.DataFrame({
            "match_uid": ["2024_1234_SGL_R32_A001_B002"],
            "player_id": ["A001"],
            "opp_id": ["B002"],
            "tournament_id": ["1234"],
            "year": [2024],
            "circuit": ["tour"],
            "round": ["R32"],
            "surface": ["Hard"],
            "indoor": [False],
            "event_type": ["250"],
            "tournament_start_date": [date(2024, 3, 1)],
            "tournament_end_date": [date(2024, 3, 7)],
            "win_loss": ["W"],
            "reason": [None],
            "player_rank": [50],
            "opp_rank": [100],
            "points": [45],
            "player_set1_games": [6],
            "opp_set1_games": [3],
            "player_set1_tiebreak": [None],
            "opp_set1_tiebreak": [None],
            "player_set2_games": [6],
            "opp_set2_games": [4],
            "player_set2_tiebreak": [None],
            "opp_set2_tiebreak": [None],
            "player_set3_games": [None],
            "opp_set3_games": [None],
            "player_set3_tiebreak": [None],
            "opp_set3_tiebreak": [None],
            "player_set4_games": [None],
            "opp_set4_games": [None],
            "player_set4_tiebreak": [None],
            "opp_set4_tiebreak": [None],
            "player_set5_games": [None],
            "opp_set5_games": [None],
            "player_set5_tiebreak": [None],
            "opp_set5_tiebreak": [None],
        })
        result = map_activity_to_layer2(act)
        assert result["won"][0] is True
        assert result["draw_type"][0] == "singles"
        assert result["activity_rank"][0] == 50
        assert result["activity_opp_rank"][0] == 100
        assert result["activity_points"][0] == 45
        # Stats columns should not be present (they get added as null during concat)
        assert "svc_aces" not in result.columns

    def test_map_activity_loss(self):
        """win_loss='L' should map to won=False."""
        from mvp.atptour.aggregators.matches import map_activity_to_layer2

        act = pl.DataFrame({
            "match_uid": ["uid1"],
            "player_id": ["A001"],
            "opp_id": ["B002"],
            "tournament_id": ["1234"],
            "year": [2024],
            "circuit": ["tour"],
            "round": ["R32"],
            "surface": ["Hard"],
            "indoor": [False],
            "event_type": ["250"],
            "tournament_start_date": [date(2024, 3, 1)],
            "tournament_end_date": [date(2024, 3, 7)],
            "win_loss": ["L"],
            "reason": ["RET"],
            "player_rank": [50],
            "opp_rank": [100],
            "points": [0],
            **{f"player_set{n}_{k}": [None] for n in range(1, 6) for k in ("games", "tiebreak")},
            **{f"opp_set{n}_{k}": [None] for n in range(1, 6) for k in ("games", "tiebreak")},
        })
        result = map_activity_to_layer2(act)
        assert result["won"][0] is False
