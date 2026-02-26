"""Tests for Layer 2 cross-tournament aggregation."""

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
