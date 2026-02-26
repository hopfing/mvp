"""Tests for Layer 2 cross-tournament aggregation."""

from datetime import date, datetime
from pathlib import Path

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
        """DC and team circuit activity rows should be excluded."""
        from mvp.atptour.aggregators.matches import filter_dc_activity

        df = pl.DataFrame({
            "event_type": ["250", "DC", "CH", "WT", "DC"],
            "circuit": ["tour", "tour", "chal", "team", "tour"],
        })
        result = filter_dc_activity(df)
        assert len(result) == 2
        assert result["event_type"].to_list() == ["250", "CH"]


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


class TestRankingsJoin:
    def test_asof_join_picks_most_recent_before_tournament(self):
        """Rankings join should use the most recent snapshot <= tournament_start_date."""
        from mvp.atptour.aggregators.matches import join_rankings

        matches = pl.DataFrame({
            "player_id": ["A001", "A001"],
            "opp_id": ["B002", "C003"],
            "tournament_start_date": [date(2024, 3, 4), date(2024, 3, 11)],
        })
        rankings = pl.DataFrame({
            "player_id": ["A001", "A001", "B002", "C003"],
            "ranking_date": [date(2024, 2, 26), date(2024, 3, 4), date(2024, 3, 4), date(2024, 3, 4)],
            "rank": [10, 9, 20, 30],
            "points": [1000, 1100, 500, 400],
            "tournaments_played": [5, 6, 10, 8],
        })
        result = join_rankings(matches, rankings)
        # First match: tournament_start 3/4, should pick 3/4 snapshot (rank=9)
        assert result.filter(pl.col("opp_id") == "B002")["rankings_rank"][0] == 9
        # Opponent B002 should have rank 20
        assert result.filter(pl.col("opp_id") == "B002")["rankings_opp_rank"][0] == 20

    def test_no_ranking_date_in_output(self):
        """join_rankings should not leak ranking_date columns into output."""
        from mvp.atptour.aggregators.matches import join_rankings

        matches = pl.DataFrame({
            "player_id": ["A001"],
            "opp_id": ["B002"],
            "tournament_start_date": [date(2024, 3, 4)],
        })
        rankings = pl.DataFrame({
            "player_id": ["A001", "B002"],
            "ranking_date": [date(2024, 2, 26), date(2024, 2, 26)],
            "rank": [10, 20],
            "points": [1000, 500],
            "tournaments_played": [5, 10],
        })
        result = join_rankings(matches, rankings)
        assert not any(c.startswith("ranking_date") for c in result.columns)

    def test_null_tournament_date_gets_null_rankings(self):
        """Matches with null tournament_start_date should get null rankings."""
        from mvp.atptour.aggregators.matches import join_rankings

        matches = pl.DataFrame({
            "player_id": ["A001"],
            "opp_id": ["B002"],
            "tournament_start_date": [None],
        }).cast({"tournament_start_date": pl.Date})
        rankings = pl.DataFrame({
            "player_id": ["A001"],
            "ranking_date": [date(2024, 3, 4)],
            "rank": [10],
            "points": [1000],
            "tournaments_played": [5],
        })
        result = join_rankings(matches, rankings)
        assert result["rankings_rank"][0] is None


class TestBioJoin:
    def test_bio_enrichment_adds_player_and_opp_fields(self):
        """Bio join should add prefixed fields for both player and opponent."""
        from mvp.atptour.aggregators.matches import join_player_bio

        matches = pl.DataFrame({
            "player_id": ["A001"],
            "opp_id": ["B002"],
        })
        bio = pl.DataFrame({
            "player_id": ["A001", "B002"],
            "first_name": ["Alice", "Bob"],
            "last_name": ["Alpha", "Beta"],
            "height_cm": [175, 185],
            "weight_kg": [65, 80],
            "right_handed": [True, False],
            "twohand_backhand": [True, True],
            "birth_date": [date(2000, 1, 1), date(1998, 6, 15)],
            "pro_year": [2018, 2016],
            "nationality": ["USA", "GBR"],
            "natl_id": ["USA", "GBR"],
        })
        result = join_player_bio(matches, bio)
        assert result["player_first_name"][0] == "Alice"
        assert result["opp_first_name"][0] == "Bob"
        assert result["player_height_cm"][0] == 175
        assert result["opp_height_cm"][0] == 185
        assert result["player_natl_id"][0] == "USA"
        assert result["opp_natl_id"][0] == "GBR"

    def test_bio_unknown_player_gets_nulls(self):
        """Players not in bio should get null fields."""
        from mvp.atptour.aggregators.matches import join_player_bio

        matches = pl.DataFrame({
            "player_id": ["UNKNOWN"],
            "opp_id": ["ALSO_UNKNOWN"],
        })
        bio = pl.DataFrame({
            "player_id": ["A001"],
            "first_name": ["Alice"],
            "last_name": ["Alpha"],
            "height_cm": [175],
            "weight_kg": [65],
            "right_handed": [True],
            "twohand_backhand": [True],
            "birth_date": [date(2000, 1, 1)],
            "pro_year": [2018],
            "nationality": ["USA"],
            "natl_id": ["USA"],
        })
        result = join_player_bio(matches, bio)
        assert result["player_first_name"][0] is None
        assert result["opp_first_name"][0] is None


class TestValidation:
    def test_clean_data_passes(self):
        """Normal data (1-2 tournaments per week) should produce no warnings."""
        from mvp.atptour.aggregators.matches import validate_tournament_clusters

        df = pl.DataFrame({
            "player_id": ["A001", "A001", "A001"],
            "tournament_id": ["100", "200", "300"],
            "tournament_start_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
        })
        warnings = validate_tournament_clusters(df)
        assert len(warnings) == 0

    def test_suspicious_cluster_flagged(self):
        """3+ tournaments within 7 days should be flagged."""
        from mvp.atptour.aggregators.matches import validate_tournament_clusters

        df = pl.DataFrame({
            "player_id": ["A001"] * 3,
            "tournament_id": ["100", "200", "300"],
            "tournament_start_date": [date(2024, 1, 1), date(2024, 1, 3), date(2024, 1, 5)],
        })
        warnings = validate_tournament_clusters(df)
        assert len(warnings) == 1
        assert warnings[0]["player_id"] == "A001"
        assert len(warnings[0]["tournament_ids"]) == 3


class TestMatchesAggregator:
    def _create_test_data(self, tmp_path: Path):
        """Create minimal Layer 1, Activity, Rankings, and Bio data for testing."""
        data_root = tmp_path / "data"

        # Layer 1: one tournament with 2 matches (4 rows at player-match grain)
        agg_dir = (
            data_root / "aggregate" / "atptour" / "tournaments"
            / "tour" / "339" / "2024"
        )
        agg_dir.mkdir(parents=True)
        l1 = pl.DataFrame({
            "match_uid": ["uid1", "uid1", "uid2", "uid2"],
            "player_id": ["A001", "B002", "A001", "C003"],
            "opp_id": ["B002", "A001", "C003", "A001"],
            "tournament_id": ["339", "339", "339", "339"],
            "year": [2024, 2024, 2024, 2024],
            "circuit": ["tour", "tour", "tour", "tour"],
            "draw_type": ["singles"] * 4,
            "round": ["R32", "R32", "R16", "R16"],
            "won": [True, False, True, False],
            "tournament_start_date": [date(2024, 3, 4)] * 4,
            "tournament_end_date": [date(2024, 3, 10)] * 4,
            "event_type": ["250", "250", "250", "250"],
            "indoor": [False] * 4,
            "surface": ["Hard"] * 4,
        })
        # Add remaining MATCHES_SCHEMA columns as null
        from mvp.atptour.aggregators.tournament_matches import MATCHES_SCHEMA

        for col, dtype in MATCHES_SCHEMA.items():
            if col not in l1.columns:
                l1 = l1.with_columns(pl.lit(None).cast(dtype).alias(col))
        l1.write_parquet(agg_dir / "matches.parquet")

        # Activity: one overlapping match + one gap-fill match
        act = pl.DataFrame({
            "match_uid": ["uid1", "uid_itf"],
            "player_id": ["A001", "D004"],
            "opp_id": ["B002", "E005"],
            "tournament_id": ["339", "9999"],
            "year": [2024, 2024],
            "event_type": ["250", "FU"],
            "circuit": ["tour", "itf"],
            "round": ["R32", "R32"],
            "surface": ["Hard", "Clay"],
            "indoor": [False, True],
            "tournament_start_date": [date(2024, 3, 4), date(2024, 4, 1)],
            "tournament_end_date": [date(2024, 3, 10), date(2024, 4, 7)],
            "win_loss": ["W", "L"],
            "reason": [None, None],
            "player_rank": [50, 500],
            "opp_rank": [100, 600],
            "points": [45, 0],
            "is_bye": [False, False],
            "match_id": ["m1", "m2"],
            **{
                f"player_set{n}_{k}": [None, None]
                for n in range(1, 6)
                for k in ("games", "tiebreak")
            },
            **{
                f"opp_set{n}_{k}": [None, None]
                for n in range(1, 6)
                for k in ("games", "tiebreak")
            },
        })
        act_dir = data_root / "stage" / "atptour"
        act_dir.mkdir(parents=True)
        act.write_parquet(act_dir / "activity.parquet")

        # Rankings
        rnk = pl.DataFrame({
            "player_id": ["A001", "B002", "C003"],
            "ranking_date": [date(2024, 2, 26)] * 3,
            "rank": [10, 20, 30],
            "points": [1000, 500, 400],
            "tournaments_played": [5, 10, 8],
            "player_name": ["A", "B", "C"],
            "nationality": ["USA", "GBR", "FRA"],
            "age": [25, 28, 22],
            "rank_move": [1, -1, 0],
            "points_move": [10, -5, 0],
            "points_dropping": [100, 50, 30],
            "next_best": [90, 45, 25],
            "source_file": ["f"] * 3,
            "parsed_at": [datetime(2024, 1, 1)] * 3,
        })
        rnk_dir = data_root / "stage" / "atptour" / "rankings"
        rnk_dir.mkdir(parents=True)
        rnk.write_parquet(rnk_dir / "rankings_singles.parquet")

        # Bio
        bio_dir = data_root / "stage" / "atptour" / "players"
        bio_dir.mkdir(parents=True)
        for pid, fname, lname, nat in [
            ("A001", "Alice", "A", "USA"),
            ("B002", "Bob", "B", "GBR"),
            ("C003", "Carol", "C", "FRA"),
        ]:
            bio = pl.DataFrame({
                "player_id": [pid],
                "first_name": [fname],
                "last_name": [lname],
                "birth_date": [date(2000, 1, 1)],
                "birth_city": [None],
                "nationality": [nat],
                "natl_id": [nat],
                "height_cm": [180],
                "weight_kg": [75],
                "right_handed": [True],
                "twohand_backhand": [True],
                "pro_year": [2018],
                "is_active": [True],
                "is_dbl_specialist": [False],
                "source_file": ["f"],
                "parsed_at": [datetime(2024, 1, 1)],
            })
            bio.write_parquet(bio_dir / f"{pid}.parquet")

        return data_root

    def test_full_aggregation(self, tmp_path):
        """End-to-end: stack, enrich, gap-fill, rank, bio, sort, validate."""
        from mvp.atptour.aggregators.matches import MatchesAggregator

        data_root = self._create_test_data(tmp_path)
        agg = MatchesAggregator(data_root=data_root)
        result = agg.aggregate()

        # 4 Layer 1 rows + 1 gap-fill row = 5 rows
        assert len(result) == 5
        assert "round_order" in result.columns
        assert "rankings_rank" in result.columns
        assert "player_first_name" in result.columns

        # Verify gap-fill row
        itf_rows = result.filter(pl.col("tournament_id") == "9999")
        assert len(itf_rows) == 1
        assert itf_rows["activity_rank"][0] == 500

        # Verify Activity enrichment on overlap
        a001_r32 = result.filter(
            (pl.col("player_id") == "A001") & (pl.col("round") == "R32")
        )
        assert a001_r32["activity_rank"][0] == 50

        # Verify Rankings enrichment
        assert a001_r32["rankings_rank"][0] == 10

        # Verify Bio enrichment
        assert a001_r32["player_first_name"][0] == "Alice"

        # Verify no duplicate (match_uid, player_id) combos
        assert result.select(["match_uid", "player_id"]).unique().height == len(result)
