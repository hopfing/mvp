"""Tests for cross-tournament aggregation."""

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

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
    def test_filter_dc_from_tournament_matches(self):
        """DC tournaments should be excluded from tournament matches."""
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
    def test_map_activity_to_matches_schema(self):
        """Activity rows should be mapped to matches schema with correct column names."""
        from mvp.atptour.aggregators.matches import map_activity_to_matches_schema

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
        result = map_activity_to_matches_schema(act)
        assert result["won"][0] is True
        assert result["draw_type"][0] == "singles"
        assert result["activity_rank"][0] == 50
        assert result["activity_opp_rank"][0] == 100
        assert result["activity_points"][0] == 45
        # Stats columns should not be present (they get added as null during concat)
        assert "svc_aces" not in result.columns

    def test_map_activity_loss(self):
        """win_loss='L' should map to won=False."""
        from mvp.atptour.aggregators.matches import map_activity_to_matches_schema

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
        result = map_activity_to_matches_schema(act)
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
        assert result.filter(pl.col("opp_id") == "B002")["player_rankings_rank"][0] == 9
        # Opponent B002 should have rank 20
        assert result.filter(pl.col("opp_id") == "B002")["opp_rankings_rank"][0] == 20

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
        assert result["player_rankings_rank"][0] is None


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
        """Normal data (separate days per tournament) should produce no warnings."""
        from mvp.atptour.aggregators.matches import validate_tournament_scheduling
        from datetime import datetime

        df = pl.DataFrame({
            "player_id": ["A001", "A001", "A001"],
            "tournament_id": ["100", "200", "300"],
            "effective_match_date": [
                datetime(2024, 1, 7),
                datetime(2024, 2, 4),
                datetime(2024, 3, 3),
            ],
        })
        warnings = validate_tournament_scheduling(df)
        assert len(warnings) == 0

    def test_same_day_different_tournaments_flagged(self):
        """Player in 2 tournaments on same day should be flagged."""
        from mvp.atptour.aggregators.matches import validate_tournament_scheduling
        from datetime import datetime

        df = pl.DataFrame({
            "player_id": ["A001", "A001"],
            "tournament_id": ["100", "200"],
            "effective_match_date": [datetime(2024, 1, 5), datetime(2024, 1, 5)],
        })
        warnings = validate_tournament_scheduling(df)
        assert len(warnings) == 1
        assert warnings[0]["type"] == "same_day"
        assert warnings[0]["player_id"] == "A001"
        assert set(warnings[0]["tournament_ids"]) == {"100", "200"}

    def test_interleaved_tournaments_flagged(self):
        """A, B, A pattern within 7 days should be flagged."""
        from mvp.atptour.aggregators.matches import validate_tournament_scheduling
        from datetime import datetime

        df = pl.DataFrame({
            "player_id": ["A001", "A001", "A001"],
            "tournament_id": ["100", "200", "100"],
            "effective_match_date": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 3),
                datetime(2024, 1, 5),
            ],
        })
        warnings = validate_tournament_scheduling(df)
        assert len(warnings) == 1
        assert warnings[0]["type"] == "interleaved"
        assert warnings[0]["player_id"] == "A001"


class TestMatchesAggregator:
    def _create_test_data(self, tmp_path: Path):
        """Create minimal tournament matches, Activity, Rankings, and Bio data for testing."""
        data_root = tmp_path / "data"

        # Tournament matches: one tournament with 2 matches (4 rows at player-match grain)
        agg_dir = (
            data_root / "aggregate" / "atptour" / "tournaments"
            / "tour" / "339" / "2024"
        )
        agg_dir.mkdir(parents=True)
        tournament_matches = pl.DataFrame({
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
            if col not in tournament_matches.columns:
                tournament_matches = tournament_matches.with_columns(
                    pl.lit(None).cast(dtype).alias(col)
                )
        tournament_matches.write_parquet(agg_dir / "matches.parquet")

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

        # 4 tournament match rows + 1 gap-fill row = 5 rows
        assert len(result) == 5
        assert "round_order" in result.columns
        assert "player_rankings_rank" in result.columns
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
        assert a001_r32["player_rankings_rank"][0] == 10

        # Verify Bio enrichment
        assert a001_r32["player_first_name"][0] == "Alice"

        # Verify no duplicate (match_uid, player_id) combos
        assert result.select(["match_uid", "player_id"]).unique().height == len(result)


class TestEffectiveMatchDate:
    def test_estimated_from_round_offsets(self):
        """Groups without Schedule data get estimated dates via scaled offset."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        # Tournament Mar 8-10 (2 day duration)
        # Rounds: R32 (order 7), SF (order 10), F (order 12)
        # Offsets after rank: R32=0, SF=1, F=2; max=2
        # Scaled: R32=0, SF=1, F=2 days from start
        df = pl.DataFrame({
            "tournament_id": ["T1"] * 4,
            "year": [2024] * 4,
            "draw_type": ["singles"] * 4,
            "round": ["F", "SF", "R32", "R32"],
            "round_order": [12, 10, 7, 7],
            "tournament_start_date": [date(2024, 3, 8)] * 4,
            "tournament_end_date": [date(2024, 3, 10)] * 4,
            "scheduled_datetime": [None] * 4,
        }).cast({
            "scheduled_datetime": pl.Datetime,
            "tournament_start_date": pl.Date,
            "tournament_end_date": pl.Date,
        })
        result = add_effective_match_date(df)
        assert "effective_match_date" in result.columns

        f_date = result.filter(pl.col("round") == "F")["effective_match_date"][0]
        sf_date = result.filter(pl.col("round") == "SF")["effective_match_date"][0]
        r32_dates = result.filter(
            pl.col("round") == "R32"
        )["effective_match_date"].to_list()

        assert f_date == datetime(2024, 3, 10)
        assert sf_date == datetime(2024, 3, 9)
        assert all(d == datetime(2024, 3, 8) for d in r32_dates)

    def test_schedule_override_when_all_present(self):
        """Groups where 100% have scheduled_datetime use that value."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        df = pl.DataFrame({
            "tournament_id": ["T1"] * 2,
            "year": [2024] * 2,
            "draw_type": ["singles"] * 2,
            "round": ["F", "SF"],
            "round_order": [12, 10],
            "tournament_start_date": [date(2024, 3, 8)] * 2,
            "tournament_end_date": [date(2024, 3, 10)] * 2,
            "scheduled_datetime": [
                datetime(2024, 3, 10, 14, 0),
                datetime(2024, 3, 9, 19, 30),
            ],
        })
        result = add_effective_match_date(df)
        f_date = result.filter(pl.col("round") == "F")["effective_match_date"][0]
        sf_date = result.filter(pl.col("round") == "SF")["effective_match_date"][0]
        assert f_date == datetime(2024, 3, 10, 14, 0)
        assert sf_date == datetime(2024, 3, 9, 19, 30)

    def test_partial_schedule_uses_estimated(self):
        """Partial schedule coverage falls back to estimation."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        # 2 rounds, duration=2, max_offset=1
        # SF offset=0 -> start+0=Mar 8, F offset=1 -> start+2=Mar 10
        df = pl.DataFrame({
            "tournament_id": ["T1"] * 2,
            "year": [2024] * 2,
            "draw_type": ["singles"] * 2,
            "round": ["F", "SF"],
            "round_order": [12, 10],
            "tournament_start_date": [date(2024, 3, 8)] * 2,
            "tournament_end_date": [date(2024, 3, 10)] * 2,
            "scheduled_datetime": [datetime(2024, 3, 10, 14, 0), None],
        })
        result = add_effective_match_date(df)
        f_date = result.filter(pl.col("round") == "F")["effective_match_date"][0]
        sf_date = result.filter(pl.col("round") == "SF")["effective_match_date"][0]
        # Both estimated: SF=start, F=start+2
        assert f_date == datetime(2024, 3, 10)
        assert sf_date == datetime(2024, 3, 8)

    def test_multiple_groups_independent(self):
        """Different tournament groups are computed independently."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        df = pl.DataFrame({
            "tournament_id": ["T1", "T1", "T2", "T2"],
            "year": [2024, 2024, 2024, 2024],
            "draw_type": ["singles", "singles", "singles", "singles"],
            "round": ["F", "SF", "F", "QF"],
            "round_order": [12, 10, 12, 9],
            "tournament_start_date": [
                date(2024, 3, 8), date(2024, 3, 8),
                date(2024, 6, 13), date(2024, 6, 13),
            ],
            "tournament_end_date": [
                date(2024, 3, 10), date(2024, 3, 10),
                date(2024, 6, 15), date(2024, 6, 15),
            ],
            "scheduled_datetime": [None] * 4,
        }).cast({
            "scheduled_datetime": pl.Datetime,
            "tournament_start_date": pl.Date,
            "tournament_end_date": pl.Date,
        })
        result = add_effective_match_date(df)

        t1_f = result.filter(
            (pl.col("tournament_id") == "T1") & (pl.col("round") == "F")
        )["effective_match_date"][0]
        t2_f = result.filter(
            (pl.col("tournament_id") == "T2") & (pl.col("round") == "F")
        )["effective_match_date"][0]
        t2_qf = result.filter(
            (pl.col("tournament_id") == "T2") & (pl.col("round") == "QF")
        )["effective_match_date"][0]

        assert t1_f == datetime(2024, 3, 10)
        assert t2_f == datetime(2024, 6, 15)
        assert t2_qf == datetime(2024, 6, 13)

    def test_singles_and_doubles_share_scaling(self):
        """Singles and doubles share tournament-wide round scaling."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        df = pl.DataFrame({
            "tournament_id": ["T1"] * 4,
            "year": [2024] * 4,
            "draw_type": ["singles", "singles", "doubles", "doubles"],
            "round": ["F", "SF", "F", "SF"],
            "round_order": [12, 10, 12, 10],
            "tournament_start_date": [date(2024, 3, 8)] * 4,
            "tournament_end_date": [date(2024, 3, 10)] * 4,
            "scheduled_datetime": [
                datetime(2024, 3, 10, 14, 0),
                datetime(2024, 3, 9, 12, 0),
                None, None,
            ],
        })
        result = add_effective_match_date(df)

        sgl_f = result.filter(
            (pl.col("draw_type") == "singles") & (pl.col("round") == "F")
        )["effective_match_date"][0]
        dbl_f = result.filter(
            (pl.col("draw_type") == "doubles") & (pl.col("round") == "F")
        )["effective_match_date"][0]

        # Tournament has partial schedule coverage -> all use estimation
        # Both F rounds get end date
        assert sgl_f == datetime(2024, 3, 10)
        assert dbl_f == datetime(2024, 3, 10)

    def test_null_tournament_dates_raises(self):
        """Rows with null tournament dates in estimated groups trigger ValueError."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        df = pl.DataFrame({
            "tournament_id": ["T1"],
            "year": [2024],
            "draw_type": ["singles"],
            "round": ["F"],
            "round_order": [12],
            "tournament_start_date": [None],
            "tournament_end_date": [None],
            "scheduled_datetime": [None],
            "circuit": ["tour"],
            "match_uid": ["test_match"],
        }).cast({
            "scheduled_datetime": pl.Datetime,
            "tournament_start_date": pl.Date,
            "tournament_end_date": pl.Date,
        })

        with pytest.raises(ValueError, match="null effective_match_date"):
            add_effective_match_date(df)

    def test_null_round_order_uses_start_date(self):
        """Null round_order defaults to start_date (offset 0)."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        df = pl.DataFrame({
            "tournament_id": ["T1"],
            "year": [2024],
            "draw_type": ["singles"],
            "round": ["UNKNOWN"],
            "round_order": [None],
            "tournament_start_date": [date(2024, 3, 8)],
            "tournament_end_date": [date(2024, 3, 10)],
            "scheduled_datetime": [None],
            "circuit": ["tour"],
            "match_uid": ["test_match"],
        }).cast({
            "scheduled_datetime": pl.Datetime,
            "tournament_start_date": pl.Date,
            "tournament_end_date": pl.Date,
            "round_order": pl.Int64,
        })

        result = add_effective_match_date(df)
        # With null round_order, max_offset check fails, defaults to offset 0
        assert result["effective_match_date"][0] == datetime(2024, 3, 8)

    def test_preserves_existing_columns(self):
        """Function should not drop any existing columns."""
        from mvp.atptour.aggregators.matches import add_effective_match_date

        df = pl.DataFrame({
            "tournament_id": ["T1"],
            "year": [2024],
            "draw_type": ["singles"],
            "round": ["F"],
            "round_order": [12],
            "tournament_start_date": [date(2024, 3, 8)],
            "tournament_end_date": [date(2024, 3, 10)],
            "scheduled_datetime": [None],
            "match_uid": ["uid1"],
            "player_id": ["A001"],
        }).cast({
            "scheduled_datetime": pl.Datetime,
            "tournament_start_date": pl.Date,
            "tournament_end_date": pl.Date,
        })
        result = add_effective_match_date(df)
        assert "match_uid" in result.columns
        assert "player_id" in result.columns
        # only added effective_match_date
        assert len(result.columns) == len(df.columns) + 1


class TestMatchesAggregatorSort:
    def test_effective_match_date_in_output(self, tmp_path):
        """aggregate() output should include effective_match_date column."""
        from mvp.atptour.aggregators.matches import MatchesAggregator

        data_root = TestMatchesAggregator()._create_test_data(tmp_path)
        agg = MatchesAggregator(data_root=data_root)
        result = agg.aggregate()
        assert "effective_match_date" in result.columns
        assert result["effective_match_date"].null_count() == 0

    def test_sorted_by_effective_match_date(self, tmp_path):
        """Output should be sorted by effective_match_date."""
        from mvp.atptour.aggregators.matches import MatchesAggregator

        data_root = TestMatchesAggregator()._create_test_data(tmp_path)
        agg = MatchesAggregator(data_root=data_root)
        result = agg.aggregate()
        expected = result.sort(
            ["effective_match_date", "draw_type", "match_uid", "player_id"],
            nulls_last=True,
        )
        assert result["match_uid"].to_list() == expected["match_uid"].to_list()
        assert result["player_id"].to_list() == expected["player_id"].to_list()
