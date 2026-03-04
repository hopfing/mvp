"""Tests for MatchBeatsAggregator."""

from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.aggregators.match_beats import MatchBeatsAggregator


def _write_match_beats_parquet(
    data_root: Path,
    circuit: str = "tour",
    tid: str = "999",
    year: int = 2025,
    *,
    points: pl.DataFrame | None = None,
) -> Path:
    """Write a match_beats parquet to the expected staging path."""
    path = (
        data_root
        / "stage"
        / "atptour"
        / "tournaments"
        / circuit
        / tid
        / str(year)
        / "match_beats.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if points is not None:
        points.write_parquet(path)
    return path


def _make_singles_points() -> pl.DataFrame:
    """Create a DataFrame of 10 points for a singles match."""
    return pl.DataFrame({
        "tournament_id": ["999"] * 10,
        "year": [2025] * 10,
        "match_id": ["MS001"] * 10,
        "is_doubles": [False] * 10,
        "p1_id": ["PLAYER1"] * 10,
        "p2_id": ["PLAYER2"] * 10,
        "set_num": [1] * 10,
        "set_winner": ["1"] * 10,
        "game_num": [1] * 5 + [2] * 5,
        "game_winner": ["1"] * 5 + ["2"] * 5,
        "game_duration": [120] * 10,
        "easy_hold": [True] * 5 + [False] * 5,
        "difficult_hold": [False] * 5 + [True] * 5,
        "multiple_deuces": [False] * 10,
        "is_tiebreak": [False] * 10,
        "point_num": list(range(1, 6)) * 2,
        "point_id": [f"P{i}" for i in range(10)],
        "result": ["A", "W", "UE", "FE", "DF", "W", "W", "UE", "FE", "N"],
        "scorer": ["1", "1", "2", "2", "2", "1", "2", "1", "1", "2"],
        "server": ["1"] * 5 + ["2"] * 5,
        "serve": [1, 1, 1, 2, 1, 1, 1, 2, 1, 1],
        "serve_speed": [200.0, 190.0, 185.0, 150.0, None, 195.0, 188.0, 145.0, None, 192.0],
        "fault_serve_speed": [None, None, None, 180.0, None, None, None, 160.0, None, None],
        "p1_rally_shots": [0, 2, 3, 4, 0, 1, 2, 3, 2, 1],
        "p2_rally_shots": [0, 2, 3, 4, 0, 1, 2, 3, 2, 1],
        "rally_length_missing": [True, False, False, False, True, False, False, False, False, False],
        "is_break_point": [False, False, True, False, False, False, True, False, False, False],
        "break_points_in_game": [0] * 10,
        "break_points_lost": [0] * 10,
        "is_crucial_point": [False, False, True, False, False, False, True, False, False, False],
        "p1_game_score": ["0"] * 10,
        "p2_game_score": ["0"] * 10,
        "match_duration_at_point": [60, 120, 180, 240, 300, 360, 420, 480, 540, 600],
        "source_file": ["test.json"] * 10,
        "parsed_at": [None] * 10,
        "schema_hash": ["test"] * 10,
    })


def _make_doubles_points() -> pl.DataFrame:
    """Create a DataFrame of 2 points for a doubles match."""
    return pl.DataFrame({
        "tournament_id": ["999"] * 2,
        "year": [2025] * 2,
        "match_id": ["MD001"] * 2,
        "is_doubles": [True] * 2,
        "p1_id": ["PLAYER1"] * 2,
        "p2_id": ["PLAYER2"] * 2,
        "set_num": [1] * 2,
        "set_winner": ["1"] * 2,
        "game_num": [1] * 2,
        "game_winner": ["1"] * 2,
        "game_duration": [120] * 2,
        "easy_hold": [False] * 2,
        "difficult_hold": [False] * 2,
        "multiple_deuces": [False] * 2,
        "is_tiebreak": [False] * 2,
        "point_num": [1, 2],
        "point_id": ["P1", "P2"],
        "result": ["W", "W"],
        "scorer": ["1", "2"],
        "server": ["1", "2"],
        "serve": [1, 1],
        "serve_speed": [None, None],
        "fault_serve_speed": [None, None],
        "p1_rally_shots": [2, 3],
        "p2_rally_shots": [2, 3],
        "rally_length_missing": [False, False],
        "is_break_point": [False, False],
        "break_points_in_game": [0, 0],
        "break_points_lost": [0, 0],
        "is_crucial_point": [False, False],
        "p1_game_score": ["0", "0"],
        "p2_game_score": ["0", "0"],
        "match_duration_at_point": [60, 120],
        "source_file": ["test.json"] * 2,
        "parsed_at": [None] * 2,
        "schema_hash": ["test"] * 2,
    })


class TestMatchBeatsAggregator:
    """Tests for MatchBeatsAggregator."""

    def test_aggregator_initializes(self, tmp_path):
        """Should initialize with data_root."""
        agg = MatchBeatsAggregator(data_root=tmp_path)
        assert agg.data_root == tmp_path

    def test_aggregates_points_to_match_level(self, tmp_path):
        """Should aggregate point data to match level with player_/opp_ columns."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert len(result) == 2
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        assert p1_row["total_points"][0] == 10
        assert p1_row["player_points_won"][0] == 5
        assert p1_row["opp_points_won"][0] == 5

    def test_filters_doubles(self, tmp_path):
        """Should filter out doubles matches."""
        _write_match_beats_parquet(tmp_path, points=_make_doubles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        assert result is None

    def test_aggregates_serve_stats(self, tmp_path):
        """Should compute serve statistics per player."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # P1 served 5 points (server=="1", points 0-4)
        assert p1_row["player_service_points"][0] == 5
        # P1 first serves: points with serve==1 when server=="1" = points 0,1,2,4
        assert p1_row["player_first_serve_points"][0] == 4
        # P1 first serve won: serve==1, server=="1", scorer=="1" = points 0(A), 1(W)
        assert p1_row["player_first_serve_won"][0] == 2
        # P1 second serve: serve==2 when server=="1" = point 3
        assert p1_row["player_second_serve_points"][0] == 1
        # P1 second serve won: serve==2, server=="1", scorer=="1" = 0 (point 3 scorer=2)
        assert p1_row["player_second_serve_won"][0] == 0
        # P1 aces: result=="A" when server=="1" = point 0
        assert p1_row["player_aces"][0] == 1
        # P1 DFs: result=="DF" when server=="1" = point 4
        assert p1_row["player_dfs"][0] == 1

        # P2 served 5 points (server=="2", points 5-9)
        assert p2_row["player_service_points"][0] == 5
        assert p2_row["player_first_serve_points"][0] == 4
        assert p2_row["player_first_serve_won"][0] == 2
        assert p2_row["player_second_serve_points"][0] == 1
        assert p2_row["player_second_serve_won"][0] == 0
        assert p2_row["player_aces"][0] == 0
        assert p2_row["player_dfs"][0] == 0

    def test_aggregates_return_stats(self, tmp_path):
        """Should compute return statistics per player."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # P1 return points = when P2 is serving = 5
        assert p1_row["player_return_points"][0] == 5
        # P1 return points won: server=="2", scorer=="1" = points 5,7,8
        assert p1_row["player_return_points_won"][0] == 3
        # P2 return points = when P1 is serving = 5
        assert p2_row["player_return_points"][0] == 5
        # P2 return points won: server=="1", scorer=="2" = points 2,3,4
        assert p2_row["player_return_points_won"][0] == 3

    def test_aggregates_break_points(self, tmp_path):
        """Should compute break point stats."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # is_break_point is True for points 2 and 6
        # Point 2: server="1", scorer="2" -> P1 faced BP, didn't save
        assert p1_row["player_bp_faced"][0] == 1
        assert p1_row["player_bp_saved"][0] == 0
        # Point 6: server="2", scorer="2" -> P1 had BP opportunity, didn't convert
        assert p1_row["player_bp_opportunities"][0] == 1
        assert p1_row["player_bp_converted"][0] == 0
        # Point 6: server="2", scorer="2" -> P2 faced BP, saved it
        assert p2_row["player_bp_faced"][0] == 1
        assert p2_row["player_bp_saved"][0] == 1
        # Point 2: server="1", scorer="2" -> P2 had BP opportunity, converted
        assert p2_row["player_bp_opportunities"][0] == 1
        assert p2_row["player_bp_converted"][0] == 1

    def test_aggregates_winners_and_errors(self, tmp_path):
        """Should compute winners/errors per player.

        Winners are attributed to the scorer (who hit the winning shot).
        Errors are attributed to the player who LOST the point (the one
        who made the error), not the scorer.
        """
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # P1 winners: scorer=="1" & result=="W" = points 1,5
        assert p1_row["player_winners"][0] == 2
        # P1 UEs: scorer=="2" & result=="UE" = point 2 (P1 made UE, P2 won)
        assert p1_row["player_ues"][0] == 1
        # P1 FEs: scorer=="2" & result=="FE" = point 3 (P1 made FE, P2 won)
        assert p1_row["player_fes"][0] == 1

        # P2 winners: scorer=="2" & result=="W" = point 6
        assert p2_row["player_winners"][0] == 1
        # P2 UEs: scorer=="1" & result=="UE" = point 7 (P2 made UE, P1 won)
        assert p2_row["player_ues"][0] == 1
        # P2 FEs: scorer=="1" & result=="FE" = point 8 (P2 made FE, P1 won)
        assert p2_row["player_fes"][0] == 1

    def test_aggregates_serve_speed(self, tmp_path):
        """Should compute serve speed averages and maxes."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # P1 1st serve speeds (server="1", serve=1): 200, 190, 185, None = [200, 190, 185]
        assert p1_row["player_avg_1st_serve_speed"][0] == pytest.approx(191.67, abs=0.01)
        assert p1_row["player_max_1st_serve_speed"][0] == 200.0
        # P1 2nd serve speed (server="1", serve=2): 150
        assert p1_row["player_avg_2nd_serve_speed"][0] == 150.0
        # P1 fault serve speed (server="1"): 180 (only non-null)
        assert p1_row["player_avg_fault_serve_speed"][0] == 180.0
        assert p1_row["player_max_fault_serve_speed"][0] == 180.0

        # P2 1st serve speeds (server="2", serve=1): 195, 188, None, 192 = [195, 188, 192]
        assert p2_row["player_avg_1st_serve_speed"][0] == pytest.approx(191.67, abs=0.01)
        assert p2_row["player_max_1st_serve_speed"][0] == 195.0
        # P2 2nd serve speed (server="2", serve=2): 145
        assert p2_row["player_avg_2nd_serve_speed"][0] == 145.0
        # P2 fault serve speed (server="2"): 160 (only non-null)
        assert p2_row["player_avg_fault_serve_speed"][0] == 160.0
        assert p2_row["player_max_fault_serve_speed"][0] == 160.0

    def test_aggregates_rally_stats(self, tmp_path):
        """Should compute rally length statistics."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # rally_length_missing is True for indices 0 and 4, so 8 points have data
        assert p1_row["rally_points_with_data"][0] == 8

        # Rally lengths (p1+p2 shots) for non-missing points:
        #   idx1: 2+2=4, idx2: 3+3=6, idx3: 4+4=8
        #   idx5: 1+1=2, idx6: 2+2=4, idx7: 3+3=6, idx8: 2+2=4, idx9: 1+1=2
        # Short (<=4): 4, 2, 4, 4, 2 = 5
        assert p1_row["rally_short_count"][0] == 5
        # Medium (5-8): 6, 8, 6 = 3
        assert p1_row["rally_medium_count"][0] == 3
        # Long (>=9): 0
        assert p1_row["rally_long_count"][0] == 0
        # Total shots: 4+6+8+2+4+6+4+2 = 36
        assert p1_row["rally_total_shots"][0] == 36

        # Rally by outcome - P1 (scorer=="1" & !missing)
        # P1 won: idx1(4), idx5(2), idx7(6), idx8(4) = 4 points, 16 shots
        assert p1_row["player_rally_won_count"][0] == 4
        assert p1_row["player_rally_won_shots"][0] == 16
        # P1 lost (scorer=="2" & !missing): idx2(6), idx3(8), idx6(4), idx9(2) = 4 points, 20 shots
        assert p1_row["player_rally_lost_count"][0] == 4
        assert p1_row["player_rally_lost_shots"][0] == 20

        # Rally by serve context - P1 serving (server=="1" & !missing)
        # idx1(4), idx2(6), idx3(8) = 3 points, 18 shots
        assert p1_row["player_rally_serving_count"][0] == 3
        assert p1_row["player_rally_serving_shots"][0] == 18
        # P1 returning (server=="2" & !missing)
        # idx5(2), idx6(4), idx7(6), idx8(4), idx9(2) = 5 points, 18 shots
        assert p1_row["player_rally_returning_count"][0] == 5
        assert p1_row["player_rally_returning_shots"][0] == 18

        # Rally by outcome - P2 (mirrors P1)
        assert p2_row["player_rally_won_count"][0] == 4
        assert p2_row["player_rally_won_shots"][0] == 20
        assert p2_row["player_rally_lost_count"][0] == 4
        assert p2_row["player_rally_lost_shots"][0] == 16

        # Rally by serve context - P2
        assert p2_row["player_rally_serving_count"][0] == 5
        assert p2_row["player_rally_serving_shots"][0] == 18
        assert p2_row["player_rally_returning_count"][0] == 3
        assert p2_row["player_rally_returning_shots"][0] == 18

    def test_aggregates_clutch_stats(self, tmp_path):
        """Should compute crucial and tiebreak point stats."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # is_crucial_point is True for points 2 and 6
        assert p1_row["player_crucial_points_played"][0] == 2
        # Point 2: scorer="2" (P1 lost), Point 6: scorer="2" (P1 lost)
        assert p1_row["player_crucial_points_won"][0] == 0
        assert p2_row["player_crucial_points_won"][0] == 2

        # is_tiebreak is False for all points
        assert p1_row["player_tiebreak_points_played"][0] == 0
        assert p1_row["player_tiebreak_points_won"][0] == 0

    def test_aggregates_game_quality(self, tmp_path):
        """Should compute game quality stats (deduped by game)."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        # Game 1: server="1", easy_hold=True, difficult_hold=False, multiple_deuces=False
        # Game 2: server="2", easy_hold=False, difficult_hold=True, multiple_deuces=False
        assert p1_row["player_easy_holds"][0] == 1
        assert p1_row["player_difficult_holds"][0] == 0
        assert p2_row["player_easy_holds"][0] == 0
        assert p2_row["player_difficult_holds"][0] == 1

        assert p1_row["player_games_multiple_deuces"][0] == 0
        assert p2_row["player_games_multiple_deuces"][0] == 0

        # Games won
        assert p1_row["player_games_won"][0] == 1
        assert p2_row["player_games_won"][0] == 1

    def test_aggregates_match_context(self, tmp_path):
        """Should compute match duration and sets played/won."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")

        assert p1_row["match_duration"][0] == 600
        assert p1_row["sets_played"][0] == 1
        assert p1_row["player_sets_won"][0] == 1
        assert p2_row["player_sets_won"][0] == 0

    def test_pivots_to_player_match_level(self, tmp_path):
        """Should output two rows per match with player_/opp_ columns."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # Should have 2 rows (one per player perspective)
        assert len(result) == 2

        # Should have player_id and opp_id columns
        assert "player_id" in result.columns
        assert "opp_id" in result.columns

        # Should NOT have p1_id or p2_id columns
        assert "p1_id" not in result.columns
        assert "p2_id" not in result.columns

        # Should have player_/opp_ prefixed columns, not p1_/p2_
        assert "player_points_won" in result.columns
        assert "opp_points_won" in result.columns
        assert "p1_points_won" not in result.columns

        # Player perspective should swap
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")
        assert p1_row["player_points_won"][0] == p2_row["opp_points_won"][0]
        assert p1_row["opp_points_won"][0] == p2_row["player_points_won"][0]

        # Shared columns should be the same
        assert p1_row["total_points"][0] == p2_row["total_points"][0]
        assert p1_row["match_duration"][0] == p2_row["match_duration"][0]

    def test_aggregates_max_2nd_serve_speed(self, tmp_path):
        """Should compute max 2nd serve speed per player."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())
        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")
        # P1 2nd serve (server="1", serve=2): speed=150 → max=150
        assert p1_row["player_max_2nd_serve_speed"][0] == 150.0
        # P2 2nd serve (server="2", serve=2): speed=145 → max=145
        assert p2_row["player_max_2nd_serve_speed"][0] == 145.0

    def test_aggregates_std_1st_serve_speed(self, tmp_path):
        """Should compute std dev of 1st serve speeds per player."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())
        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        # P1 1st serve speeds (server="1", serve=1): [200, 190, 185] → std ≈ 7.64
        assert p1_row["player_std_1st_serve_speed"][0] == pytest.approx(7.64, abs=0.01)

    def test_aggregates_service_games(self, tmp_path):
        """Should count service games per player."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())
        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        p1_row = result.filter(pl.col("player_id") == "PLAYER1")
        p2_row = result.filter(pl.col("player_id") == "PLAYER2")
        # Game 1: server="1" (point_num=1 exists) → p1 has 1 service game
        # Game 2: server="2" (point_num=1 exists) → p2 has 1 service game
        assert p1_row["player_service_games"][0] == 1
        assert p2_row["player_service_games"][0] == 1

    def test_returns_none_when_no_data(self, tmp_path):
        """Should return None when no staged data exists."""
        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        assert result is None
