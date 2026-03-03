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
        """Should aggregate point data to match level with p1_/p2_ columns."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert len(result) == 1
        assert result["total_points"][0] == 10
        assert result["p1_points_won"][0] == 5
        assert result["p2_points_won"][0] == 5

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

        # P1 served 5 points (server=="1", points 0-4)
        assert result["p1_service_points"][0] == 5
        # P1 first serves: points with serve==1 when server=="1" = points 0,1,2,4
        assert result["p1_first_serve_points"][0] == 4
        # P1 first serve won: serve==1, server=="1", scorer=="1" = points 0(A), 1(W)
        assert result["p1_first_serve_won"][0] == 2
        # P1 second serve: serve==2 when server=="1" = point 3
        assert result["p1_second_serve_points"][0] == 1
        # P1 second serve won: serve==2, server=="1", scorer=="1" = 0 (point 3 scorer=2)
        assert result["p1_second_serve_won"][0] == 0
        # P1 aces: result=="A" when server=="1" = point 0
        assert result["p1_aces"][0] == 1
        # P1 DFs: result=="DF" when server=="1" = point 4
        assert result["p1_dfs"][0] == 1

        # P2 served 5 points (server=="2", points 5-9)
        assert result["p2_service_points"][0] == 5
        assert result["p2_first_serve_points"][0] == 4
        assert result["p2_first_serve_won"][0] == 2
        assert result["p2_second_serve_points"][0] == 1
        assert result["p2_second_serve_won"][0] == 0
        assert result["p2_aces"][0] == 0
        assert result["p2_dfs"][0] == 0

    def test_aggregates_return_stats(self, tmp_path):
        """Should compute return statistics per player."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # P1 return points = when P2 is serving = 5
        assert result["p1_return_points"][0] == 5
        # P1 return points won: server=="2", scorer=="1" = points 5,7,8
        assert result["p1_return_points_won"][0] == 3
        # P2 return points = when P1 is serving = 5
        assert result["p2_return_points"][0] == 5
        # P2 return points won: server=="1", scorer=="2" = points 2,3,4
        assert result["p2_return_points_won"][0] == 3

    def test_aggregates_break_points(self, tmp_path):
        """Should compute break point stats."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # is_break_point is True for points 2 and 6
        # Point 2: server="1", scorer="2" -> P1 faced BP, didn't save
        assert result["p1_bp_faced"][0] == 1
        assert result["p1_bp_saved"][0] == 0
        # Point 6: server="2", scorer="2" -> P1 had BP opportunity, didn't convert
        assert result["p1_bp_opportunities"][0] == 1
        assert result["p1_bp_converted"][0] == 0
        # Point 6: server="2", scorer="2" -> P2 faced BP, saved it
        assert result["p2_bp_faced"][0] == 1
        assert result["p2_bp_saved"][0] == 1
        # Point 2: server="1", scorer="2" -> P2 had BP opportunity, converted
        assert result["p2_bp_opportunities"][0] == 1
        assert result["p2_bp_converted"][0] == 1

    def test_aggregates_winners_and_errors(self, tmp_path):
        """Should compute winners/errors per player.

        Winners are attributed to the scorer (who hit the winning shot).
        Errors are attributed to the player who LOST the point (the one
        who made the error), not the scorer.
        """
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # P1 winners: scorer=="1" & result=="W" = points 1,5
        assert result["p1_winners"][0] == 2
        # P1 UEs: scorer=="2" & result=="UE" = point 2 (P1 made UE, P2 won)
        assert result["p1_ues"][0] == 1
        # P1 FEs: scorer=="2" & result=="FE" = point 3 (P1 made FE, P2 won)
        assert result["p1_fes"][0] == 1

        # P2 winners: scorer=="2" & result=="W" = point 6
        assert result["p2_winners"][0] == 1
        # P2 UEs: scorer=="1" & result=="UE" = point 7 (P2 made UE, P1 won)
        assert result["p2_ues"][0] == 1
        # P2 FEs: scorer=="1" & result=="FE" = point 8 (P2 made FE, P1 won)
        assert result["p2_fes"][0] == 1

    def test_aggregates_serve_speed(self, tmp_path):
        """Should compute serve speed averages and maxes."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # P1 1st serve speeds (server="1", serve=1): 200, 190, 185, None = [200, 190, 185]
        assert result["p1_avg_1st_serve_speed"][0] == pytest.approx(191.67, abs=0.01)
        assert result["p1_max_1st_serve_speed"][0] == 200.0
        # P1 2nd serve speed (server="1", serve=2): 150
        assert result["p1_avg_2nd_serve_speed"][0] == 150.0
        # P1 fault serve speed (server="1"): 180 (only non-null)
        assert result["p1_avg_fault_serve_speed"][0] == 180.0
        assert result["p1_max_fault_serve_speed"][0] == 180.0

        # P2 1st serve speeds (server="2", serve=1): 195, 188, None, 192 = [195, 188, 192]
        assert result["p2_avg_1st_serve_speed"][0] == pytest.approx(191.67, abs=0.01)
        assert result["p2_max_1st_serve_speed"][0] == 195.0
        # P2 2nd serve speed (server="2", serve=2): 145
        assert result["p2_avg_2nd_serve_speed"][0] == 145.0
        # P2 fault serve speed (server="2"): 160 (only non-null)
        assert result["p2_avg_fault_serve_speed"][0] == 160.0
        assert result["p2_max_fault_serve_speed"][0] == 160.0

    def test_aggregates_rally_stats(self, tmp_path):
        """Should compute rally length statistics."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # rally_length_missing is True for indices 0 and 4, so 8 points have data
        assert result["rally_points_with_data"][0] == 8

        # Rally lengths (p1+p2 shots) for non-missing points:
        #   idx1: 2+2=4, idx2: 3+3=6, idx3: 4+4=8
        #   idx5: 1+1=2, idx6: 2+2=4, idx7: 3+3=6, idx8: 2+2=4, idx9: 1+1=2
        # Short (<=4): 4, 2, 4, 4, 2 = 5
        assert result["rally_short_count"][0] == 5
        # Medium (5-8): 6, 8, 6 = 3
        assert result["rally_medium_count"][0] == 3
        # Long (>=9): 0
        assert result["rally_long_count"][0] == 0
        # Total shots: 4+6+8+2+4+6+4+2 = 36
        assert result["rally_total_shots"][0] == 36

        # Rally by outcome - P1 (scorer=="1" & !missing)
        # P1 won: idx1(4), idx5(2), idx7(6), idx8(4) = 4 points, 16 shots
        assert result["p1_rally_won_count"][0] == 4
        assert result["p1_rally_won_shots"][0] == 16
        # P1 lost (scorer=="2" & !missing): idx2(6), idx3(8), idx6(4), idx9(2) = 4 points, 20 shots
        assert result["p1_rally_lost_count"][0] == 4
        assert result["p1_rally_lost_shots"][0] == 20

        # Rally by serve context - P1 serving (server=="1" & !missing)
        # idx1(4), idx2(6), idx3(8) = 3 points, 18 shots
        assert result["p1_rally_serving_count"][0] == 3
        assert result["p1_rally_serving_shots"][0] == 18
        # P1 returning (server=="2" & !missing)
        # idx5(2), idx6(4), idx7(6), idx8(4), idx9(2) = 5 points, 18 shots
        assert result["p1_rally_returning_count"][0] == 5
        assert result["p1_rally_returning_shots"][0] == 18

        # Rally by outcome - P2 (mirrors P1)
        assert result["p2_rally_won_count"][0] == 4
        assert result["p2_rally_won_shots"][0] == 20
        assert result["p2_rally_lost_count"][0] == 4
        assert result["p2_rally_lost_shots"][0] == 16

        # Rally by serve context - P2
        assert result["p2_rally_serving_count"][0] == 5
        assert result["p2_rally_serving_shots"][0] == 18
        assert result["p2_rally_returning_count"][0] == 3
        assert result["p2_rally_returning_shots"][0] == 18

    def test_aggregates_clutch_stats(self, tmp_path):
        """Should compute crucial and tiebreak point stats."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # is_crucial_point is True for points 2 and 6
        assert result["p1_crucial_points_played"][0] == 2
        # Point 2: scorer="2" (P1 lost), Point 6: scorer="2" (P1 lost)
        assert result["p1_crucial_points_won"][0] == 0
        assert result["p2_crucial_points_won"][0] == 2

        # is_tiebreak is False for all points
        assert result["p1_tiebreak_points_played"][0] == 0
        assert result["p1_tiebreak_points_won"][0] == 0

    def test_aggregates_game_quality(self, tmp_path):
        """Should compute game quality stats (deduped by game)."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        # Game 1: server="1", easy_hold=True, difficult_hold=False, multiple_deuces=False
        # Game 2: server="2", easy_hold=False, difficult_hold=True, multiple_deuces=False
        assert result["p1_easy_holds"][0] == 1
        assert result["p1_difficult_holds"][0] == 0
        assert result["p2_easy_holds"][0] == 0
        assert result["p2_difficult_holds"][0] == 1

        assert result["p1_games_multiple_deuces"][0] == 0
        assert result["p2_games_multiple_deuces"][0] == 0

        # Games won
        assert result["p1_games_won"][0] == 1
        assert result["p2_games_won"][0] == 1

    def test_aggregates_match_context(self, tmp_path):
        """Should compute match duration and sets played/won."""
        _write_match_beats_parquet(tmp_path, points=_make_singles_points())

        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()

        assert result["match_duration"][0] == 600
        assert result["sets_played"][0] == 1
        assert result["p1_sets_won"][0] == 1
        assert result["p2_sets_won"][0] == 0

    def test_returns_none_when_no_data(self, tmp_path):
        """Should return None when no staged data exists."""
        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        assert result is None
