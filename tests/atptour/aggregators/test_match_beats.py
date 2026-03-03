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

    def test_returns_none_when_no_data(self, tmp_path):
        """Should return None when no staged data exists."""
        agg = MatchBeatsAggregator(data_root=tmp_path)
        result = agg.run()
        assert result is None
