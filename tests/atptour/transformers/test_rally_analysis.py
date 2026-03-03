"""Tests for rally_analysis transformer."""

import json
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.schemas.rally_analysis import SCHEMA_HASH
from mvp.atptour.transformers.rally_analysis import RallyAnalysisTransformer
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit


@pytest.fixture
def tournament():
    return Tournament(
        tournament_id="339",
        year=2023,
        circuit=Circuit.tour,
        location="Indian Wells, USA",
    )


def _make_category(name, t1w=0, t1e=0, t2w=0, t2e=0):
    """Create a rally_analysis category with the given point counts."""
    return {
        "name": name,
        "t1win": [{"pointId": f"pt_{i}", "pointEndType": "WINNER"} for i in range(t1w)],
        "t1err": [{"pointId": f"pt_{i}", "pointEndType": "UNFORCED ERROR"} for i in range(t1e)],
        "t2win": [{"pointId": f"pt_{i}", "pointEndType": "WINNER"} for i in range(t2w)],
        "t2err": [{"pointId": f"pt_{i}", "pointEndType": "UNFORCED ERROR"} for i in range(t2e)],
    }


CATEGORY_NAMES = [
    "Serve", "Return", "3rd shot", "4th shot",
    "5th shot", "6th shot", "7th shot", "8th shot",
    "9+ odd shots", "10+ even shots",
]


def _make_empty_rally_data():
    """Create 10 empty rally categories."""
    return [_make_category(name) for name in CATEGORY_NAMES]


def _make_match_data(
    match_completed=True, is_doubles=False, points_missing=False,
    rally_data=None,
):
    """Create a full rally_analysis JSON structure."""
    if rally_data is None:
        rally_data = _make_empty_rally_data()

    return {
        "setsCompleted": 2,
        "matchCompleted": match_completed,
        "isDoubles": is_doubles,
        "maxSets": 3,
        "playerDetails": [
            {"seed": "", "player1Name": "P. One", "player1Id": "A123",
             "player1Country": "USA", "player2Name": None, "player2Id": None, "player2Country": None},
            {"seed": "", "player1Name": "P. Two", "player1Id": "B456",
             "player1Country": "GBR", "player2Name": None, "player2Id": None, "player2Country": None},
        ],
        "pointsMissing": points_missing,
        "rallyData": rally_data,
    }


def _write_json(tmp_path, tournament, filename, data):
    """Write JSON file in the expected raw directory structure."""
    raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "rally_analysis"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / filename
    path.write_text(json.dumps(data))
    return path


class TestRallyAnalysisTransformer:
    def test_basic_transform(self, tmp_path, tournament):
        rally_data = _make_empty_rally_data()
        # Short: Serve(0) + Return(1) + 3rd(2) + 4th(3)
        rally_data[0] = _make_category("Serve", t1w=4, t1e=1, t2w=3, t2e=1)
        rally_data[1] = _make_category("Return", t1w=3, t1e=20, t2w=1, t2e=11)

        data = _make_match_data(rally_data=rally_data)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        assert output.exists()
        df = pl.read_parquet(output)
        assert len(df) == 1

        row = df.row(0, named=True)
        assert row["tournament_id"] == "339"
        assert row["year"] == 2023
        assert row["match_id"] == "MS001"
        assert row["p1_id"] == "A123"
        assert row["p2_id"] == "B456"
        # Serve + Return both short
        assert row["p1_short_won"] == 7   # 4+3
        assert row["p1_short_err"] == 21  # 1+20
        assert row["p2_short_won"] == 4   # 3+1
        assert row["p2_short_err"] == 12  # 1+11

    def test_rally_length_buckets(self, tmp_path, tournament):
        """Each category maps to the right rally length bucket."""
        rally_data = _make_empty_rally_data()
        # Short: indices 0-3
        rally_data[0] = _make_category("Serve", t1w=1)
        rally_data[3] = _make_category("4th shot", t1w=2)
        # Medium: indices 4-7
        rally_data[4] = _make_category("5th shot", t1w=3)
        rally_data[7] = _make_category("8th shot", t1w=4)
        # Long: indices 8-9
        rally_data[8] = _make_category("9+ odd shots", t1w=5)
        rally_data[9] = _make_category("10+ even shots", t1w=6)

        data = _make_match_data(rally_data=rally_data)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        )
        row = df.row(0, named=True)
        assert row["p1_short_won"] == 3   # 1+2
        assert row["p1_medium_won"] == 7  # 3+4
        assert row["p1_long_won"] == 11   # 5+6

    def test_unclassified_category(self, tmp_path, tournament):
        rally_data = _make_empty_rally_data()
        rally_data.append(_make_category("UNCLASSIFIED", t1e=4, t2e=3))

        data = _make_match_data(rally_data=rally_data)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        )
        row = df.row(0, named=True)
        assert row["p1_unclassified_won"] == 0
        assert row["p1_unclassified_err"] == 4
        assert row["p2_unclassified_won"] == 0
        assert row["p2_unclassified_err"] == 3

    def test_points_missing_flag(self, tmp_path, tournament):
        data = _make_match_data(points_missing=True)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        )
        assert df.row(0, named=True)["points_missing"] is True

    def test_skips_incomplete_match(self, tmp_path, tournament):
        data = _make_match_data(match_completed=False)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        assert not output.exists()

    def test_doubles_still_transformed(self, tmp_path, tournament):
        """Doubles matches are transformed; filtering happens at aggregator."""
        data = _make_match_data(is_doubles=True)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        assert output.exists()
        df = pl.read_parquet(output)
        assert df.row(0, named=True)["is_doubles"] is True

    def test_multiple_matches(self, tmp_path, tournament):
        rally1 = _make_empty_rally_data()
        rally1[0] = _make_category("Serve", t1w=5)
        rally2 = _make_empty_rally_data()
        rally2[0] = _make_category("Serve", t1w=10)

        _write_json(tmp_path, tournament, "MS001.json", _make_match_data(rally_data=rally1))
        _write_json(tmp_path, tournament, "MS002.json", _make_match_data(rally_data=rally2))

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        )
        assert len(df) == 2

    def test_no_data_directory(self, tmp_path, tournament):
        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()  # should not raise

    def test_schema_hash_added(self, tmp_path, tournament):
        data = _make_match_data()
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        )
        assert df.row(0, named=True)["schema_hash"] == SCHEMA_HASH

    def test_player_ids_uppercased(self, tmp_path, tournament):
        data = _make_match_data()
        data["playerDetails"][0]["player1Id"] = "a123"
        data["playerDetails"][1]["player1Id"] = "b456"
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = RallyAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "rally_analysis.parquet"
        )
        row = df.row(0, named=True)
        assert row["p1_id"] == "A123"
        assert row["p2_id"] == "B456"
