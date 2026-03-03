"""Tests for stroke_analysis transformer."""

import json
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.schemas.stroke_analysis import SCHEMA_HASH
from mvp.atptour.transformers.stroke_analysis import StrokeAnalysisTransformer
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


def _make_shot_type_entry(name, p1w=0, p1f=0, p1u=0, p1o=0, p2w=0, p2f=0, p2u=0, p2o=0):
    """Create a single shot type entry for allPoints."""
    return {
        "player1": 0,
        "player2": 0,
        "name": name,
        "player1Wins": p1w,
        "player1Frcs": p1f,
        "player1Unfs": p1u,
        "player1Others": p1o,
        "player2Wins": p2w,
        "player2Frcs": p2f,
        "player2Unfs": p2u,
        "player2Others": p2o,
        "player1Points": {"winners": [], "forcedErrors": [], "unforcedErrors": [], "others": []},
        "player2Points": {"winners": [], "forcedErrors": [], "unforcedErrors": [], "others": []},
        "player1RetUnfs": 0,
        "player2RetUnfs": 0,
        "player1RetFrcs": 0,
        "player2RetFrcs": 0,
    }


SHOT_NAMES = [
    "Ground Stroke", "Overhead Shots", "Passing Shots", "Volley Shots",
    "Approach Shots", "Drop Shots", "Lob Shots",
]


def _make_empty_group():
    """Create 7 shot type entries with all zeros."""
    return [_make_shot_type_entry(name) for name in SHOT_NAMES]


def _make_tpc_entry(p1_fh_w=0, p1_fh_f=0, p1_fh_u=0, p2_fh_w=0, p2_fh_f=0, p2_fh_u=0,
                     p1_bh_w=0, p1_bh_f=0, p1_bh_u=0, p2_bh_w=0, p2_bh_f=0, p2_bh_u=0):
    """Create a totalPointsCount entry."""
    return {
        "forehand": {
            "player1": 0, "player2": 0,
            "player1Wins": p1_fh_w, "player1Frcs": p1_fh_f, "player1Unfs": p1_fh_u, "player1Others": 0,
            "player2Wins": p2_fh_w, "player2Frcs": p2_fh_f, "player2Unfs": p2_fh_u, "player2Others": 0,
            "player1RetUnfs": 0, "player2RetUnfs": 0, "player1RetFrcs": 0, "player2RetFrcs": 0,
        },
        "backhand": {
            "player1": 0, "player2": 0,
            "player1Wins": p1_bh_w, "player1Frcs": p1_bh_f, "player1Unfs": p1_bh_u, "player1Others": 0,
            "player2Wins": p2_bh_w, "player2Frcs": p2_bh_f, "player2Unfs": p2_bh_u, "player2Others": 0,
            "player1RetUnfs": 0, "player2RetUnfs": 0, "player1RetFrcs": 0, "player2RetFrcs": 0,
        },
    }


def _make_match_data(sets_completed=2, is_doubles=False, match_completed=True,
                     tpc_entry=None, all_points_groups=None):
    """Create a full stroke_analysis JSON structure."""
    if tpc_entry is None:
        tpc_entry = _make_tpc_entry()
    if all_points_groups is None:
        all_points_groups = [_make_empty_group() for _ in range(sets_completed)]

    # Flatten groups into single forehand/backhand arrays
    fh = []
    bh = []
    for group in all_points_groups:
        fh.extend(group)
        bh.extend(_make_empty_group())

    all_points = [{"forehand": fh, "backhand": bh}]
    # Add empty entries for [1] through [setsCompleted]
    for _ in range(sets_completed):
        all_points.append({"forehand": [], "backhand": []})

    tpc = [tpc_entry]
    for _ in range(sets_completed):
        tpc.append(_make_tpc_entry())

    return {
        "courtId": 1,
        "matchCompleted": match_completed,
        "isDoubles": is_doubles,
        "setsCompleted": sets_completed,
        "players": [
            {"seed": "1", "player1Name": "P. One", "player1Id": "A123", "player1Country": "USA", "player1Hand": ""},
            {"seed": "2", "player1Name": "P. Two", "player1Id": "B456", "player1Country": "GBR", "player1Hand": ""},
        ],
        "rallyShots": {
            "allPoints": all_points,
            "crucialPoints": [],
            "totalPointsCount": tpc,
        },
    }


def _write_json(tmp_path, tournament, filename, data):
    """Write JSON file in the expected raw directory structure."""
    raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "stroke_analysis"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / filename
    path.write_text(json.dumps(data))
    return path


class TestStrokeAnalysisTransformer:
    def test_basic_transform(self, tmp_path, tournament):
        data = _make_match_data(
            tpc_entry=_make_tpc_entry(p1_fh_w=10, p1_bh_w=5, p2_fh_w=8, p2_bh_w=3),
        )
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        assert output.exists()
        df = pl.read_parquet(output)
        assert len(df) == 1

        row = df.row(0, named=True)
        assert row["tournament_id"] == "339"
        assert row["year"] == 2023
        assert row["match_id"] == "MS001"
        assert row["p1_id"] == "A123"
        assert row["p2_id"] == "B456"
        assert row["p1_fh_winners"] == 10
        assert row["p1_bh_winners"] == 5
        assert row["p2_fh_winners"] == 8
        assert row["p2_bh_winners"] == 3

    def test_total_points_count_extraction(self, tmp_path, tournament):
        data = _make_match_data(
            tpc_entry=_make_tpc_entry(
                p1_fh_w=16, p1_fh_f=16, p1_fh_u=8,
                p1_bh_w=3, p1_bh_f=13, p1_bh_u=5,
                p2_fh_w=25, p2_fh_f=9, p2_fh_u=22,
                p2_bh_w=9, p2_bh_f=9, p2_bh_u=5,
            ),
        )
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        )
        row = df.row(0, named=True)
        assert row["p1_fh_winners"] == 16
        assert row["p1_fh_forced_errors"] == 16
        assert row["p1_fh_unforced_errors"] == 8
        assert row["p1_bh_winners"] == 3
        assert row["p1_bh_forced_errors"] == 13
        assert row["p1_bh_unforced_errors"] == 5
        assert row["p2_fh_winners"] == 25
        assert row["p2_fh_forced_errors"] == 9
        assert row["p2_fh_unforced_errors"] == 22
        assert row["p2_bh_winners"] == 9
        assert row["p2_bh_forced_errors"] == 9
        assert row["p2_bh_unforced_errors"] == 5

    def test_shot_type_sums_across_sets(self, tmp_path, tournament):
        """Shot type counts should be summed across all set groups."""
        set1 = _make_empty_group()
        set1[0] = _make_shot_type_entry("Ground Stroke", p1w=4, p1f=3, p1o=35, p2w=7, p2f=2)
        set1[3] = _make_shot_type_entry("Volley Shots", p1w=1, p2w=2, p2f=1)

        set2 = _make_empty_group()
        set2[0] = _make_shot_type_entry("Ground Stroke", p1w=5, p1f=4, p1o=50, p2w=3, p2f=5)
        set2[3] = _make_shot_type_entry("Volley Shots", p1w=2, p2w=3, p2f=2)

        data = _make_match_data(all_points_groups=[set1, set2])
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        )
        row = df.row(0, named=True)
        assert row["p1_ground_stroke_winners"] == 9  # 4+5
        assert row["p1_ground_stroke_forced_errors"] == 7  # 3+4
        assert row["p1_ground_stroke_others"] == 85  # 35+50
        assert row["p2_ground_stroke_winners"] == 10  # 7+3
        assert row["p2_ground_stroke_forced_errors"] == 7  # 2+5
        assert row["p1_volley_winners"] == 3  # 1+2
        assert row["p2_volley_winners"] == 5  # 2+3
        assert row["p2_volley_forced_errors"] == 3  # 1+2

    def test_three_set_match(self, tmp_path, tournament):
        """Three-set match should have 3 groups summed."""
        groups = []
        for i in range(3):
            g = _make_empty_group()
            g[5] = _make_shot_type_entry("Drop Shots", p1w=i + 1, p2u=i + 2)
            groups.append(g)

        data = _make_match_data(sets_completed=3, all_points_groups=groups)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        )
        row = df.row(0, named=True)
        assert row["p1_drop_shot_winners"] == 6  # 1+2+3
        assert row["p2_drop_shot_unforced_errors"] == 9  # 2+3+4

    def test_skips_incomplete_match(self, tmp_path, tournament):
        data = _make_match_data(match_completed=False)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        assert not output.exists()

    def test_skips_doubles(self, tmp_path, tournament):
        """Doubles matches should still be transformed (filtering happens at aggregator)."""
        data = _make_match_data(is_doubles=True)
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        assert output.exists()
        df = pl.read_parquet(output)
        assert df.row(0, named=True)["is_doubles"] is True

    def test_multiple_matches(self, tmp_path, tournament):
        data1 = _make_match_data(
            tpc_entry=_make_tpc_entry(p1_fh_w=10),
        )
        data2 = _make_match_data(
            tpc_entry=_make_tpc_entry(p1_fh_w=20),
        )
        _write_json(tmp_path, tournament, "MS001.json", data1)
        _write_json(tmp_path, tournament, "MS002.json", data2)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        )
        assert len(df) == 2

    def test_no_data_directory(self, tmp_path, tournament):
        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()  # should not raise

    def test_schema_hash_added(self, tmp_path, tournament):
        data = _make_match_data()
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        )
        assert df.row(0, named=True)["schema_hash"] == SCHEMA_HASH

    def test_player_ids_uppercased(self, tmp_path, tournament):
        data = _make_match_data()
        data["players"][0]["player1Id"] = "a123"
        data["players"][1]["player1Id"] = "b456"
        _write_json(tmp_path, tournament, "MS001.json", data)

        transformer = StrokeAnalysisTransformer(tournament, data_root=tmp_path)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / tournament.path / "stroke_analysis.parquet"
        )
        row = df.row(0, named=True)
        assert row["p1_id"] == "A123"
        assert row["p2_id"] == "B456"
