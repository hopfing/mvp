"""Tests for stroke_analysis aggregator."""

from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.aggregators.stroke_analysis import StrokeAnalysisAggregator


def _make_staged_df(
    tournament_id="339", year=2023, match_id="MS001",
    p1_id="A123", p2_id="B456", is_doubles=False,
    p1_fh_winners=10, p1_bh_winners=5,
    p2_fh_winners=8, p2_bh_winners=3,
    p1_ground_stroke_winners=7, p2_ground_stroke_winners=6,
):
    """Create a staged stroke_analysis DataFrame."""
    data = {
        "tournament_id": [tournament_id],
        "year": [year],
        "match_id": [match_id],
        "is_doubles": [is_doubles],
        "p1_id": [p1_id],
        "p2_id": [p2_id],
        "p1_fh_winners": [p1_fh_winners],
        "p1_fh_forced_errors": [0],
        "p1_fh_unforced_errors": [0],
        "p1_bh_winners": [p1_bh_winners],
        "p1_bh_forced_errors": [0],
        "p1_bh_unforced_errors": [0],
        "p2_fh_winners": [p2_fh_winners],
        "p2_fh_forced_errors": [0],
        "p2_fh_unforced_errors": [0],
        "p2_bh_winners": [p2_bh_winners],
        "p2_bh_forced_errors": [0],
        "p2_bh_unforced_errors": [0],
        "p1_ground_stroke_winners": [p1_ground_stroke_winners],
        "p1_ground_stroke_forced_errors": [0],
        "p1_ground_stroke_unforced_errors": [0],
        "p1_ground_stroke_others": [0],
        "p2_ground_stroke_winners": [p2_ground_stroke_winners],
        "p2_ground_stroke_forced_errors": [0],
        "p2_ground_stroke_unforced_errors": [0],
        "p2_ground_stroke_others": [0],
    }
    # Add remaining shot types with zeros
    for shot in ["overhead", "passing", "volley", "approach", "drop_shot", "lob"]:
        for prefix in ["p1", "p2"]:
            for suffix in ["winners", "forced_errors", "unforced_errors", "others"]:
                data[f"{prefix}_{shot}_{suffix}"] = [0]

    data["source_file"] = ["test.json"]
    data["parsed_at"] = ["2026-03-03T00:00:00"]
    data["schema_hash"] = ["stroke_analysis_v1_2026_03_03"]

    return pl.DataFrame(data)


def _write_staged(tmp_path, df, tournament_path="tournaments/tour/339/2023"):
    """Write staged parquet file."""
    path = tmp_path / "stage" / "atptour" / tournament_path / "stroke_analysis.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


class TestStrokeAnalysisAggregator:
    def test_pivot_to_player_match(self, tmp_path):
        df = _make_staged_df(
            p1_fh_winners=10, p1_bh_winners=5,
            p2_fh_winners=8, p2_bh_winners=3,
        )
        _write_staged(tmp_path, df)

        agg = StrokeAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert len(result) == 2  # two perspectives

        p1_row = result.filter(pl.col("player_id") == "A123").row(0, named=True)
        assert p1_row["player_fh_winners"] == 10
        assert p1_row["player_bh_winners"] == 5
        assert p1_row["opp_fh_winners"] == 8
        assert p1_row["opp_bh_winners"] == 3

        p2_row = result.filter(pl.col("player_id") == "B456").row(0, named=True)
        assert p2_row["player_fh_winners"] == 8
        assert p2_row["player_bh_winners"] == 3
        assert p2_row["opp_fh_winners"] == 10
        assert p2_row["opp_bh_winners"] == 5

    def test_shot_type_columns_pivoted(self, tmp_path):
        df = _make_staged_df(
            p1_ground_stroke_winners=7,
            p2_ground_stroke_winners=6,
        )
        _write_staged(tmp_path, df)

        agg = StrokeAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        p1_row = result.filter(pl.col("player_id") == "A123").row(0, named=True)
        assert p1_row["player_ground_stroke_winners"] == 7
        assert p1_row["opp_ground_stroke_winners"] == 6

    def test_filters_doubles(self, tmp_path):
        singles = _make_staged_df(p1_id="A123", is_doubles=False)
        doubles = _make_staged_df(p1_id="C789", is_doubles=True, match_id="MD001")
        combined = pl.concat([singles, doubles])
        _write_staged(tmp_path, combined)

        agg = StrokeAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert "C789" not in result["player_id"].to_list()

    def test_multiple_tournaments(self, tmp_path):
        df1 = _make_staged_df(tournament_id="339", match_id="MS001")
        df2 = _make_staged_df(tournament_id="404", match_id="MS001")
        _write_staged(tmp_path, df1, "tournaments/tour/339/2023")
        _write_staged(tmp_path, df2, "tournaments/tour/404/2023")

        agg = StrokeAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert len(result) == 4  # 2 matches × 2 perspectives

    def test_no_data(self, tmp_path):
        agg = StrokeAnalysisAggregator(data_root=tmp_path)
        result = agg.run()
        assert result is None

    def test_output_written(self, tmp_path):
        df = _make_staged_df()
        _write_staged(tmp_path, df)

        agg = StrokeAnalysisAggregator(data_root=tmp_path)
        agg.run()

        output = tmp_path / "aggregate" / "atptour" / "stroke_analysis.parquet"
        assert output.exists()
