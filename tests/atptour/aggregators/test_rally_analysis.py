"""Tests for rally_analysis aggregator."""

from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.aggregators.rally_analysis import RallyAnalysisAggregator


def _make_staged_df(
    tournament_id="339", year=2023, match_id="MS001",
    p1_id="A123", p2_id="B456", is_doubles=False,
    p1_short_won=10, p1_short_err=5,
    p2_short_won=8, p2_short_err=3,
    p1_medium_won=4, p1_medium_err=2,
    p2_medium_won=3, p2_medium_err=1,
    p1_long_won=2, p1_long_err=1,
    p2_long_won=1, p2_long_err=0,
):
    """Create a staged rally_analysis DataFrame."""
    data = {
        "tournament_id": [tournament_id],
        "year": [year],
        "match_id": [match_id],
        "is_doubles": [is_doubles],
        "p1_id": [p1_id],
        "p2_id": [p2_id],
        "p1_short_won": [p1_short_won],
        "p1_short_err": [p1_short_err],
        "p2_short_won": [p2_short_won],
        "p2_short_err": [p2_short_err],
        "p1_medium_won": [p1_medium_won],
        "p1_medium_err": [p1_medium_err],
        "p2_medium_won": [p2_medium_won],
        "p2_medium_err": [p2_medium_err],
        "p1_long_won": [p1_long_won],
        "p1_long_err": [p1_long_err],
        "p2_long_won": [p2_long_won],
        "p2_long_err": [p2_long_err],
        "p1_unclassified_won": [0],
        "p1_unclassified_err": [0],
        "p2_unclassified_won": [0],
        "p2_unclassified_err": [0],
        "points_missing": [False],
        "source_file": ["test.json"],
        "parsed_at": ["2026-03-03T00:00:00"],
        "schema_hash": ["rally_analysis_v1_2026_03_03"],
    }
    return pl.DataFrame(data)


def _write_staged(tmp_path, df, tournament_path="tournaments/tour/339/2023"):
    """Write staged parquet file."""
    path = tmp_path / "stage" / "atptour" / tournament_path / "rally_analysis.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


class TestRallyAnalysisAggregator:
    def test_pivot_to_player_match(self, tmp_path):
        df = _make_staged_df(
            p1_short_won=10, p1_short_err=5,
            p2_short_won=8, p2_short_err=3,
        )
        _write_staged(tmp_path, df)

        agg = RallyAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert len(result) == 2  # two perspectives

        p1_row = result.filter(pl.col("player_id") == "A123").row(0, named=True)
        assert p1_row["player_short_won"] == 10
        assert p1_row["player_short_err"] == 5
        assert p1_row["opp_short_won"] == 8
        assert p1_row["opp_short_err"] == 3

        p2_row = result.filter(pl.col("player_id") == "B456").row(0, named=True)
        assert p2_row["player_short_won"] == 8
        assert p2_row["player_short_err"] == 3
        assert p2_row["opp_short_won"] == 10
        assert p2_row["opp_short_err"] == 5

    def test_all_rally_lengths_pivoted(self, tmp_path):
        df = _make_staged_df(
            p1_medium_won=4, p2_medium_won=3,
            p1_long_won=2, p2_long_won=1,
        )
        _write_staged(tmp_path, df)

        agg = RallyAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        p1_row = result.filter(pl.col("player_id") == "A123").row(0, named=True)
        assert p1_row["player_medium_won"] == 4
        assert p1_row["opp_medium_won"] == 3
        assert p1_row["player_long_won"] == 2
        assert p1_row["opp_long_won"] == 1

    def test_filters_doubles(self, tmp_path):
        singles = _make_staged_df(p1_id="A123", is_doubles=False)
        doubles = _make_staged_df(p1_id="C789", is_doubles=True, match_id="MD001")
        combined = pl.concat([singles, doubles])
        _write_staged(tmp_path, combined)

        agg = RallyAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert "C789" not in result["player_id"].to_list()

    def test_multiple_tournaments(self, tmp_path):
        df1 = _make_staged_df(tournament_id="339", match_id="MS001")
        df2 = _make_staged_df(tournament_id="404", match_id="MS001")
        _write_staged(tmp_path, df1, "tournaments/tour/339/2023")
        _write_staged(tmp_path, df2, "tournaments/tour/404/2023")

        agg = RallyAnalysisAggregator(data_root=tmp_path)
        result = agg.run()

        assert result is not None
        assert len(result) == 4  # 2 matches x 2 perspectives

    def test_no_data(self, tmp_path):
        agg = RallyAnalysisAggregator(data_root=tmp_path)
        result = agg.run()
        assert result is None

    def test_output_written(self, tmp_path):
        df = _make_staged_df()
        _write_staged(tmp_path, df)

        agg = RallyAnalysisAggregator(data_root=tmp_path)
        agg.run()

        output = tmp_path / "aggregate" / "atptour" / "rally_analysis.parquet"
        assert output.exists()
