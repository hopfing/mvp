"""Tests for the explode_to_player_match helper."""

import polars as pl

from mvp.atptour.aggregators.helpers import explode_to_player_match


def _make_match_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "match_uid": ["2023_580_SGL_F_AAAA_BBBB"],
            "p1_id": ["AAAA"],
            "p2_id": ["BBBB"],
            "p1_seed": [1],
            "p2_seed": [None],
        }
    )


PLAYER_COLS = {"p1_id": "player_id", "p1_seed": "player_seed"}
OPP_COLS = {"p2_id": "opp_id", "p2_seed": "opp_seed"}
SHARED_COLS = ["match_uid"]


def test_explode_produces_two_rows_per_match():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert len(result) == 2


def test_explode_player_ids():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    players = result.sort("player_id")
    assert players["player_id"].to_list() == ["AAAA", "BBBB"]
    assert players["opp_id"].to_list() == ["BBBB", "AAAA"]


def test_explode_swaps_player_indexed_fields():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    row_a = result.filter(pl.col("player_id") == "AAAA")
    row_b = result.filter(pl.col("player_id") == "BBBB")
    assert row_a["player_seed"].item() == 1
    assert row_a["opp_seed"].item() is None
    assert row_b["player_seed"].item() is None
    assert row_b["opp_seed"].item() == 1


def test_explode_preserves_shared_cols():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert result["match_uid"].to_list() == [
        "2023_580_SGL_F_AAAA_BBBB",
        "2023_580_SGL_F_AAAA_BBBB",
    ]


def test_explode_drops_original_p1_p2_cols():
    df = _make_match_df()
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert "p1_id" not in result.columns
    assert "p2_id" not in result.columns
    assert "p1_seed" not in result.columns
    assert "p2_seed" not in result.columns


def test_explode_empty_df():
    df = pl.DataFrame(
        {
            "match_uid": pl.Series([], dtype=pl.String),
            "p1_id": pl.Series([], dtype=pl.String),
            "p2_id": pl.Series([], dtype=pl.String),
            "p1_seed": pl.Series([], dtype=pl.Int64),
            "p2_seed": pl.Series([], dtype=pl.Int64),
        }
    )
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert len(result) == 0
    assert "player_id" in result.columns
    assert "opp_id" in result.columns


def test_explode_multiple_matches():
    df = pl.DataFrame(
        {
            "match_uid": ["UID1", "UID2"],
            "p1_id": ["AAAA", "CCCC"],
            "p2_id": ["BBBB", "DDDD"],
            "p1_seed": [1, 3],
            "p2_seed": [2, 4],
        }
    )
    result = explode_to_player_match(df, PLAYER_COLS, OPP_COLS, SHARED_COLS)
    assert len(result) == 4
