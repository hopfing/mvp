"""Tests for the shared match-completeness predicate."""

import polars as pl

from mvp.model.completeness import is_incomplete_match


def _kept(df: pl.DataFrame, exclude_incomplete: bool = False) -> list:
    return df.filter(~is_incomplete_match(df.columns, exclude_incomplete))["id"].to_list()


def test_walkover_excluded_from_either_field():
    df = pl.DataFrame({
        "reason": ["W/O", None, "RET", None, None],
        "result_type": [None, "walkover", None, "completed", None],
        "id": [1, 2, 3, 4, 5],
    })
    # both walkover encodings dropped; RET + completed + null kept
    assert _kept(df) == [3, 4, 5]


def test_retirement_kept_by_default_excluded_under_flag():
    df = pl.DataFrame({"reason": ["RET", "RET"], "result_type": [None, None], "id": [1, 2]})
    assert _kept(df, exclude_incomplete=False) == [1, 2]
    assert _kept(df, exclude_incomplete=True) == []


def test_def_unp_only_excluded_under_flag():
    df = pl.DataFrame({"reason": ["DEF", "UNP"], "result_type": [None, None], "id": [1, 2]})
    assert _kept(df, exclude_incomplete=False) == [1, 2]
    assert _kept(df, exclude_incomplete=True) == []


def test_graceful_when_columns_missing():
    # neither field present → nothing dropped
    df = pl.DataFrame({"id": [1, 2, 3]})
    assert _kept(df) == [1, 2, 3]
    # only reason present → still catches reason-flagged walkovers
    df2 = pl.DataFrame({"reason": ["W/O", "RET"], "id": [1, 2]})
    assert _kept(df2) == [2]
    # only result_type present → still catches result_type-flagged walkovers
    df3 = pl.DataFrame({"result_type": ["walkover", "completed"], "id": [1, 2]})
    assert _kept(df3) == [2]
