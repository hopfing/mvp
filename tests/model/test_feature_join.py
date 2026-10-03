"""The model-errors fold reader (error_analysis/feature_join.py)."""

import polars as pl

from mvp.model.error_analysis.feature_join import _load_fold_predictions


def _write(path, is_holdout=None):
    df = pl.DataFrame({
        "match_uid": ["M0", "M1", "M2"],
        "player_id": ["A", "A", "A"],
        "fold_idx": [1, 2, 3],
        "y_test": [1, 0, 1],
        "y_prob": [0.6, 0.4, 0.7],
    })
    if is_holdout is not None:
        df = df.with_columns(pl.Series("is_holdout", is_holdout))
    df.write_parquet(path)
    return path


def test_held_out_rows_are_excluded(tmp_path):
    """A holdout_end evaluation's held-out folds stay out of model-errors,
    which shows the selection period as before."""
    path = _write(tmp_path / "fold_predictions.parquet", [False, False, True])
    df = _load_fold_predictions(path)
    assert df["match_uid"].to_list() == ["M0", "M1"]


def test_files_without_the_column_load_whole(tmp_path):
    path = _write(tmp_path / "fold_predictions.parquet")
    assert _load_fold_predictions(path).height == 3
