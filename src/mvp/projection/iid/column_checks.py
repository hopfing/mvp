"""Live/train parity check on a serve estimator's input columns.

The 2026-09-02 → 09-05 defect was a column present on ~99% of training rows
and null on every live row; the model that was promoted was not the model
that was betting. This check reads the null share of each column an
estimator declares, on the frame it is about to project, and reports it.

It raises only on the unambiguous case — enough rows to mean something and
every one of them null — because a stage raise degrades that stage for the
tick and posts a failure, while one or two debutant matches with no filter
state yet are legitimately all-null on a quiet day.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)

# Below this many rows an all-null frame is a small-sample fact, not a defect.
ALL_NULL_MIN_ROWS = 20


def null_shares(df: pl.DataFrame, columns: list[str]) -> dict[str, float]:
    """Null share per column present in `df`; absent columns are reported as 1.0."""
    out: dict[str, float] = {}
    n = df.height
    for c in columns:
        if c not in df.columns:
            out[c] = 1.0
        elif n == 0:
            out[c] = 0.0
        else:
            out[c] = float(df[c].null_count()) / n
    return out


def check_required_columns(
    df: pl.DataFrame, columns: list[str], *, where: str,
    raise_on_all_null: bool = True,
) -> dict[str, float]:
    """Log every declared column's null share on `df`; raise when the frame is
    large enough to matter and a column is null on all of it."""
    shares = null_shares(df, columns)
    if not shares:
        return shares
    summary = ", ".join(f"{c}={s:.3f}" for c, s in shares.items())
    logger.info("%s: %d rows; null share by required column: %s", where, df.height, summary)
    if raise_on_all_null and df.height >= ALL_NULL_MIN_ROWS:
        dead = [c for c, s in shares.items() if s >= 1.0]
        if dead:
            raise ValueError(
                f"{where}: required column(s) {dead} are null on all "
                f"{df.height} rows — the frame the estimator is about to "
                "project carries none of the input it was trained on"
            )
    return shares
