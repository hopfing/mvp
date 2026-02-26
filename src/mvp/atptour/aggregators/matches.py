"""Layer 2: Cross-tournament aggregation into a single enriched matches dataset."""

import polars as pl

ROUND_ORDER: dict[str, int] = {
    "Q1": 1,
    "Q2": 2,
    "Q3": 3,
    "RR": 4,
    "R128": 5,
    "R64": 6,
    "R32": 7,
    "R16": 8,
    "QF": 9,
    "SF": 10,
    "THIRDPLACE": 11,
    "HCF": 11,
    "BRONZE": 11,
    "F": 12,
}


def add_round_order(df: pl.DataFrame) -> pl.DataFrame:
    """Add round_order column from the round column using ROUND_ORDER mapping."""
    return df.with_columns(
        pl.col("round")
        .replace_strict(ROUND_ORDER, default=None)
        .cast(pl.Int64)
        .alias("round_order")
    )
