"""Confidence validator — orchestrates OOF preparation and validation."""

from typing import Any

import polars as pl


def prepare_oof(all_predictions: list[dict[str, Any]]) -> pl.DataFrame:
    """Concatenate fold predictions and orient to favored side.

    Takes the all_predictions list from ExperimentRunner.run() and produces
    a single DataFrame with favored-side orientation and probability bucketing.
    """
    frames = []
    for pred in all_predictions:
        df = pred["df"]
        y_true = pred["y_true"]
        y_prob = pred["y_prob"]
        frames.append(
            df.with_columns(
                pl.Series("y_true", y_true),
                pl.Series("y_prob", y_prob),
            )
        )

    combined = pl.concat(frames, how="diagonal_relaxed")

    combined = combined.with_columns(
        pl.when(pl.col("y_prob") >= 0.5)
        .then(pl.col("y_prob"))
        .otherwise(1.0 - pl.col("y_prob"))
        .alias("favored_prob"),
        pl.when(pl.col("y_prob") >= 0.5)
        .then(pl.col("y_true"))
        .otherwise(1 - pl.col("y_true"))
        .alias("favored_won"),
    )

    combined = combined.with_columns(
        pl.when(pl.col("favored_prob") >= 0.95).then(pl.lit("95-100%"))
        .when(pl.col("favored_prob") >= 0.90).then(pl.lit("90-95%"))
        .when(pl.col("favored_prob") >= 0.85).then(pl.lit("85-90%"))
        .when(pl.col("favored_prob") >= 0.80).then(pl.lit("80-85%"))
        .when(pl.col("favored_prob") >= 0.75).then(pl.lit("75-80%"))
        .when(pl.col("favored_prob") >= 0.70).then(pl.lit("70-75%"))
        .when(pl.col("favored_prob") >= 0.65).then(pl.lit("65-70%"))
        .when(pl.col("favored_prob") >= 0.60).then(pl.lit("60-65%"))
        .when(pl.col("favored_prob") >= 0.55).then(pl.lit("55-60%"))
        .otherwise(pl.lit("50-55%"))
        .alias("prob_bucket")
    )

    return combined
