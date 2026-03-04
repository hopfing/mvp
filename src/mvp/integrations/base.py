"""Prediction sync protocol and shared merge logic."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol
from zoneinfo import ZoneInfo

import polars as pl

# Column schema: ordered list defining the sheet layout.
# "owner" is "pipeline", "user", or "formula".
COLUMN_SCHEMA = [
    # Match info (pipeline-written)
    {"name": "match_date", "owner": "pipeline"},
    {"name": "match_time", "owner": "pipeline"},
    {"name": "circuit", "owner": "pipeline"},
    {"name": "tournament", "owner": "pipeline"},
    {"name": "surface", "owner": "pipeline"},
    {"name": "round", "owner": "pipeline"},
    # Players & predictions
    {"name": "p1", "owner": "pipeline"},
    {"name": "p2", "owner": "pipeline"},
    {"name": "p1_elo", "owner": "pipeline"},
    {"name": "p2_elo", "owner": "pipeline"},
    {"name": "p1_prob", "owner": "pipeline"},
    {"name": "p2_prob", "owner": "pipeline"},
    {"name": "prediction", "owner": "pipeline"},
    # Odds (user-filled)
    {"name": "p1_odds", "owner": "user"},
    {"name": "p2_odds", "owner": "user"},
    {"name": "pin_p1_odds", "owner": "user"},
    {"name": "pin_p2_odds", "owner": "user"},
    # Edge analysis (formulas)
    {"name": "p1_edge", "owner": "formula"},
    {"name": "p1_pin_edge", "owner": "formula"},
    {"name": "p2_edge", "owner": "formula"},
    {"name": "p2_pin_edge", "owner": "formula"},
    # Bet action
    {"name": "bet_side", "owner": "user"},
    {"name": "stake", "owner": "user"},
    {"name": "to_win", "owner": "formula"},
    {"name": "bet_edge", "owner": "formula"},
    {"name": "bet_pin_edge", "owner": "formula"},
    # Results
    {"name": "result", "owner": "pipeline"},  # auto-filled
    {"name": "bet_result", "owner": "user"},
    {"name": "net", "owner": "formula"},
    {"name": "notes", "owner": "user"},
    # Metadata
    {"name": "match_uid", "owner": "pipeline"},
    {"name": "tournament_day", "owner": "pipeline"},
    {"name": "model_version", "owner": "pipeline"},
    {"name": "predicted_at", "owner": "pipeline"},
]

COLUMN_NAMES = [c["name"] for c in COLUMN_SCHEMA]
PIPELINE_COLUMNS = {c["name"] for c in COLUMN_SCHEMA if c["owner"] == "pipeline"}
USER_COLUMNS = {c["name"] for c in COLUMN_SCHEMA if c["owner"] == "user"}
FORMULA_COLUMNS = {c["name"] for c in COLUMN_SCHEMA if c["owner"] == "formula"}

CIRCUIT_LABELS = {"tour": "ATP", "chal": "CH"}


CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

PIPELINE_COLUMN_ORDER = [
    c["name"] for c in COLUMN_SCHEMA if c["owner"] == "pipeline"
]


def prepare_predictions(predictions: pl.DataFrame) -> pl.DataFrame:
    """Transform raw predictor output into the sheet column layout.

    Applies timezone conversion, column renaming, circuit label mapping,
    elo rounding, prediction column derivation, and tournament_day computation.

    Args:
        predictions: DataFrame from ProductionPredictor.predict().

    Returns:
        DataFrame with only pipeline-owned columns from COLUMN_SCHEMA.
    """
    rows = []
    for row in predictions.iter_rows(named=True):
        scheduled_dt = row.get("scheduled_datetime")
        effective_date = row["effective_match_date"]

        if scheduled_dt is not None:
            if isinstance(scheduled_dt, datetime):
                utc_dt = scheduled_dt.replace(tzinfo=UTC)
            else:
                utc_dt = datetime(
                    scheduled_dt.year, scheduled_dt.month, scheduled_dt.day,
                    tzinfo=UTC,
                )
            ct_dt = utc_dt.astimezone(CT)
            match_date = ct_dt.strftime("%Y-%m-%d")
            match_time = ct_dt.strftime("%H:%M")
        else:
            if hasattr(effective_date, "strftime"):
                match_date = effective_date.strftime("%Y-%m-%d")
            else:
                match_date = str(effective_date)
            match_time = ""

        p1_prob = row["p1_win_prob"]
        p2_prob = row["p2_win_prob"]
        prediction = "P1" if p1_prob >= p2_prob else "P2"

        predicted_at = row["predicted_at"]
        if isinstance(predicted_at, datetime):
            predicted_at_str = predicted_at.isoformat()
        else:
            predicted_at_str = str(predicted_at)

        rows.append({
            "match_date": match_date,
            "match_time": match_time,
            "circuit": CIRCUIT_LABELS.get(row["circuit"], row["circuit"]),
            "tournament": row["tournament_name"],
            "surface": row["surface"],
            "round": row["round"],
            "p1": row["p1_name"],
            "p2": row["p2_name"],
            "p1_elo": round(row["p1_elo"]),
            "p2_elo": round(row["p2_elo"]),
            "p1_prob": p1_prob,
            "p2_prob": p2_prob,
            "prediction": prediction,
            "result": "",
            "match_uid": row["match_uid"],
            "tournament_day": match_date,  # placeholder, computed below
            "model_version": row["model_version"],
            "predicted_at": predicted_at_str,
        })

    if not rows:
        return pl.DataFrame(
            schema={col: pl.Utf8 for col in PIPELINE_COLUMN_ORDER}
        )

    result = pl.DataFrame(rows)

    # tournament_day: min match_date per tournament
    min_dates = result.group_by("tournament").agg(
        pl.col("match_date").min().alias("tournament_day")
    )
    result = result.drop("tournament_day").join(min_dates, on="tournament")

    # Ensure correct types: elo as int, probs as float, everything else string
    result = result.with_columns(
        pl.col("p1_elo").cast(pl.Int64),
        pl.col("p2_elo").cast(pl.Int64),
        pl.col("p1_prob").cast(pl.Float64),
        pl.col("p2_prob").cast(pl.Float64),
    )

    return result.select(PIPELINE_COLUMN_ORDER)


class PredictionSync(Protocol):
    """Interface for reading/writing predictions to an external store."""

    def read_existing(self) -> pl.DataFrame: ...
    def write(self, df: pl.DataFrame) -> None: ...
