"""Prediction sync protocol and shared merge logic."""

from __future__ import annotations

from typing import Protocol

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


class PredictionSync(Protocol):
    """Interface for reading/writing predictions to an external store."""

    def read_existing(self) -> pl.DataFrame: ...
    def write(self, df: pl.DataFrame) -> None: ...
