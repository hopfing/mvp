"""Prediction sync protocol and shared merge logic."""

from __future__ import annotations

import logging
import string
from datetime import datetime
from typing import Protocol
from zoneinfo import ZoneInfo

import polars as pl

logger = logging.getLogger(__name__)

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

def _col_letter(index: int) -> str:
    """Convert 0-based column index to spreadsheet column letter(s)."""
    if index < 26:
        return string.ascii_uppercase[index]
    return string.ascii_uppercase[index // 26 - 1] + string.ascii_uppercase[index % 26]


COL_LETTERS = {col["name"]: _col_letter(i) for i, col in enumerate(COLUMN_SCHEMA)}


def generate_formulas(row: int) -> dict[str, str]:
    """Return a dict mapping formula column name to spreadsheet formula for the given row.

    Args:
        row: 1-indexed row number (row 1 = header, row 2 = first data row).

    Returns:
        Dict mapping each formula column name to its formula string.
    """
    r = row
    p1_prob = COL_LETTERS["p1_prob"]
    p2_prob = COL_LETTERS["p2_prob"]
    p1_odds = COL_LETTERS["p1_odds"]
    p2_odds = COL_LETTERS["p2_odds"]
    pin_p1 = COL_LETTERS["pin_p1_odds"]
    pin_p2 = COL_LETTERS["pin_p2_odds"]
    bet_side = COL_LETTERS["bet_side"]
    stake_col = COL_LETTERS["stake"]
    to_win_col = COL_LETTERS["to_win"]
    bet_result_col = COL_LETTERS["bet_result"]
    p1_edge_col = COL_LETTERS["p1_edge"]
    p1_pin_edge_col = COL_LETTERS["p1_pin_edge"]
    p2_edge_col = COL_LETTERS["p2_edge"]
    p2_pin_edge_col = COL_LETTERS["p2_pin_edge"]

    return {
        "p1_edge": f'=IF({p1_odds}{r}="", "", {p1_prob}{r}-(1/{p1_odds}{r}))',
        "p1_pin_edge": f'=IF({pin_p1}{r}="", "", {p1_prob}{r}-(1/{pin_p1}{r}))',
        "p2_edge": f'=IF({p2_odds}{r}="", "", {p2_prob}{r}-(1/{p2_odds}{r}))',
        "p2_pin_edge": f'=IF({pin_p2}{r}="", "", {p2_prob}{r}-(1/{pin_p2}{r}))',
        "to_win": f'=IF({bet_side}{r}="P1", {stake_col}{r}*({p1_odds}{r}-1), IF({bet_side}{r}="P2", {stake_col}{r}*({p2_odds}{r}-1), ""))',
        "bet_edge": f'=IF({bet_side}{r}="P1", {p1_edge_col}{r}, IF({bet_side}{r}="P2", {p2_edge_col}{r}, ""))',
        "bet_pin_edge": f'=IF({bet_side}{r}="P1", {p1_pin_edge_col}{r}, IF({bet_side}{r}="P2", {p2_pin_edge_col}{r}, ""))',
        "net": f'=IF({bet_result_col}{r}="W", {to_win_col}{r}, IF({bet_result_col}{r}="L", -{stake_col}{r}, IF({bet_result_col}{r}="V", 0, "")))',
    }


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
            "_tournament_id": row["tournament_id"],
            "tournament_day": match_date,  # placeholder, computed below
            "model_version": row["model_version"],
            "predicted_at": predicted_at_str,
        })

    if not rows:
        return pl.DataFrame(
            schema={col: pl.Utf8 for col in PIPELINE_COLUMN_ORDER}
        )

    result = pl.DataFrame(rows)

    # Validate: null tournament names indicate upstream data issues
    null_tournaments = result.filter(pl.col("tournament").is_null())
    if len(null_tournaments) > 0:
        uids = null_tournaments["match_uid"].to_list()
        raise ValueError(
            f"{len(uids)} predictions have null tournament_name: {uids}"
        )

    # tournament_day: min match_date per tournament_id (unique per event)
    min_dates = result.group_by("_tournament_id").agg(
        pl.col("match_date").min().alias("tournament_day")
    )
    result = result.drop("tournament_day").join(min_dates, on="_tournament_id")
    result = result.drop("_tournament_id")

    # Ensure correct types: elo as int, probs as float, everything else string
    result = result.with_columns(
        pl.col("p1_elo").cast(pl.Int64),
        pl.col("p2_elo").cast(pl.Int64),
        pl.col("p1_prob").cast(pl.Float64),
        pl.col("p2_prob").cast(pl.Float64),
    )

    return result.select(PIPELINE_COLUMN_ORDER)


def merge_predictions(
    existing: pl.DataFrame,
    new_predictions: pl.DataFrame,
    matches: pl.DataFrame,
) -> pl.DataFrame:
    """Merge new predictions with existing sheet data, auto-filling results.

    Args:
        existing: Current sheet data (all Utf8 columns). Empty if first run.
        new_predictions: Output of prepare_predictions() — pipeline columns only.
        matches: Full matches.parquet DataFrame for result lookup.

    Returns:
        Merged DataFrame with all 34 columns, sorted by tournament_day/tournament/
        match_time/round.
    """
    # 1. Identify new match_uids
    existing_uids: set[str] = set()
    if len(existing) > 0 and "match_uid" in existing.columns:
        existing_uids = set(existing["match_uid"].to_list())

    new_uids = set(new_predictions["match_uid"].to_list())
    truly_new = new_uids - existing_uids

    # 2. Build new rows with all 34 columns
    if truly_new:
        new_rows = new_predictions.filter(pl.col("match_uid").is_in(list(truly_new)))
        for col_def in COLUMN_SCHEMA:
            if col_def["name"] not in new_rows.columns:
                new_rows = new_rows.with_columns(pl.lit("").alias(col_def["name"]))
        new_rows = new_rows.select(COLUMN_NAMES)
        new_rows = new_rows.cast({col: pl.Utf8 for col in COLUMN_NAMES})

        if len(existing) > 0:
            merged = pl.concat([existing, new_rows], how="diagonal_relaxed")
        else:
            merged = new_rows
    else:
        if len(existing) > 0:
            merged = existing
        else:
            return pl.DataFrame(schema={col: pl.Utf8 for col in COLUMN_NAMES})

    # 3. Auto-fill results
    if len(merged) > 0 and len(matches) > 0:
        if "draw_p1_id" in matches.columns:
            canonical = matches.filter(
                pl.col("won").is_not_null()
                & (
                    pl.when(pl.col("draw_p1_id").is_not_null())
                    .then(pl.col("player_id") == pl.col("draw_p1_id"))
                    .otherwise(pl.col("player_id") < pl.col("opp_id"))
                )
            )
        else:
            canonical = matches.filter(
                pl.col("won").is_not_null()
                & (pl.col("player_id") < pl.col("opp_id"))
            )

        result_map: dict[str, str] = {}
        for row in canonical.select("match_uid", "won").iter_rows(named=True):
            result_map[row["match_uid"]] = "P1" if row["won"] else "P2"

        new_results = []
        for row in merged.iter_rows(named=True):
            uid = row["match_uid"]
            current_result = (row.get("result") or "").strip()

            if uid in result_map:
                data_result = result_map[uid]
                if not current_result:
                    new_results.append(data_result)
                else:
                    if current_result != data_result:
                        logger.warning(
                            "Result mismatch for %s: sheet says %s, data says %s",
                            uid,
                            current_result,
                            data_result,
                        )
                    new_results.append(current_result)
            else:
                new_results.append(current_result)

        merged = merged.with_columns(pl.Series("result", new_results))

    # 4. Sort
    from mvp.atptour.aggregators.matches import ROUND_ORDER

    merged = merged.with_columns(
        pl.col("round").replace_strict(ROUND_ORDER, default=99).alias("_round_order")
    )
    merged = merged.sort(
        ["tournament_day", "circuit", "tournament", "match_date", "match_time", "_round_order"]
    ).drop("_round_order")

    return merged


class PredictionSync(Protocol):
    """Interface for reading/writing predictions to an external store."""

    def read_existing(self) -> pl.DataFrame: ...
    def write(self, df: pl.DataFrame) -> None: ...
