"""Prediction sync protocol and shared merge logic."""


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
    {"name": "date", "owner": "pipeline"},
    {"name": "time", "owner": "pipeline"},
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
    {"name": "p1_pin", "owner": "user"},
    {"name": "p2_pin", "owner": "user"},
    # Edge analysis (formulas)
    {"name": "p1_edge", "owner": "formula"},
    {"name": "p1_pe", "owner": "formula"},
    {"name": "p2_edge", "owner": "formula"},
    {"name": "p2_pe", "owner": "formula"},
    # Bet action
    {"name": "bet_side", "owner": "user"},
    {"name": "bet_odds", "owner": "formula"},
    {"name": "stake", "owner": "user"},
    {"name": "to_win", "owner": "formula"},
    {"name": "book", "owner": "user"},
    # Results
    {"name": "result", "owner": "pipeline"},  # auto-filled
    {"name": "bet_result", "owner": "user"},
    {"name": "net", "owner": "formula"},
    {"name": "notes", "owner": "user"},
    # Metadata
    {"name": "match_uid", "owner": "pipeline"},
    {"name": "p1_id", "owner": "pipeline"},
    {"name": "p2_id", "owner": "pipeline"},
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
    pin_p1 = COL_LETTERS["p1_pin"]
    pin_p2 = COL_LETTERS["p2_pin"]
    bet_side = COL_LETTERS["bet_side"]
    stake_col = COL_LETTERS["stake"]
    to_win_col = COL_LETTERS["to_win"]
    bet_result_col = COL_LETTERS["bet_result"]

    bet_odds_col = COL_LETTERS["bet_odds"]

    return {
        "p1_edge": f'=IF({p1_odds}{r}="", "", {p1_prob}{r}-(1/{p1_odds}{r}))',
        "p1_pe": f'=IF({pin_p1}{r}="", "", {p1_prob}{r}-(1/{pin_p1}{r}))',
        "p2_edge": f'=IF({p2_odds}{r}="", "", {p2_prob}{r}-(1/{p2_odds}{r}))',
        "p2_pe": f'=IF({pin_p2}{r}="", "", {p2_prob}{r}-(1/{pin_p2}{r}))',
        "bet_odds": f'=IF({bet_side}{r}="P1", {p1_odds}{r}, IF({bet_side}{r}="P2", {p2_odds}{r}, ""))',
        "to_win": f'=IF({stake_col}{r}="", "", ROUND({stake_col}{r}*{bet_odds_col}{r}, 2))',
        "net": f'=IF({bet_result_col}{r}="W", {to_win_col}{r}-{stake_col}{r}, IF({bet_result_col}{r}="L", -{stake_col}{r}, IF({bet_result_col}{r}="V", 0, "")))',
    }


CIRCUIT_LABELS = {"tour": "ATP", "chal": "CH"}


def _format_date(val) -> str | None:
    """Format a date/datetime value as YYYY-MM-DD string, or None."""
    if val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return str(val) or None

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
            "date": match_date,
            "time": match_time,
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
            "p1_id": row["p1_id"],
            "p2_id": row["p2_id"],
            "_tournament_id": row["tournament_id"],
            "tournament_day": _format_date(row.get("match_date")) or match_date,
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
        Merged DataFrame with all 36 columns, sorted by tournament_day/tournament/
        match_time/round.
    """
    # 1. Identify new match_uids
    existing_uids: set[str] = set()
    if len(existing) > 0 and "match_uid" in existing.columns:
        existing_uids = set(existing["match_uid"].to_list())

    new_uids = set(new_predictions["match_uid"].to_list())
    truly_new = new_uids - existing_uids

    # 2. Build new rows with all 36 columns
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

    # 3. Auto-fill results using player IDs
    if len(merged) > 0 and len(matches) > 0:
        # Build winner_id map: match_uid -> player_id of winner
        won_rows = matches.filter(pl.col("won") == True).select(
            "match_uid", pl.col("player_id").alias("winner_id"),
        )
        winner_map: dict[str, str] = {}
        for row in won_rows.iter_rows(named=True):
            winner_map[row["match_uid"]] = row["winner_id"]

        new_results = []
        for row in merged.iter_rows(named=True):
            uid = row["match_uid"]
            current_result = (row.get("result") or "").strip()
            sheet_p1_id = (row.get("p1_id") or "").strip()

            if uid in winner_map and sheet_p1_id:
                data_result = "P1" if winner_map[uid] == sheet_p1_id else "P2"
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

    # 3b. Auto-fill bet_result from result + bet_side (don't overwrite user entries)
    if len(merged) > 0:
        new_bet_results = []
        for row in merged.iter_rows(named=True):
            current_bet_result = (row.get("bet_result") or "").strip()
            if current_bet_result:
                new_bet_results.append(current_bet_result)
                continue
            bet_side = (row.get("bet_side") or "").strip()
            result_val = (row.get("result") or "").strip()
            if bet_side in ("P1", "P2") and result_val in ("P1", "P2"):
                new_bet_results.append("W" if bet_side == result_val else "L")
            else:
                new_bet_results.append(current_bet_result)
        merged = merged.with_columns(pl.Series("bet_result", new_bet_results))

    # 4. Re-pad time column (Google Sheets strips leading zeros)
    merged = merged.with_columns(
        pl.col("time").map_elements(
            lambda t: t.zfill(5) if t else t, return_dtype=pl.Utf8
        )
    )

    # 5. Sort
    from mvp.atptour.aggregators.matches import ROUND_ORDER

    merged = merged.with_columns(
        pl.col("round").replace_strict(ROUND_ORDER, default=99).alias("_round_order")
    )
    merged = merged.sort(
        ["tournament_day", "circuit", "tournament", "date", "time", "_round_order"]
    ).drop("_round_order")

    return merged


class PredictionSync(Protocol):
    """Interface for reading/writing predictions to an external store."""

    def read_existing(self) -> pl.DataFrame: ...
    def write(self, df: pl.DataFrame) -> None: ...
