"""Prediction sync protocol and shared merge logic."""


import logging
import string
from datetime import datetime
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

import polars as pl
import yaml

from mvp.model.cal_tiers import classify_cal_tier, load_cal_tiers_from_path

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
    {"name": "elo_diff", "owner": "formula"},
    # Context diff (picked - opponent), pipeline-written from the lead model's features
    {"name": "age_diff", "owner": "pipeline"},
    {"name": "prediction", "owner": "pipeline"},
    {"name": "pred_prob", "owner": "pipeline"},
    {"name": "consensus", "owner": "pipeline"},
    # Picked player's price for display; raw p1/p2 odds live in the meta block at the end
    {"name": "pred_odds", "owner": "formula"},
    # Edge analysis
    {"name": "fav_edge_open", "owner": "pipeline"},
    {"name": "fav_edge", "owner": "formula"},
    # Recommended stake — fractional Kelly off the row's control inputs
    # (bankroll/kelly_fraction/max_pct). Live while the bet is open; frozen to a
    # literal snapshot at bet time (when a stake is entered).
    {"name": "kelly_stake", "owner": "formula"},
    # Bet action
    {"name": "bet_side", "owner": "user"},
    {"name": "bet_odds", "owner": "formula"},
    {"name": "stake", "owner": "user"},
    {"name": "to_win", "owner": "formula"},
    {"name": "book", "owner": "user"},
    {"name": "book2", "owner": "user"},
    # Results
    {"name": "result", "owner": "pipeline"},  # auto-filled
    {"name": "pred_result", "owner": "formula"},
    {"name": "bet_result", "owner": "user"},
    {"name": "net", "owner": "formula"},
    {"name": "notes", "owner": "user"},
    # Kelly control inputs — user-maintained. Put whatever formula you like here
    # (e.g. inherit-from-the-row-above); the sync preserves the formula on
    # round-trip instead of flattening it to a computed value.
    {"name": "bankroll", "owner": "user"},
    {"name": "kelly_fraction", "owner": "user"},
    {"name": "max_pct", "owner": "user"},
    # Meta columns — moved out of the way but kept for downstream analysis.
    # Raw per-side odds (auto-filled) feed pred_odds / fav_edge / dog_edge / bet_odds
    # and the CLV/de-vig analysis in dataset.py.
    {"name": "p1_odds", "owner": "user"},
    {"name": "p2_odds", "owner": "user"},
    {"name": "dog_edge", "owner": "formula"},
    {"name": "cell_cal", "header": "cal", "owner": "pipeline"},
    {"name": "cal_tier", "header": "tier", "owner": "pipeline"},
    # Court (indoor/outdoor) — static match metadata from matches.parquet
    {"name": "court", "owner": "pipeline"},
    # Metadata
    {"name": "match_uid", "owner": "pipeline"},
    {"name": "p1_id", "owner": "pipeline"},
    {"name": "p2_id", "owner": "pipeline"},
    {"name": "tournament_day", "owner": "pipeline"},
    {"name": "model_version", "owner": "pipeline"},
    {"name": "predicted_at", "owner": "pipeline"},
    {"name": "bet_placed_at", "owner": "pipeline"},
]

COLUMN_NAMES = [c["name"] for c in COLUMN_SCHEMA]
# What gets written as / compared against the bets-sheet header row. Most
# columns expose `name` directly; entries with an explicit `header` field
# show a shorter user-facing label (e.g. cal_tier -> tier) without changing
# the in-code DataFrame column name.
SHEET_HEADERS = [c.get("header", c["name"]) for c in COLUMN_SCHEMA]
PIPELINE_COLUMNS = {c["name"] for c in COLUMN_SCHEMA if c["owner"] == "pipeline"}
USER_COLUMNS = {c["name"] for c in COLUMN_SCHEMA if c["owner"] == "user"}
FORMULA_COLUMNS = {c["name"] for c in COLUMN_SCHEMA if c["owner"] == "formula"}
# User-maintained columns that may hold a formula (e.g. an inherit-from-above
# bankroll). The sync reads these with FORMULA rendering and writes the raw
# formula back, rather than flattening them to a computed value.
FORMULA_PRESERVE_COLUMNS = {"bankroll", "kelly_fraction", "max_pct"}
# Formula columns that stay live while a bet is open but freeze to a literal
# snapshot once the bet is placed (a stake is entered) — the recommended stake
# should record what was advised at bet time, not keep recomputing afterward.
FREEZE_AT_BET_COLUMNS = {"kelly_stake"}

def _col_letter(index: int) -> str:
    """Convert 0-based column index to spreadsheet column letter(s)."""
    if index < 26:
        return string.ascii_uppercase[index]
    return string.ascii_uppercase[index // 26 - 1] + string.ascii_uppercase[index % 26]


COL_LETTERS = {col["name"]: _col_letter(i) for i, col in enumerate(COLUMN_SCHEMA)}


def _resolve_lead_sidecar_path(production_yaml: Path = Path("production.yaml")) -> Path | None:
    """Locate the cal_tiers sidecar for the production lead artifact.

    Reads `winner.active.artifact` (or top-level `active.artifact` for the
    flat config shape) and returns `<artifact_dir>/<artifact_stem>_cal_tiers.json`
    if it exists. Returns None when the config or sidecar is missing.
    """
    if not production_yaml.exists():
        return None
    with open(production_yaml) as f:
        raw = yaml.safe_load(f) or {}
    section = raw.get("winner") or raw
    active = (section or {}).get("active") or {}
    artifact = active.get("artifact")
    if not artifact:
        return None
    p = Path(artifact)
    sidecar = p.with_name(f"{p.stem}_cal_tiers.json")
    return sidecar if sidecar.exists() else None


def generate_formulas(row: int) -> dict[str, str]:
    """Return a dict mapping formula column name to spreadsheet formula for the given row.

    Args:
        row: 1-indexed row number (row 1 = header, row 2 = first data row).

    Returns:
        Dict mapping each formula column name to its formula string.
    """
    r = row
    p1_elo = COL_LETTERS["p1_elo"]
    p2_elo = COL_LETTERS["p2_elo"]
    pred_prob = COL_LETTERS["pred_prob"]
    pred_odds_col = COL_LETTERS["pred_odds"]
    p1_odds = COL_LETTERS["p1_odds"]
    p2_odds = COL_LETTERS["p2_odds"]
    bet_side = COL_LETTERS["bet_side"]
    stake_col = COL_LETTERS["stake"]
    to_win_col = COL_LETTERS["to_win"]
    bet_result_col = COL_LETTERS["bet_result"]

    bet_odds_col = COL_LETTERS["bet_odds"]
    prediction_col = COL_LETTERS["prediction"]
    result_col = COL_LETTERS["result"]
    bankroll_col = COL_LETTERS["bankroll"]
    kelly_fraction_col = COL_LETTERS["kelly_fraction"]
    max_pct_col = COL_LETTERS["max_pct"]

    # pred_odds: the picked player's price (raw p1/p2 odds live in the meta block)
    pred_odds_formula = (
        f'=IF({prediction_col}{r}="P1", {p1_odds}{r}, '
        f'IF({prediction_col}{r}="P2", {p2_odds}{r}, ""))'
    )
    # fav_edge: edge on the model's picked player at its price
    fav_edge_formula = (
        f'=IF({pred_odds_col}{r}="", "", {pred_prob}{r}-(1/{pred_odds_col}{r}))'
    )
    # dog_edge: edge on the other side = (1 - pred_prob) at the opponent's price
    dog_edge_formula = (
        f'=IF({prediction_col}{r}="P1", '
        f'IF({p2_odds}{r}="", "", (1-{pred_prob}{r})-(1/{p2_odds}{r})), '
        f'IF({p1_odds}{r}="", "", (1-{pred_prob}{r})-(1/{p1_odds}{r})))'
    )

    # kelly_stake: fractional-Kelly dollars off this row's bankroll. Full Kelly
    # for decimal odds o and win prob p is (p*o - 1)/(o - 1); we scale that by
    # kelly_fraction, floor at 0 for non-positive edge, and cap at
    # max_pct * bankroll. Blanks until every input is set, and on any negative
    # control value (odds/bankroll/fraction/max_pct) rather than emitting a
    # nonsensical or negative stake.
    kelly_stake_formula = (
        f'=IF(OR({pred_prob}{r}="",{pred_odds_col}{r}="",{pred_odds_col}{r}<=1,'
        f'{bankroll_col}{r}="",{bankroll_col}{r}<=0,'
        f'{kelly_fraction_col}{r}="",{kelly_fraction_col}{r}<0,'
        f'{max_pct_col}{r}="",{max_pct_col}{r}<0),"",'
        f'MIN({max_pct_col}{r}*{bankroll_col}{r},'
        f'MAX(0,{kelly_fraction_col}{r}*{bankroll_col}{r}*'
        f'({pred_prob}{r}*{pred_odds_col}{r}-1)/({pred_odds_col}{r}-1))))'
    )

    return {
        "elo_diff": f'=ABS({p1_elo}{r}-{p2_elo}{r})',
        "pred_odds": pred_odds_formula,
        "fav_edge": fav_edge_formula,
        "kelly_stake": kelly_stake_formula,
        "dog_edge": dog_edge_formula,
        "bet_odds": f'=IF({bet_side}{r}="P1", {p1_odds}{r}, IF({bet_side}{r}="P2", {p2_odds}{r}, ""))',
        "to_win": f'=IF({stake_col}{r}="", "", ROUND({stake_col}{r}*{bet_odds_col}{r}, 2))',
        "pred_result": f'=IF({result_col}{r}="", "", IF({prediction_col}{r}={result_col}{r}, "W", "L"))',
        "net": f'=IF({bet_result_col}{r}="W", {to_win_col}{r}-{stake_col}{r}, IF({bet_result_col}{r}="L", -{stake_col}{r}, IF({bet_result_col}{r}="V", 0, "")))',
    }


CIRCUIT_LABELS = {"tour": "ATP", "chal": "CH"}
# Reverse map for callers that need to translate the sheet's display label
# back to the raw circuit value used by upstream artifacts (cal_tiers
# sidecar keys on raw "tour" / "chal").
CIRCUIT_LABELS_INVERSE = {v: k for k, v in CIRCUIT_LABELS.items()}


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
        pred_prob = p1_prob if prediction == "P1" else p2_prob
        # Reorient the p1 - p2 context diffs onto the picked player, so they
        # read picked - opponent regardless of which slot the pick landed in.
        pick_sign = 1 if prediction == "P1" else -1

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
            "pred_prob": pred_prob,
            # picked - opponent age diff from the lead model's features (null -> blank)
            "age_diff": (
                round(pick_sign * row["player_age_diff"], 1)
                if row.get("player_age_diff") is not None else None
            ),
            "prediction": prediction,
            "consensus": row.get("consensus") if row.get("consensus") is not None else "",
            "result": "",
            "match_uid": row["match_uid"],
            "p1_id": row["p1_id"],
            "p2_id": row["p2_id"],
            "_tournament_id": row["tournament_id"],
            "_schedule_day": row.get("schedule_day"),
            "_raw_match_date": _format_date(row.get("match_date")) or match_date,
            "tournament_day": "",  # filled below after grouping
            "model_version": row["model_version"],
            "predicted_at": predicted_at_str,
            "bet_placed_at": "",
            "fav_edge_open": "",
            "cell_cal": "",
            "cal_tier": "",
            "court": "",
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

    # tournament_day = venue-local date of the ATP session a match belongs to.
    # Using venue date (rather than CT date) keeps Asian/Australian sessions
    # that span midnight CT — e.g. a Wuning "Day 8" R16 block running 11pm CT
    # 4/7 → 4am CT 4/8 — bucketed under their true ATP day (4/8) instead of
    # collapsing onto the previous CT date alongside the prior session's matches.
    # Group by (tournament, schedule_day || raw_match_date) so all matches in
    # the same ATP day share a value even if raw_match_date is briefly stale.
    result = result.with_columns(
        pl.coalesce(
            pl.col("_schedule_day").cast(pl.Utf8),
            pl.col("_raw_match_date"),
        ).alias("_day_group"),
    )
    result = result.with_columns(
        pl.col("_raw_match_date")
        .min()
        .over("_tournament_id", "_day_group")
        .alias("tournament_day"),
    )
    result = result.drop("_tournament_id", "_schedule_day", "_raw_match_date", "_day_group")

    # Ensure correct types: elo as int, probs as float, everything else string
    result = result.with_columns(
        pl.col("p1_elo").cast(pl.Int64),
        pl.col("p2_elo").cast(pl.Int64),
        pl.col("pred_prob").cast(pl.Float64),
    )

    return result.select(PIPELINE_COLUMN_ORDER)


def merge_predictions(
    existing: pl.DataFrame,
    new_predictions: pl.DataFrame,
    matches: pl.DataFrame,
    odds_maps: dict[str, dict[str, dict[str, float]]] | None = None,
    opening_odds_maps: dict[str, dict[str, dict[str, float]]] | None = None,
) -> pl.DataFrame:
    """Merge new predictions with existing sheet data, auto-filling results.

    Args:
        existing: Current sheet data (all Utf8 columns). Empty if first run.
        new_predictions: Output of prepare_predictions() — pipeline columns only.
        matches: Full matches.parquet DataFrame for result lookup.

    Returns:
        Merged DataFrame with all columns, sorted by tournament_day/tournament/
        match_time/round.
    """
    # 1. Identify new match_uids
    existing_uids: set[str] = set()
    if len(existing) > 0 and "match_uid" in existing.columns:
        existing_uids = set(existing["match_uid"].to_list())

    new_uids = set(new_predictions["match_uid"].to_list())
    truly_new = new_uids - existing_uids

    # 2a. Update schedule/logistics columns on existing rows
    REFRESH_COLUMNS = {"date", "time", "round", "tournament", "surface", "circuit", "tournament_day"}
    if existing_uids and len(new_predictions) > 0:
        refresh_lookup: dict[str, dict[str, str]] = {}
        refresh_preds = new_predictions.filter(
            pl.col("match_uid").is_in(list(existing_uids & new_uids))
        )
        for row in refresh_preds.iter_rows(named=True):
            refresh_lookup[row["match_uid"]] = {
                col: str(row[col]) if row[col] is not None else ""
                for col in REFRESH_COLUMNS
                if col in row
            }

        if refresh_lookup:
            updated_cols: dict[str, list[str]] = {col: [] for col in REFRESH_COLUMNS}
            for row in existing.iter_rows(named=True):
                uid = row.get("match_uid", "")
                refreshed = refresh_lookup.get(uid)
                for col in REFRESH_COLUMNS:
                    if refreshed and refreshed.get(col):
                        updated_cols[col].append(refreshed[col])
                    else:
                        updated_cols[col].append(row.get(col, ""))
            existing = existing.with_columns(
                pl.Series(col, vals) for col, vals in updated_cols.items()
            )

    # 2b. Build new rows with all columns
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
        won_rows = matches.filter(pl.col("won")).select(
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
    #     Walkovers are always voided (bet_result="V") when a bet was placed.
    #     Retirements are left blank — varies by book, user decides.
    result_type_map: dict[str, str] = {}
    if len(matches) > 0 and "result_type" in matches.columns:
        for row in matches.filter(
            pl.col("result_type").is_in(["walkover", "retirement"])
        ).select("match_uid", "result_type").unique("match_uid").iter_rows(named=True):
            result_type_map[row["match_uid"]] = row["result_type"]

    if len(merged) > 0:
        new_bet_results = []
        new_notes = []
        for row in merged.iter_rows(named=True):
            current_bet_result = (row.get("bet_result") or "").strip()
            current_notes = (row.get("notes") or "").strip()
            uid = row.get("match_uid") or ""
            rt = result_type_map.get(uid)

            # Auto-fill notes for walkovers/retirements (don't overwrite)
            if rt and not current_notes:
                new_notes.append(rt)
            else:
                new_notes.append(current_notes)

            if current_bet_result:
                new_bet_results.append(current_bet_result)
                continue

            has_bet = bool((row.get("stake") or "").strip())

            if rt == "walkover" and has_bet:
                new_bet_results.append("V")
            elif rt == "retirement":
                # Leave blank — varies by book, user decides
                new_bet_results.append("")
            else:
                bet_side = (row.get("bet_side") or "").strip()
                result_val = (row.get("result") or "").strip()
                if bet_side in ("P1", "P2") and result_val in ("P1", "P2"):
                    new_bet_results.append("W" if bet_side == result_val else "L")
                else:
                    new_bet_results.append(current_bet_result)

        merged = merged.with_columns(
            pl.Series("bet_result", new_bet_results),
            pl.Series("notes", new_notes),
        )

    # 3c. Auto-fill p1_odds, p2_odds, book from best available odds.
    # book = max odds, tiebroken by odds_maps iteration order.
    # book2 = max odds among remaining books within <0.02 of best, same tiebreak.
    if len(merged) > 0 and odds_maps:
        new_p1_odds = []
        new_p2_odds = []
        new_books = []
        new_books2 = []
        for row in merged.iter_rows(named=True):
            current_stake = (row.get("stake") or "").strip()
            current_p1_odds = (row.get("p1_odds") or "").strip()
            current_p2_odds = (row.get("p2_odds") or "").strip()
            current_book = (row.get("book") or "").strip()
            current_book2 = (row.get("book2") or "").strip()

            if current_stake:
                new_p1_odds.append(current_p1_odds)
                new_p2_odds.append(current_p2_odds)
                new_books.append(current_book)
                new_books2.append(current_book2)
                continue

            uid = row.get("match_uid") or ""
            p1_id = (row.get("p1_id") or "").strip()
            p2_id = (row.get("p2_id") or "").strip()
            prediction = (row.get("prediction") or "").strip()

            best_p1 = None
            best_p2 = None
            pred_offers: list[tuple[str, float]] = []

            for book_name, book_odds in odds_maps.items():
                match_odds = book_odds.get(uid, {})
                p1_o = match_odds.get(p1_id)
                p2_o = match_odds.get(p2_id)
                if p1_o is not None and (best_p1 is None or p1_o > best_p1):
                    best_p1 = p1_o
                if p2_o is not None and (best_p2 is None or p2_o > best_p2):
                    best_p2 = p2_o
                pred_odds = p1_o if prediction == "P1" else p2_o if prediction == "P2" else None
                if pred_odds is not None:
                    pred_offers.append((book_name, pred_odds))

            if pred_offers:
                best_raw = max(o for _, o in pred_offers)
                primary = next(b for b, o in pred_offers if o == best_raw)
                candidates = [
                    (b, o)
                    for b, o in pred_offers
                    if b != primary and best_raw - o < 0.02 - 1e-9
                ]
                if candidates:
                    best2 = max(o for _, o in candidates)
                    secondary = next(b for b, o in candidates if o == best2)
                else:
                    secondary = ""
                new_books.append(primary)
                new_books2.append(secondary)
            else:
                new_books.append(current_book)
                new_books2.append(current_book2)

            new_p1_odds.append(f"{best_p1:.2f}" if best_p1 is not None else current_p1_odds)
            new_p2_odds.append(f"{best_p2:.2f}" if best_p2 is not None else current_p2_odds)

        merged = merged.with_columns(
            pl.Series("p1_odds", new_p1_odds),
            pl.Series("p2_odds", new_p2_odds),
            pl.Series("book", new_books),
            pl.Series("book2", new_books2),
        )

    # 3c2. Populate fav_edge_open from best opening odds across books on the
    # predicted side. Frozen once set — opening is captured a single time per
    # match and never recomputed.
    if len(merged) > 0 and opening_odds_maps:
        new_fav_open = []
        for row in merged.iter_rows(named=True):
            current = (row.get("fav_edge_open") or "").strip()
            if current:
                new_fav_open.append(current)
                continue

            uid = row.get("match_uid") or ""
            prediction = (row.get("prediction") or "").strip()
            p1_id = (row.get("p1_id") or "").strip()
            p2_id = (row.get("p2_id") or "").strip()
            if not uid or prediction not in ("P1", "P2"):
                new_fav_open.append("")
                continue

            try:
                pred_prob = float(row.get("pred_prob") or "")
            except (TypeError, ValueError):
                new_fav_open.append("")
                continue

            pred_pid = p1_id if prediction == "P1" else p2_id
            if not pred_pid:
                new_fav_open.append("")
                continue

            best_open = None
            for _, book_odds in opening_odds_maps.items():
                o = book_odds.get(uid, {}).get(pred_pid)
                if o is not None and (best_open is None or o > best_open):
                    best_open = o

            if best_open and best_open > 0:
                edge = pred_prob - (1.0 / best_open)
                new_fav_open.append(f"{edge:.4f}")
            else:
                new_fav_open.append("")

        merged = merged.with_columns(pl.Series("fav_edge_open", new_fav_open))

    # 3c3. Populate cell_cal + cal_tier from the production lead's sidecar —
    # but ONLY for matches that are first appearing this sync (`truly_new`).
    # For pre-existing rows, whatever value is already on the row is preserved
    # (frozen-once-set). Crucially we must NOT fill blanks on pre-existing
    # rows: those bets were placed under earlier model state, and stamping
    # them with today's sidecar values poisons the historical analysis.
    sidecar_path = _resolve_lead_sidecar_path()
    cal_lookup = load_cal_tiers_from_path(sidecar_path) if sidecar_path else {}
    if len(merged) > 0:
        new_cell_cal: list[str] = []
        new_cal_tier: list[str] = []
        for row in merged.iter_rows(named=True):
            current_tier = (row.get("cal_tier") or "").strip()
            uid = row.get("match_uid") or ""
            if current_tier or uid not in truly_new:
                # Preserve whatever's there. Pre-existing rows without a tier
                # stay null — they predate the feature and have no honest tier.
                new_cell_cal.append(row.get("cell_cal") or "")
                new_cal_tier.append(current_tier)
                continue
            sheet_circuit = (row.get("circuit") or "").strip()
            # Sheet stores display labels (ATP / CH); sidecar keys on raw
            # circuit values (tour / chal). Translate before lookup.
            circuit = CIRCUIT_LABELS_INVERSE.get(sheet_circuit, sheet_circuit)
            rnd = (row.get("round") or "").strip()
            cal = cal_lookup.get((circuit, rnd))
            tier = classify_cal_tier(cal)
            new_cell_cal.append(f"{cal:.4f}" if cal is not None else "")
            new_cal_tier.append(tier or "")
        merged = merged.with_columns(
            pl.Series("cell_cal", new_cell_cal),
            pl.Series("cal_tier", new_cal_tier),
        )

    # 3c4. court (indoor/outdoor) from matches.parquet — static match metadata,
    # refreshed from the source of truth each sync; preserved if a match isn't
    # in matches yet.
    if len(merged) > 0:
        indoor_map: dict[str, str] = {}
        if "indoor" in matches.columns and len(matches) > 0:
            for row in matches.select("match_uid", "indoor").unique("match_uid").iter_rows(named=True):
                v = row["indoor"]
                indoor_map[row["match_uid"]] = (
                    "indoor" if v is True else "outdoor" if v is False else ""
                )
        existing_court = (
            merged["court"].to_list() if "court" in merged.columns else [""] * len(merged)
        )
        uids = merged["match_uid"].to_list()
        merged = merged.with_columns(
            pl.Series("court", [
                indoor_map.get(uids[i] or "") or (existing_court[i] or "")
                for i in range(len(merged))
            ])
        )

    # 3d. Stamp bet_placed_at when we first see a stake
    if len(merged) > 0:
        now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
        new_bet_placed = []
        for row in merged.iter_rows(named=True):
            current = (row.get("bet_placed_at") or "").strip()
            stake = (row.get("stake") or "").strip()
            if stake and not current:
                new_bet_placed.append(now_str)
            else:
                new_bet_placed.append(current)
        merged = merged.with_columns(pl.Series("bet_placed_at", new_bet_placed))

    # 4. Re-pad time column (Google Sheets strips leading zeros)
    merged = merged.with_columns(
        pl.col("time").map_elements(
            lambda t: t.zfill(5) if t else t, return_dtype=pl.Utf8
        )
    )

    # 5. Sort
    from mvp.atptour.aggregators.matches import ROUND_ORDER

    merged = merged.with_columns(
        pl.col("round").replace_strict(ROUND_ORDER, default=99).alias("_round_order"),
        (pl.col("date") + " " + pl.col("time"))
        .min()
        .over("tournament_day", "tournament")
        .alias("_session_start"),
    )
    merged = merged.sort(
        ["_session_start", "tournament", "date", "time", "_round_order", "circuit"]
    ).drop("_round_order", "_session_start")

    return merged


class PredictionSync(Protocol):
    """Interface for reading/writing predictions to an external store."""

    def read_existing(self) -> pl.DataFrame: ...
    def write(self, df: pl.DataFrame) -> None: ...
