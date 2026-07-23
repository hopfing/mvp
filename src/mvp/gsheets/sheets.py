"""Google Sheets implementation of PredictionSync."""


import json
import logging
import os
from pathlib import Path

import gspread
import polars as pl
from dotenv import load_dotenv
from gspread.utils import ValueRenderOption

from mvp.gsheets.base import (
    COLUMN_NAMES,
    FORMULA_PRESERVE_COLUMNS,
    FREEZE_AT_BET_COLUMNS,
    SHEET_HEADERS,
    _col_letter,
    generate_formulas,
)

logger = logging.getLogger(__name__)


class SheetsSync:
    """Read/write predictions to a Google Sheet."""

    def __init__(self) -> None:
        load_dotenv()
        creds_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
        sheet_id = os.environ.get("GOOGLE_SHEET_ID")

        if not creds_path or not sheet_id:
            raise ValueError(
                "Missing GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SHEET_ID in .env"
            )

        creds = json.loads(Path(creds_path).read_text())
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open_by_key(sheet_id)
        self._spreadsheet = spreadsheet
        self._worksheet = spreadsheet.worksheet("bets")

    def read_config(self) -> dict[str, str]:
        """Read the `config` tab (header row of names + one value row) into a
        {name: value} dict. Empty dict if the tab is missing or has no value
        row, so the sync degrades gracefully when config isn't set up."""
        try:
            values = self._spreadsheet.worksheet("config").get_all_values()
        except Exception:
            return {}
        if len(values) < 2:
            return {}
        header, row = values[0], values[1]
        return {
            header[i].strip(): row[i].strip()
            for i in range(min(len(header), len(row)))
            if header[i].strip() and row[i].strip()
        }

    def read_existing(self) -> pl.DataFrame:
        """Read all rows from the sheet."""
        data = self._worksheet.get_all_values()

        if not data:
            return pl.DataFrame(schema={col: pl.Utf8 for col in COLUMN_NAMES})

        header = data[0]
        if header != SHEET_HEADERS:
            raise ValueError(
                f"Schema mismatch: expected {SHEET_HEADERS}, got {header}"
            )

        if len(data) == 1:
            return pl.DataFrame(schema={col: pl.Utf8 for col in COLUMN_NAMES})

        rows = [list(r) for r in data[1:]]

        # User-maintained columns may hold a formula (e.g. an inherit-from-above
        # bankroll). get_all_values() returns the computed value, which we'd then
        # write back as a literal — freezing the formula. Re-read those columns
        # with FORMULA rendering and overlay the raw formula text so it survives
        # the round-trip.
        preserve_idx = [COLUMN_NAMES.index(c) for c in FORMULA_PRESERVE_COLUMNS]
        if preserve_idx:
            formula_rows = self._worksheet.get_all_values(
                value_render_option=ValueRenderOption.formula
            )[1:]
            for i, row_list in enumerate(rows):
                if i >= len(formula_rows):
                    break
                fr = formula_rows[i]
                for ci in preserve_idx:
                    if ci < len(fr):
                        row_list[ci] = fr[ci]

        return pl.DataFrame(
            rows, schema={col: pl.Utf8 for col in COLUMN_NAMES}, orient="row"
        )

    def write(self, df: pl.DataFrame) -> None:
        """Write merged DataFrame to the sheet, including formulas."""
        str_df = df.select(
            pl.col(c).cast(pl.Utf8).fill_null("") for c in COLUMN_NAMES
        )

        rows = str_df.rows()

        always_formula = {"elo_diff", "fav_edge", "dog_edge", "pred_result", "bet_odds"}
        bet_placed_idx = COLUMN_NAMES.index("bet_placed_at")

        cell_rows = []
        for i, row in enumerate(rows):
            row_list = list(row)
            sheet_row = i + 2  # 1-indexed, row 1 is header
            formulas = generate_formulas(sheet_row)
            bet_placed = bool(row_list[bet_placed_idx].strip())
            for col_name, formula in formulas.items():
                col_idx = COLUMN_NAMES.index(col_name)
                if col_name in FREEZE_AT_BET_COLUMNS:
                    # Live while the bet is open; once placed, keep whatever
                    # literal is already there (the frozen bet-time snapshot).
                    if not bet_placed:
                        row_list[col_idx] = formula
                    continue
                if col_name in always_formula or not row_list[col_idx]:
                    row_list[col_idx] = formula
            cell_rows.append(row_list)

        all_data = [SHEET_HEADERS] + cell_rows

        self._worksheet.clear()
        self._worksheet.update(
            range_name=f"A1:{_col_letter(len(COLUMN_NAMES) - 1)}{len(all_data)}",
            values=all_data,
            value_input_option="USER_ENTERED",
        )
        logger.info("Wrote %d rows to Google Sheets", len(cell_rows))
