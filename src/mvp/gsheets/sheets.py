"""Google Sheets implementation of PredictionSync."""


import json
import logging
import os
from pathlib import Path

import gspread
import polars as pl
from dotenv import load_dotenv

from mvp.gsheets.base import (
    COLUMN_NAMES,
    FORMULA_COLUMNS,
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
        self._worksheet = spreadsheet.worksheet("bets")

    def read_existing(self) -> pl.DataFrame:
        """Read all rows from the sheet."""
        data = self._worksheet.get_all_values()

        if not data:
            return pl.DataFrame(schema={col: pl.Utf8 for col in COLUMN_NAMES})

        header = data[0]
        if header != COLUMN_NAMES:
            raise ValueError(
                f"Schema mismatch: expected {COLUMN_NAMES}, got {header}"
            )

        if len(data) == 1:
            return pl.DataFrame(schema={col: pl.Utf8 for col in COLUMN_NAMES})

        rows = data[1:]
        return pl.DataFrame(
            rows, schema={col: pl.Utf8 for col in COLUMN_NAMES}, orient="row"
        )

    def write(self, df: pl.DataFrame) -> None:
        """Write merged DataFrame to the sheet, including formulas."""
        str_df = df.select(
            pl.col(c).cast(pl.Utf8).fill_null("") for c in COLUMN_NAMES
        )

        rows = str_df.rows()

        cell_rows = []
        for i, row in enumerate(rows):
            row_list = list(row)
            sheet_row = i + 2  # 1-indexed, row 1 is header
            formulas = generate_formulas(sheet_row)
            always_formula = {"elo_diff", "fav_edge", "dog_edge", "pred_result", "bet_odds"}
            for col_name, formula in formulas.items():
                col_idx = COLUMN_NAMES.index(col_name)
                if col_name in always_formula or not row_list[col_idx]:
                    row_list[col_idx] = formula
            cell_rows.append(row_list)

        all_data = [COLUMN_NAMES] + cell_rows

        self._worksheet.clear()
        self._worksheet.update(
            range_name=f"A1:{_col_letter(len(COLUMN_NAMES) - 1)}{len(all_data)}",
            values=all_data,
            value_input_option="USER_ENTERED",
        )
        logger.info("Wrote %d rows to Google Sheets", len(cell_rows))
