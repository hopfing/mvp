"""Tests for SheetsSync (mocked -- no real Sheets calls)."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from mvp.gsheets.base import (
    COLUMN_NAMES,
    SHEET_HEADERS,
    _col_letter,
    generate_formulas,
)


class TestSheetsSync:
    def test_read_existing_empty_sheet(self):
        """Empty sheet (no data at all) returns empty DataFrame with correct columns."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = []
            sync._worksheet = mock_ws

            result = sync.read_existing()
            assert len(result) == 0
            assert list(result.columns) == COLUMN_NAMES

    def test_read_existing_header_only(self):
        """Sheet with header but no data rows returns empty DataFrame."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [SHEET_HEADERS]
            sync._worksheet = mock_ws

            result = sync.read_existing()
            assert len(result) == 0
            assert list(result.columns) == COLUMN_NAMES

    def test_read_existing_with_data(self):
        """Sheet with data returns correct DataFrame."""
        row = [""] * len(COLUMN_NAMES)
        row[COLUMN_NAMES.index("match_uid")] = "M1"
        row[COLUMN_NAMES.index("p1")] = "John"
        row[COLUMN_NAMES.index("p2")] = "Jane"

        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [SHEET_HEADERS, row]
            sync._worksheet = mock_ws

            result = sync.read_existing()
            assert len(result) == 1
            assert result["match_uid"].item() == "M1"
            assert result["p1"].item() == "John"

    def test_read_existing_tolerates_display_padded_headers(self):
        """A display-only number format must not halt the sync. Accounting pads
        text with spaces via its `_(@_)` section, and get_all_values() renders
        FORMATTED_VALUE, so the padding is in what we read — not what's stored.
        """
        padded = list(SHEET_HEADERS)
        padded[COLUMN_NAMES.index("net")] = " net "

        row = [""] * len(COLUMN_NAMES)
        row[COLUMN_NAMES.index("match_uid")] = "M1"

        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [padded, row]
            sync._worksheet = mock_ws

            result = sync.read_existing()
            assert len(result) == 1
            assert result["match_uid"].item() == "M1"

    def test_read_existing_still_raises_on_real_mismatch(self):
        """Stripping must not weaken the check into accepting a wrong header."""
        wrong = list(SHEET_HEADERS)
        wrong[COLUMN_NAMES.index("net")] = "profit"

        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [wrong]
            sync._worksheet = mock_ws

            with pytest.raises(ValueError, match="Schema mismatch"):
                sync.read_existing()

    def test_write_updates_then_clears_only_the_tail(self):
        """Write overwrites the live range first, then clears only the rows
        below it. Never a whole-tab clear: if the write that followed failed,
        the tab would be left empty and every row of history lost."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame({col: ["val"] for col in COLUMN_NAMES})
            sync.write(df)

            mock_ws.clear.assert_not_called()
            mock_ws.update.assert_called_once()
            call_kwargs = mock_ws.update.call_args
            assert call_kwargs.kwargs["value_input_option"] == "USER_ENTERED"

            # Header + one data row, so the tail starts at row 3.
            mock_ws.batch_clear.assert_called_once()
            (ranges,) = mock_ws.batch_clear.call_args.args
            assert ranges == [f"A3:{_col_letter(len(COLUMN_NAMES) - 1)}"]

    def test_write_injects_formulas_when_empty(self):
        """Write injects formulas into empty formula cells."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame({col: [""] for col in COLUMN_NAMES})
            sync.write(df)

            call_args = mock_ws.update.call_args
            all_data = call_args.kwargs["values"]

            data_row = all_data[1]
            expected_formulas = generate_formulas(2)
            for col_name, formula in expected_formulas.items():
                col_idx = COLUMN_NAMES.index(col_name)
                assert data_row[col_idx] == formula, f"Formula mismatch for {col_name}"

    def test_write_preserves_manual_formula_overrides(self):
        """Write does not overwrite non-empty formula cells with formulas."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            data = {col: [""] for col in COLUMN_NAMES}
            data["to_win"] = ["150.50"]
            df = pl.DataFrame(data)
            sync.write(df)

            call_args = mock_ws.update.call_args
            all_data = call_args.kwargs["values"]
            data_row = all_data[1]
            col_idx = COLUMN_NAMES.index("to_win")
            assert data_row[col_idx] == "150.50"

    def test_write_relives_pred_odds_while_bet_is_open(self):
        """pred_odds tracks the current price until a bet is placed.

        A previous sync leaves the sheet's computed price behind as a literal,
        so re-emitting the formula only when the cell is empty would freeze
        pred_odds on the first price ever seen.
        """
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.base import generate_formulas
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            data = {col: [""] for col in COLUMN_NAMES}
            data["pred_odds"] = ["2.38"]  # stale literal from an earlier sync
            sync.write(pl.DataFrame(data))

            data_row = mock_ws.update.call_args.kwargs["values"][1]
            col_idx = COLUMN_NAMES.index("pred_odds")
            assert data_row[col_idx] == generate_formulas(2)["pred_odds"]

    def test_write_freezes_pred_odds_once_bet_is_placed(self):
        """A placed bet keeps its literal, so a hand-corrected line survives."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            data = {col: [""] for col in COLUMN_NAMES}
            data["pred_odds"] = ["1.95"]  # the line actually taken
            data["bet_placed_at"] = ["2026-07-29 16:00"]
            sync.write(pl.DataFrame(data))

            data_row = mock_ws.update.call_args.kwargs["values"][1]
            assert data_row[COLUMN_NAMES.index("pred_odds")] == "1.95"

    def test_write_header_is_sheet_headers(self):
        """Write puts SHEET_HEADERS as the header row (display names, not internal names)."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame({col: ["val"] for col in COLUMN_NAMES})
            sync.write(df)

            call_args = mock_ws.update.call_args
            all_data = call_args.kwargs["values"]
            assert all_data[0] == SHEET_HEADERS

    def test_schema_validation_on_read(self):
        """Reading a sheet with wrong columns raises ValueError."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [["wrong", "columns"]]
            sync._worksheet = mock_ws

            with pytest.raises(ValueError, match="Schema mismatch"):
                sync.read_existing()

    def test_init_missing_env_vars(self):
        """SheetsSync.__init__ raises ValueError if env vars missing."""
        with (
            patch("mvp.gsheets.sheets.gspread"),
            patch("mvp.gsheets.sheets.load_dotenv"),
            patch.dict("os.environ", {}, clear=True),
        ):
            from mvp.gsheets.sheets import SheetsSync

            with pytest.raises(ValueError, match="Missing"):
                SheetsSync()

    def test_write_multiple_rows(self):
        """Write handles multiple data rows with correct formula row numbers."""
        with patch("mvp.gsheets.sheets.gspread"):
            from mvp.gsheets.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame(
                {col: ["", "", ""] for col in COLUMN_NAMES}
            )
            sync.write(df)

            call_args = mock_ws.update.call_args
            all_data = call_args.kwargs["values"]

            assert len(all_data) == 4  # header + 3 data rows

            for i in range(3):
                expected_formulas = generate_formulas(i + 2)  # rows 2, 3, 4
                data_row = all_data[i + 1]
                for col_name, formula in expected_formulas.items():
                    col_idx = COLUMN_NAMES.index(col_name)
                    assert data_row[col_idx] == formula
