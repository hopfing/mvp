"""Tests for SheetsSync (mocked -- no real Sheets calls)."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from mvp.integrations.base import COLUMN_NAMES, generate_formulas


class TestSheetsSync:
    def test_read_existing_empty_sheet(self):
        """Empty sheet (no data at all) returns empty DataFrame with correct columns."""
        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = []
            sync._worksheet = mock_ws

            result = sync.read_existing()
            assert len(result) == 0
            assert list(result.columns) == COLUMN_NAMES

    def test_read_existing_header_only(self):
        """Sheet with header but no data rows returns empty DataFrame."""
        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [COLUMN_NAMES]
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

        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [COLUMN_NAMES, row]
            sync._worksheet = mock_ws

            result = sync.read_existing()
            assert len(result) == 1
            assert result["match_uid"].item() == "M1"
            assert result["p1"].item() == "John"

    def test_write_clears_and_updates(self):
        """Write clears the sheet then updates with header + data."""
        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame({col: ["val"] for col in COLUMN_NAMES})
            sync.write(df)

            mock_ws.clear.assert_called_once()
            mock_ws.update.assert_called_once()
            call_kwargs = mock_ws.update.call_args
            assert call_kwargs.kwargs["value_input_option"] == "USER_ENTERED"

    def test_write_injects_formulas(self):
        """Write replaces formula column values with actual formulas."""
        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame({col: ["val"] for col in COLUMN_NAMES})
            sync.write(df)

            call_args = mock_ws.update.call_args
            all_data = call_args.kwargs["values"]

            # Row 0 is header, row 1 is data
            data_row = all_data[1]
            expected_formulas = generate_formulas(2)  # sheet row 2
            for col_name, formula in expected_formulas.items():
                col_idx = COLUMN_NAMES.index(col_name)
                assert data_row[col_idx] == formula, f"Formula mismatch for {col_name}"

    def test_write_header_is_column_names(self):
        """Write puts COLUMN_NAMES as the header row."""
        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame({col: ["val"] for col in COLUMN_NAMES})
            sync.write(df)

            call_args = mock_ws.update.call_args
            all_data = call_args.kwargs["values"]
            assert all_data[0] == COLUMN_NAMES

    def test_schema_validation_on_read(self):
        """Reading a sheet with wrong columns raises ValueError."""
        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            mock_ws.get_all_values.return_value = [["wrong", "columns"]]
            sync._worksheet = mock_ws

            with pytest.raises(ValueError, match="Schema mismatch"):
                sync.read_existing()

    def test_init_missing_env_vars(self):
        """SheetsSync.__init__ raises ValueError if env vars missing."""
        with (
            patch("mvp.integrations.sheets.gspread"),
            patch("mvp.integrations.sheets.load_dotenv"),
            patch.dict("os.environ", {}, clear=True),
        ):
            from mvp.integrations.sheets import SheetsSync

            with pytest.raises(ValueError, match="Missing"):
                SheetsSync()

    def test_write_multiple_rows(self):
        """Write handles multiple data rows with correct formula row numbers."""
        with patch("mvp.integrations.sheets.gspread"):
            from mvp.integrations.sheets import SheetsSync

            sync = SheetsSync.__new__(SheetsSync)
            mock_ws = MagicMock()
            sync._worksheet = mock_ws

            df = pl.DataFrame(
                {col: ["val1", "val2", "val3"] for col in COLUMN_NAMES}
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
