"""Integration tests for SheetsSync (requires real Google Sheets credentials)."""

import pytest
import polars as pl

from mvp.gsheets.base import COLUMN_NAMES


@pytest.mark.integration
class TestSheetsIntegration:
    def test_round_trip(self):
        """Write data then read it back."""
        from mvp.gsheets.sheets import SheetsSync

        sync = SheetsSync()

        # Write a test row
        test_df = pl.DataFrame(
            {col: ["test_value"] for col in COLUMN_NAMES}
        )
        test_df = test_df.with_columns(pl.lit("INTEGRATION_TEST").alias("match_uid"))
        sync.write(test_df)

        # Read back
        result = sync.read_existing()
        assert len(result) >= 1
        assert "INTEGRATION_TEST" in result["match_uid"].to_list()
