"""Tests for sheet data persistence."""

import polars as pl
import pytest
from pathlib import Path


class TestSheetPersistence:
    def test_saves_sheet_to_parquet(self, tmp_path):
        """Sheet data should be saved as-is to parquet."""
        sheet_df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "circuit": ["CH", "ATP"],
            "p1_odds": ["2.10", "1.80"],
            "result": ["P1", ""],
        })

        output_path = tmp_path / "bets.parquet"
        sheet_df.write_parquet(output_path)

        loaded = pl.read_parquet(output_path)
        assert loaded.shape == (2, 4)
        assert loaded["circuit"].to_list() == ["CH", "ATP"]

    def test_overwrites_existing(self, tmp_path):
        """Each save should be a full overwrite, not append."""
        output_path = tmp_path / "bets.parquet"

        old = pl.DataFrame({"match_uid": ["m0"], "circuit": ["CH"]})
        old.write_parquet(output_path)

        new = pl.DataFrame({"match_uid": ["m1", "m2"], "circuit": ["ATP", "CH"]})
        new.write_parquet(output_path)

        loaded = pl.read_parquet(output_path)
        assert loaded.shape == (2, 2)
        assert "m0" not in loaded["match_uid"].to_list()
