"""Tests for common utility functions."""

from datetime import date

import polars as pl
from pydantic import BaseModel

from mvp.common.utils import polars_schema_overrides


class SampleModel(BaseModel):
    required_str: str
    required_int: int
    optional_str: str | None = None
    optional_int: int | None = None
    optional_date: date | None = None
    optional_bool: bool | None = None


class TestPolarsSchemaOverrides:
    def test_only_optional_fields(self):
        overrides = polars_schema_overrides(SampleModel)
        assert "required_str" not in overrides
        assert "required_int" not in overrides
        assert overrides["optional_str"] == pl.String
        assert overrides["optional_int"] == pl.Int64
        assert overrides["optional_date"] == pl.Date
        assert overrides["optional_bool"] == pl.Boolean

    def test_empty_model(self):
        class Empty(BaseModel):
            name: str

        assert polars_schema_overrides(Empty) == {}
