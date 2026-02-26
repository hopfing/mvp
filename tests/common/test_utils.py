"""Tests for common utility functions."""

from datetime import date

import polars as pl
from pydantic import BaseModel

from mvp.common.utils import polars_schema


class SampleModel(BaseModel):
    required_str: str
    required_int: int
    optional_str: str | None = None
    optional_int: int | None = None
    optional_date: date | None = None
    optional_bool: bool | None = None


class TestPolarsSchema:
    def test_includes_non_nullable_fields(self):
        """polars_schema should map ALL fields, not just nullable ones."""
        schema = polars_schema(SampleModel)
        assert schema["required_str"] == pl.String
        assert schema["required_int"] == pl.Int64

    def test_includes_nullable_fields(self):
        schema = polars_schema(SampleModel)
        assert schema["optional_str"] == pl.String
        assert schema["optional_int"] == pl.Int64
        assert schema["optional_date"] == pl.Date
        assert schema["optional_bool"] == pl.Boolean

    def test_all_fields_present(self):
        schema = polars_schema(SampleModel)
        assert set(schema.keys()) == {
            "required_str",
            "required_int",
            "optional_str",
            "optional_int",
            "optional_date",
            "optional_bool",
        }

    def test_non_nullable_only_model(self):
        class Simple(BaseModel):
            name: str
            age: int

        schema = polars_schema(Simple)
        assert schema == {"name": pl.String, "age": pl.Int64}
