"""Tests for prediction sync base module."""

from mvp.integrations.base import (
    COLUMN_NAMES,
    COLUMN_SCHEMA,
    FORMULA_COLUMNS,
    PIPELINE_COLUMNS,
    USER_COLUMNS,
)


class TestColumnSchema:
    def test_column_schema_has_34_columns(self):
        assert len(COLUMN_SCHEMA) == 34

    def test_match_uid_is_in_schema(self):
        assert "match_uid" in COLUMN_NAMES

    def test_pipeline_columns_are_subset_of_schema(self):
        assert PIPELINE_COLUMNS.issubset(set(COLUMN_NAMES))

    def test_user_columns_are_subset_of_schema(self):
        assert USER_COLUMNS.issubset(set(COLUMN_NAMES))

    def test_formula_columns_are_subset_of_schema(self):
        assert FORMULA_COLUMNS.issubset(set(COLUMN_NAMES))

    def test_no_column_in_both_pipeline_and_user(self):
        assert PIPELINE_COLUMNS.isdisjoint(USER_COLUMNS)

    def test_column_names_unique(self):
        assert len(COLUMN_NAMES) == len(set(COLUMN_NAMES))
