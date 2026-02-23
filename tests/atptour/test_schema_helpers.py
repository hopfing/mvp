"""Tests for ATP-specific schema validation helpers."""

import pytest

from mvp.atptour.schema_helpers import (
    empty_to_none,
    parse_indoor,
    strip_or_none,
    validate_doubles_partners,
    validate_match_uid_placeholders,
    validate_winner_in_players,
)
from mvp.common.enums import DrawType


class TestParseIndoor:
    def test_indoor(self):
        assert parse_indoor("I") is True

    def test_outdoor(self):
        assert parse_indoor("O") is False

    def test_empty_string(self):
        assert parse_indoor("") is None

    def test_none(self):
        assert parse_indoor(None) is None

    def test_bool_passthrough(self):
        assert parse_indoor(True) is True
        assert parse_indoor(False) is False

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unknown InOutdoor"):
            parse_indoor("X")


class TestEmptyToNone:
    def test_empty_string(self):
        assert empty_to_none("") is None

    def test_non_empty(self):
        assert empty_to_none("hello") == "hello"

    def test_none_passthrough(self):
        assert empty_to_none(None) is None

    def test_zero(self):
        assert empty_to_none(0) == 0


class TestStripOrNone:
    def test_strips_whitespace(self):
        assert strip_or_none("  hello  ") == "hello"

    def test_whitespace_only_returns_none(self):
        assert strip_or_none("   ") is None

    def test_empty_string_returns_none(self):
        assert strip_or_none("") is None

    def test_none_returns_none(self):
        assert strip_or_none(None) is None

    def test_no_whitespace(self):
        assert strip_or_none("hello") == "hello"


class TestValidateWinnerInPlayers:
    def test_winner_is_p1(self):
        validate_winner_in_players("AB12", "AB12", "CD34")

    def test_winner_is_p2(self):
        validate_winner_in_players("CD34", "AB12", "CD34")

    def test_winner_mismatch_raises(self):
        with pytest.raises(ValueError, match="winner_id"):
            validate_winner_in_players("EF56", "AB12", "CD34")


class TestValidateDoublesPartners:
    def test_doubles_with_partners(self):
        validate_doubles_partners(DrawType.doubles, ["AA11", "BB22"])

    def test_doubles_missing_partner_raises(self):
        with pytest.raises(ValueError, match="non-null for doubles"):
            validate_doubles_partners(DrawType.doubles, ["AA11", None])

    def test_singles_no_partners(self):
        validate_doubles_partners(DrawType.singles, [None, None])

    def test_singles_with_partner_raises(self):
        with pytest.raises(ValueError, match="null for singles"):
            validate_doubles_partners(DrawType.singles, ["AA11", None])


class TestValidateMatchUidPlaceholders:
    def test_placeholder_with_null_uid(self):
        validate_match_uid_placeholders(None, ["0", "AB12"])

    def test_placeholder_with_uid_raises(self):
        with pytest.raises(ValueError, match="must be null"):
            validate_match_uid_placeholders("some_uid", ["0", "AB12"])

    def test_no_placeholder_with_uid(self):
        validate_match_uid_placeholders("some_uid", ["AB12", "CD34"])

    def test_no_placeholder_without_uid_raises(self):
        with pytest.raises(ValueError, match="must be non-null"):
            validate_match_uid_placeholders(None, ["AB12", "CD34"])
