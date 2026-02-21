"""Tests for shared mapping tables and normalization functions."""

import pytest

from mvp.common.enums import Round
from mvp.common.mappings import (
    ROUND_NORMALIZATION,
    SR_ID_MAPPING,
    is_placeholder_id,
    map_player_id,
    normalize_flag_url,
    normalize_round,
    parse_duration,
)


class TestNormalizeRound:
    """Test round name normalization."""

    def test_all_mapping_entries_resolve(self):
        for raw, expected in ROUND_NORMALIZATION.items():
            assert normalize_round(raw) == expected

    def test_final_variants(self):
        assert normalize_round("Final") == Round.F
        assert normalize_round("Finals") == Round.F

    def test_semifinal_variants(self):
        assert normalize_round("Semifinals") == Round.SF
        assert normalize_round("Semi-Finals") == Round.SF
        assert normalize_round("Semifinal") == Round.SF

    def test_quarterfinal_variants(self):
        assert normalize_round("Quarterfinals") == Round.QF
        assert normalize_round("Quarter-Finals") == Round.QF
        assert normalize_round("Quarterfinal") == Round.QF

    def test_numbered_rounds(self):
        assert normalize_round("Round of 16") == Round.R16
        assert normalize_round("Round of 32") == Round.R32
        assert normalize_round("Round of 64") == Round.R64
        assert normalize_round("Round of 128") == Round.R128

    def test_round_robin(self):
        assert normalize_round("Round Robin") == Round.RR

    def test_round_robin_day_suffix_stripped(self):
        assert normalize_round("Round Robin Day 1") == Round.RR
        assert normalize_round("Round Robin Day 2") == Round.RR
        assert normalize_round("Round Robin Day 3") == Round.RR

    def test_qualifying_rounds(self):
        assert normalize_round("1st Round Qualifying") == Round.Q1
        assert normalize_round("2nd Round Qualifying") == Round.Q2
        assert normalize_round("3rd Round Qualifying") == Round.Q3

    def test_third_place_variants(self):
        assert normalize_round("Bronze Medal Match") == Round.THIRDPLACE
        assert normalize_round("Olympic Bronze") == Round.THIRDPLACE
        assert normalize_round("Third Place") == Round.THIRDPLACE
        assert normalize_round("3rd/4th") == Round.THIRDPLACE
        assert normalize_round("3rd/4th Place Match") == Round.THIRDPLACE

    def test_host_city_finals(self):
        assert normalize_round("Host City Finals") == Round.HCF

    def test_strips_whitespace(self):
        assert normalize_round("  Final  ") == Round.F

    def test_unmapped_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unmapped round name"):
            normalize_round("Unknown Round")

    def test_empty_string_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unmapped round name"):
            normalize_round("")


class TestMapPlayerId:
    """Test Sportradar ID mapping."""

    def test_all_sr_mappings_resolve(self):
        for sr_id, expected_atp in SR_ID_MAPPING.items():
            assert map_player_id(sr_id) == expected_atp

    def test_normal_atp_id_passthrough(self):
        assert map_player_id("s0ag") == "s0ag"
        assert map_player_id("mm58") == "mm58"
        assert map_player_id("TE30") == "TE30"

    def test_unmapped_sr_id_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unmapped Sportradar player ID"):
            map_player_id("SR:COMPETITOR:999999")

    def test_sr_prefix_case_insensitive(self):
        assert map_player_id("sr:competitor:972327") == "J0DZ"
        assert map_player_id("Sr:Competitor:972327") == "J0DZ"


class TestIsPlaceholderId:
    """Test placeholder player ID detection."""

    def test_placeholder_ids(self):
        assert is_placeholder_id("0") is True
        assert is_placeholder_id("AAA1") is True
        assert is_placeholder_id("AAA2") is True

    def test_normal_ids_are_not_placeholders(self):
        assert is_placeholder_id("s0ag") is False
        assert is_placeholder_id("mm58") is False
        assert is_placeholder_id("TE30") is False

    def test_similar_but_not_placeholder(self):
        assert is_placeholder_id("00") is False
        assert is_placeholder_id("AAA3") is False
        assert is_placeholder_id("1") is False


class TestNormalizeFlagUrl:
    """Test flag URL country code extraction."""

    def test_standard_extraction(self):
        assert normalize_flag_url("flags.svg#flag-ita") == "ita"
        assert normalize_flag_url("flags.svg#flag-rus") == "rus"
        assert normalize_flag_url("flags.svg#flag-usa") == "usa"

    def test_full_path(self):
        assert normalize_flag_url("/assets/atptour/assets/flags.svg#flag-gbr") == "gbr"

    def test_missing_pattern_raises_valueerror(self):
        with pytest.raises(ValueError, match="does not contain"):
            normalize_flag_url("flags.svg")

    def test_empty_string_raises_valueerror(self):
        with pytest.raises(ValueError, match="does not contain"):
            normalize_flag_url("")


class TestParseDuration:
    """Test duration string parsing."""

    def test_hours_minutes(self):
        assert parse_duration("03:44") == 13440  # 3h 44m
        assert parse_duration("01:30") == 5400  # 1h 30m
        assert parse_duration("00:00") == 0

    def test_hours_minutes_seconds(self):
        assert parse_duration("02:50:39") == 10239  # 2h 50m 39s
        assert parse_duration("01:00:00") == 3600  # 1h exactly
        assert parse_duration("00:00:01") == 1  # 1 second

    def test_strips_whitespace(self):
        assert parse_duration("  03:44  ") == 13440

    def test_empty_string_raises_valueerror(self):
        with pytest.raises(ValueError, match="Empty duration"):
            parse_duration("")

    def test_invalid_format_raises_valueerror(self):
        with pytest.raises(ValueError):
            parse_duration("3:44:00:00")  # Too many parts

    def test_non_numeric_raises_valueerror(self):
        with pytest.raises(ValueError, match="Non-numeric"):
            parse_duration("ab:cd")
