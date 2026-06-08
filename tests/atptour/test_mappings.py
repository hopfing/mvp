"""Tests for ATP-specific mapping tables and normalization functions."""

import pytest

from mvp.atptour.mappings import (
    MATCH_UID_PATTERN,
    ROUND_NORMALIZATION,
    SR_ID_MAPPING,
    create_match_uid,
    is_placeholder_id,
    is_unknown_round,
    map_player_id,
    normalize_flag_url,
    normalize_round,
    parse_duration,
    parse_seed_entry,
)
from mvp.common.enums import Round


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

    def test_is_unknown_round_true_for_sentinel(self):
        # ATP's explicit no-round sentinel — skipped at the transformer level.
        assert is_unknown_round("Unknown Round") is True
        assert is_unknown_round("  Unknown Round  ") is True

    def test_is_unknown_round_false_for_real_rounds(self):
        assert is_unknown_round("Quarterfinals") is False
        assert is_unknown_round("1st Round Qualifying") is False
        assert is_unknown_round("") is False

    def test_empty_string_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unmapped round name"):
            normalize_round("")

    def test_enum_member_name_lookup(self):
        assert normalize_round("F") == Round.F
        assert normalize_round("QF") == Round.QF
        assert normalize_round("R16") == Round.R16
        assert normalize_round("BRONZE") == Round.BRONZE

    def test_enum_member_name_case_insensitive(self):
        assert normalize_round("qf") == Round.QF
        assert normalize_round("r16") == Round.R16


class TestMapPlayerId:
    """Test Sportradar ID mapping."""

    def test_all_sr_mappings_resolve(self):
        for sr_id, expected_atp in SR_ID_MAPPING.items():
            assert map_player_id(sr_id) == expected_atp

    def test_normal_atp_id_uppercased(self):
        assert map_player_id("s0ag") == "S0AG"
        assert map_player_id("mm58") == "MM58"
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
        assert is_placeholder_id("AAA9") is False
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


class TestCreateMatchUid:
    """Test match UID creation."""

    def test_singles(self):
        uid = create_match_uid(
            2023, "404", Round.F, ["AB12", "CD34"], is_doubles=False
        )
        assert uid == "2023_404_SGL_F_AB12_CD34"

    def test_doubles(self):
        uid = create_match_uid(
            2023,
            "404",
            Round.F,
            ["AB12", "CD34", "EF56", "GH78"],
            is_doubles=True,
        )
        assert uid == "2023_404_DBL_F_AB12_CD34_EF56_GH78"

    def test_ids_sorted_alphabetically(self):
        uid = create_match_uid(
            2023, "404", Round.F, ["ZZ99", "AA01"], is_doubles=False
        )
        assert uid == "2023_404_SGL_F_AA01_ZZ99"

    def test_matches_pattern(self):
        uid = create_match_uid(
            2023, "404", Round.QF, ["AB12", "CD34"], is_doubles=False
        )
        assert MATCH_UID_PATTERN.match(uid)

    def test_sportradar_id_raises(self):
        with pytest.raises(ValueError, match="Sportradar"):
            create_match_uid(
                2023, "404", Round.F, ["SR:COMPETITOR:123", "CD34"], is_doubles=False
            )


class TestParseSeedEntry:
    """Test seed/entry parsing."""

    def test_numeric_seed(self):
        assert parse_seed_entry("1") == (1, None)
        assert parse_seed_entry("16") == (16, None)

    def test_entry_only(self):
        assert parse_seed_entry("WC") == (None, "WC")
        assert parse_seed_entry("LL") == (None, "LL")

    def test_seed_and_entry(self):
        assert parse_seed_entry("1/Alt") == (1, "Alt")

    def test_parenthesized_seed(self):
        assert parse_seed_entry("(3)") == (3, None)

    def test_empty_returns_none(self):
        assert parse_seed_entry("") == (None, None)
        assert parse_seed_entry(None) == (None, None)
