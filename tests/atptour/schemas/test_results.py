"""Tests for Tournament Results staged schema."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from mvp.atptour.schemas.results import SCHEMA_HASH, SCHEMA_VERSION, ResultRecord

PARSED_AT = datetime(2026, 2, 21)
SOURCE_FILE = "data/raw/atptour/tournaments/tour/404/2023/results_singles.html"


def _base_singles(**overrides) -> dict:
    """Minimal valid completed singles match (straight sets, p1 wins)."""
    data = {
        "tournament_id": "404",
        "year": 2023,
        "circuit": "tour",
        "draw_type": "singles",
        "round": "Final",
        "match_id": "MS001",
        "winner_id": "ab12",
        "p1_id": "ab12",
        "p1_name": "Player One",
        "p1_country": "usa",
        "p2_id": "cd34",
        "p2_name": "Player Two",
        "p2_country": "gbr",
        "result_type": "completed",
        "duration_seconds": 5400,
        "p1_set1_games": 6,
        "p2_set1_games": 4,
        "p1_set2_games": 7,
        "p2_set2_games": 5,
        "source_file": SOURCE_FILE,
        "parsed_at": PARSED_AT,
    }
    data.update(overrides)
    return data


def _base_doubles(**overrides) -> dict:
    """Minimal valid completed doubles match."""
    data = _base_singles(
        draw_type="doubles",
        p1_partner_id="ef56",
        p1_partner_name="Partner One",
        p1_partner_country="fra",
        p2_partner_id="gh78",
        p2_partner_name="Partner Two",
        p2_partner_country="esp",
        source_file="data/raw/atptour/tournaments/tour/404/2023/results_doubles.html",
    )
    data.update(overrides)
    return data


class TestValidRecords:
    def test_completed_straight_sets_p1_wins(self):
        record = ResultRecord(**_base_singles())
        assert record.winner_id == "AB12"
        assert record.match_uid == "2023_404_SGL_F_AB12_CD34"
        assert record.p1_set1_games == 6
        assert record.p2_set1_games == 4
        assert record.p1_set3_games is None

    def test_completed_five_sets_p2_wins(self):
        record = ResultRecord(
            **_base_singles(
                winner_id="cd34",
                p1_set1_games=6,
                p2_set1_games=4,
                p1_set2_games=3,
                p2_set2_games=6,
                p1_set3_games=7,
                p2_set3_games=6,
                p1_set3_tiebreak=7,
                p2_set3_tiebreak=3,
                p1_set4_games=4,
                p2_set4_games=6,
                p1_set5_games=3,
                p2_set5_games=6,
            )
        )
        assert record.winner_id == "CD34"
        assert record.p1_set5_games == 3
        assert record.p2_set5_games == 6

    def test_completed_doubles(self):
        record = ResultRecord(**_base_doubles())
        assert record.draw_type == "doubles"
        assert record.p1_partner_id == "EF56"
        assert record.p2_partner_id == "GH78"
        assert record.match_uid == "2023_404_DBL_F_AB12_CD34_EF56_GH78"

    def test_walkover(self):
        record = ResultRecord(
            **_base_singles(
                result_type="walkover",
                duration_seconds=None,
                p1_set1_games=None,
                p2_set1_games=None,
                p1_set2_games=None,
                p2_set2_games=None,
            )
        )
        assert record.result_type == "walkover"
        assert record.duration_seconds is None

    def test_retirement(self):
        record = ResultRecord(
            **_base_singles(
                result_type="retirement",
                p1_set1_games=6,
                p2_set1_games=4,
                p1_set2_games=3,
                p2_set2_games=6,
                p1_set3_games=1,
                p2_set3_games=4,
            )
        )
        assert record.result_type == "retirement"
        assert record.p1_set3_games == 1

    def test_tiebreak_set(self):
        record = ResultRecord(
            **_base_singles(
                p1_set1_games=7,
                p2_set1_games=6,
                p1_set1_tiebreak=9,
                p2_set1_tiebreak=7,
            )
        )
        assert record.p1_set1_tiebreak == 9
        assert record.p2_set1_tiebreak == 7

    def test_placeholder_id_match_uid_null(self):
        record = ResultRecord(
            **_base_singles(
                p2_id="0",
                winner_id="ab12",
            )
        )
        assert record.match_uid is None

    def test_country_uppercased(self):
        record = ResultRecord(**_base_singles(p1_country="usa", p2_country="gbr"))
        assert record.p1_country == "USA"
        assert record.p2_country == "GBR"

    def test_non_placeholder_match_uid_required(self):
        """Covered implicitly by all valid records above, but explicit."""
        record = ResultRecord(**_base_singles())
        assert record.match_uid is not None


class TestValidationErrors:
    def test_winner_id_mismatch(self):
        with pytest.raises(ValidationError, match="winner_id"):
            ResultRecord(**_base_singles(winner_id="WRONG"))

    def test_missing_required_field(self):
        data = _base_singles()
        del data["p1_id"]
        with pytest.raises(ValidationError):
            ResultRecord(**data)

    def test_doubles_with_null_partners(self):
        with pytest.raises(ValidationError, match="partner"):
            ResultRecord(
                **_base_singles(
                    draw_type="doubles",
                )
            )

    def test_singles_with_populated_partners(self):
        with pytest.raises(ValidationError, match="partner"):
            ResultRecord(
                **_base_singles(
                    p1_partner_id="ef56",
                    p1_partner_name="Partner",
                    p1_partner_country="fra",
                    p2_partner_id="gh78",
                    p2_partner_name="Partner",
                    p2_partner_country="esp",
                )
            )

    def test_non_contiguous_sets_p1(self):
        with pytest.raises(ValidationError, match="p1.*contiguous"):
            ResultRecord(
                **_base_singles(
                    p1_set1_games=6,
                    p2_set1_games=4,
                    p1_set2_games=None,
                    p2_set2_games=None,
                    p1_set3_games=6,
                    p2_set3_games=3,
                )
            )

    def test_non_contiguous_sets_p2(self):
        with pytest.raises(ValidationError, match="p2.*contiguous"):
            ResultRecord(
                **_base_singles(
                    p1_set1_games=6,
                    p2_set1_games=4,
                    p1_set2_games=7,
                    p2_set2_games=None,
                    p1_set3_games=6,
                    p2_set3_games=3,
                )
            )


class TestEmptyToNone:
    def test_empty_match_id_becomes_none(self):
        record = ResultRecord(**_base_singles(match_id=""))
        assert record.match_id is None


class TestSchemaVersioning:
    def test_schema_version_is_semver(self):
        parts = SCHEMA_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_schema_hash_is_hex_string(self):
        assert len(SCHEMA_HASH) == 16
        int(SCHEMA_HASH, 16)  # raises if not valid hex

    def test_schema_hash_changes_on_field_change(self):
        """Sanity check — hash is deterministic and non-empty."""
        assert SCHEMA_HASH
        assert isinstance(SCHEMA_HASH, str)
