"""Tests for Match Stats staged schema."""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from mvp.atptour.schemas.match_stats import (
    SCHEMA_HASH,
    SCHEMA_VERSION,
    MatchStatsRecord,
)

PARSED_AT = datetime(2026, 2, 23)
SOURCE_FILE = "data/raw/atptour/tournaments/tour/404/2023/match_stats/ms001.json"


def _stat_fields(prefix: str, **overrides) -> dict:
    """Generate all 26 stat fields for a player side with sensible defaults."""
    defaults = {
        f"{prefix}_svc_aces": 5,
        f"{prefix}_svc_double_faults": 2,
        f"{prefix}_svc_first_serve_in": 40,
        f"{prefix}_svc_first_serve_att": 60,
        f"{prefix}_svc_first_serve_pts_won": 30,
        f"{prefix}_svc_first_serve_pts_played": 40,
        f"{prefix}_svc_second_serve_pts_won": 10,
        f"{prefix}_svc_second_serve_pts_played": 20,
        f"{prefix}_svc_bp_saved": 3,
        f"{prefix}_svc_bp_faced": 5,
        f"{prefix}_svc_games_played": 8,
        f"{prefix}_svc_serve_rating": 220,
        f"{prefix}_ret_first_serve_pts_won": 15,
        f"{prefix}_ret_first_serve_pts_played": 40,
        f"{prefix}_ret_second_serve_pts_won": 8,
        f"{prefix}_ret_second_serve_pts_played": 20,
        f"{prefix}_ret_bp_converted": 2,
        f"{prefix}_ret_bp_opportunities": 5,
        f"{prefix}_ret_games_played": 8,
        f"{prefix}_ret_return_rating": 180,
        f"{prefix}_pts_service_pts_won": 40,
        f"{prefix}_pts_service_pts_played": 60,
        f"{prefix}_pts_return_pts_won": 23,
        f"{prefix}_pts_return_pts_played": 60,
        f"{prefix}_pts_total_pts_won": 63,
        f"{prefix}_pts_total_pts_played": 120,
    }
    defaults.update(overrides)
    return defaults


def _base_singles(**overrides) -> dict:
    """Minimal valid completed singles match stats record."""
    data = {
        "tournament_id": "404",
        "year": 2023,
        "circuit": "tour",
        "draw_type": "singles",
        "round": "Final",
        "round_id": 1,
        "match_id": "MS001",
        "match_uid": "404__2023__singles__f__AB12|CD34",
        "surface": "Hard",
        "tournament_start_date": date(2023, 1, 16),
        "tournament_end_date": date(2023, 1, 22),
        "tournament_city": "Melbourne",
        "prize_money": 5000000,
        "currency": "USD",
        "draw_size_singles": 128,
        "draw_size_doubles": 64,
        "winner_id": "ab12",
        "duration_seconds": 5400,
        "reason": None,
        "number_of_sets": 3,
        "sets_played": 2,
        "is_qualifier": False,
        "scoring_system": "Best of 3",
        "court_name": "Center Court",
        "umpire_first_name": "Carlos",
        "umpire_last_name": "Bernardes",
        "p1_id": "ab12",
        "p2_id": "cd34",
        "p1_partner_id": None,
        "p2_partner_id": None,
        "p1_seed": "1",
        "p2_seed": "2",
        **_stat_fields("p1"),
        **_stat_fields("p2"),
        "source_file": SOURCE_FILE,
        "parsed_at": PARSED_AT,
    }
    data.update(overrides)
    return data


def _base_doubles(**overrides) -> dict:
    """Minimal valid completed doubles match stats record."""
    data = _base_singles(
        draw_type="doubles",
        match_uid="404__2023__doubles__f__AB12|CD34|EF56|GH78",
        p1_partner_id="ef56",
        p2_partner_id="gh78",
        match_id="MD001",
        source_file="data/raw/atptour/tournaments/tour/404/2023/match_stats/md001.json",
    )
    data.update(overrides)
    return data


class TestValidRecords:
    def test_completed_singles(self):
        record = MatchStatsRecord(**_base_singles())
        assert record.winner_id == "AB12"
        assert record.p1_id == "AB12"
        assert record.p2_id == "CD34"
        assert record.draw_type == "singles"
        assert record.duration_seconds == 5400
        assert record.p1_svc_aces == 5
        assert record.p2_ret_return_rating == 180

    def test_completed_doubles(self):
        record = MatchStatsRecord(**_base_doubles())
        assert record.draw_type == "doubles"
        assert record.p1_partner_id == "EF56"
        assert record.p2_partner_id == "GH78"

    def test_retirement_with_reason(self):
        record = MatchStatsRecord(**_base_singles(reason="RET"))
        assert record.reason == "RET"

    def test_null_winner_id(self):
        record = MatchStatsRecord(**_base_singles(winner_id=None))
        assert record.winner_id is None

    def test_null_optional_fields(self):
        record = MatchStatsRecord(
            **_base_singles(
                surface=None,
                tournament_start_date=None,
                tournament_end_date=None,
                tournament_city=None,
                prize_money=None,
                currency=None,
                draw_size_singles=None,
                draw_size_doubles=None,
                winner_id=None,
                duration_seconds=None,
                round_id=None,
                is_qualifier=None,
                scoring_system=None,
                court_name=None,
                umpire_first_name=None,
                umpire_last_name=None,
                p1_seed=None,
                p2_seed=None,
            )
        )
        assert record.surface is None
        assert record.winner_id is None
        assert record.duration_seconds is None
        assert record.court_name is None

    def test_empty_strings_converted_to_none(self):
        record = MatchStatsRecord(
            **_base_singles(
                winner_id=None,
                court_name="",
                scoring_system="",
                currency="",
                umpire_first_name="",
                umpire_last_name="",
            )
        )
        assert record.court_name is None
        assert record.scoring_system is None
        assert record.currency is None
        assert record.umpire_first_name is None
        assert record.umpire_last_name is None

    def test_placeholder_id_match_uid_null(self):
        record = MatchStatsRecord(
            **_base_singles(
                p2_id="0",
                winner_id="ab12",
                match_uid=None,
            )
        )
        assert record.match_uid is None


class TestValidationErrors:
    def test_winner_id_mismatch(self):
        with pytest.raises(ValidationError, match="winner_id"):
            MatchStatsRecord(**_base_singles(winner_id="WRONG"))

    def test_invalid_reason(self):
        with pytest.raises(ValidationError, match="reason"):
            MatchStatsRecord(**_base_singles(reason="INVALID"))

    def test_doubles_missing_partner_ids(self):
        with pytest.raises(ValidationError, match="partner"):
            MatchStatsRecord(
                **_base_singles(
                    draw_type="doubles",
                    match_uid="404__2023__doubles__f__AB12|CD34",
                )
            )

    def test_singles_with_partner_ids(self):
        with pytest.raises(ValidationError, match="partner"):
            MatchStatsRecord(
                **_base_singles(
                    p1_partner_id="ef56",
                    p2_partner_id="gh78",
                )
            )

    def test_unmapped_round(self):
        with pytest.raises(ValidationError, match="[Uu]nmapped round"):
            MatchStatsRecord(**_base_singles(round="Nonexistent Round"))

    def test_placeholder_with_non_null_uid(self):
        with pytest.raises(ValidationError, match="match_uid must be null"):
            MatchStatsRecord(
                **_base_singles(
                    p2_id="0",
                    winner_id="ab12",
                    match_uid="404__2023__singles__f__AB12|CD34",
                )
            )


class TestSchemaVersioning:
    def test_schema_version_is_semver(self):
        parts = SCHEMA_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_schema_hash_is_hex_string(self):
        assert len(SCHEMA_HASH) == 16
        int(SCHEMA_HASH, 16)  # raises if not valid hex
