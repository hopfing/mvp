"""Tests for Overview staged schema."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from mvp.atptour.schemas.overview import (
    SCHEMA_HASH,
    OverviewRecord,
)

PARSED_AT = datetime(2026, 2, 24)
SOURCE_FILE = "data/raw/atptour/tournaments/tour/404/2025/overview.json"


def _base_overview(**overrides) -> dict:
    """Minimal valid overview record."""
    data = {
        "tournament_id": "404",
        "year": 2025,
        "tournament_name": "Barcelona",
        "city": "Barcelona",
        "country": "Spain",
        "circuit": "tour",
        "sponsor_title": "Barcelona Open Banc Sabadell",
        "event_type": "500",
        "event_type_detail": 0,
        "singles_draw_size": 48,
        "doubles_draw_size": 16,
        "surface": "Clay",
        "surface_detail": "Red Clay",
        "indoor": "O",
        "prize": "€2,722,480",
        "total_financial_commitment": "€2,997,200",
        "location": "Barcelona, Spain",
        "source_file": SOURCE_FILE,
        "parsed_at": PARSED_AT,
    }
    data.update(overrides)
    return data


class TestValidRecords:
    def test_tour_event(self):
        record = OverviewRecord(**_base_overview())
        assert record.tournament_name == "Barcelona"
        assert record.circuit == "tour"
        assert record.surface == "Clay"
        assert record.indoor is False

    def test_challenger_event(self):
        record = OverviewRecord(**_base_overview(
            circuit="chal",
            event_type="CH",
            tournament_id="1234",
        ))
        assert record.circuit == "chal"
        assert record.event_type == "CH"

    def test_null_surface(self):
        record = OverviewRecord(**_base_overview(surface=None))
        assert record.surface is None

    def test_null_country(self):
        record = OverviewRecord(**_base_overview(country=None))
        assert record.country is None

    def test_null_sponsor_title(self):
        record = OverviewRecord(**_base_overview(sponsor_title=None))
        assert record.sponsor_title is None


class TestFieldValidation:
    def test_empty_surface_becomes_none(self):
        record = OverviewRecord(**_base_overview(surface=""))
        assert record.surface is None

    def test_empty_surface_detail_becomes_none(self):
        record = OverviewRecord(**_base_overview(surface_detail=""))
        assert record.surface_detail is None

    def test_empty_sponsor_title_becomes_none(self):
        record = OverviewRecord(**_base_overview(sponsor_title=""))
        assert record.sponsor_title is None

    def test_indoor_true(self):
        record = OverviewRecord(**_base_overview(indoor="I"))
        assert record.indoor is True

    def test_indoor_false(self):
        record = OverviewRecord(**_base_overview(indoor="O"))
        assert record.indoor is False

    def test_indoor_none_from_empty(self):
        record = OverviewRecord(**_base_overview(indoor=""))
        assert record.indoor is None

    def test_indoor_none_from_none(self):
        record = OverviewRecord(**_base_overview(indoor=None))
        assert record.indoor is None

    def test_invalid_indoor_raises(self):
        with pytest.raises(ValidationError, match="InOutdoor"):
            OverviewRecord(**_base_overview(indoor="X"))

    def test_invalid_event_type_raises(self):
        with pytest.raises(ValidationError):
            OverviewRecord(**_base_overview(event_type="BOGUS"))


class TestEdgeCases:
    def test_location_with_trailing_comma(self):
        record = OverviewRecord(**_base_overview(
            location="Fes, ",
            city="Fes",
            country=None,
        ))
        assert record.location == "Fes, "
        assert record.country is None

    def test_single_part_location(self):
        record = OverviewRecord(**_base_overview(
            location="London",
            city="London",
            country=None,
        ))
        assert record.city == "London"
        assert record.country is None


class TestSchemaHash:
    def test_schema_hash_is_hex_string(self):
        assert len(SCHEMA_HASH) == 16
        int(SCHEMA_HASH, 16)
