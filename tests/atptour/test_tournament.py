import pytest

from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit


class TestTournament:
    def test_basic_construction(self):
        t = Tournament(
            tournament_id="580",
            year=2023,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
        )
        assert t.tournament_id == "580"
        assert t.year == 2023
        assert t.circuit == Circuit.tour

    def test_frozen(self):
        t = Tournament(
            tournament_id="580",
            year=2023,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
        )
        with pytest.raises(AttributeError):
            t.year = 2024

    def test_path(self):
        t = Tournament(
            tournament_id="580",
            year=2023,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
        )
        assert t.path == "tournaments/tour/580/2023"

    def test_path_challenger(self):
        t = Tournament(
            tournament_id="1200",
            year=2022,
            circuit=Circuit.chal,
            location="Somewhere, USA",
        )
        assert t.path == "tournaments/chal/1200/2022"

    def test_logging_id(self):
        t = Tournament(
            tournament_id="580",
            year=2023,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
        )
        assert t.logging_id == "ATP Melbourne 2023 (580)"

    def test_logging_id_uses_city(self):
        t = Tournament(
            tournament_id="999",
            year=2023,
            circuit=Circuit.chal,
            location="Buenos Aires, Argentina",
        )
        assert "Buenos Aires" in t.logging_id


class TestFromOverviewData:
    def test_tour_tournament(self):
        data = {
            "EventType": "GS",
            "Location": "Melbourne, Australia",
            "Surface": "Hard",
            "InOutdoor": "O",
        }
        t = Tournament.from_overview_data(data, tournament_id="580", year=2023)
        assert t.circuit == Circuit.tour
        assert t.surface == "Hard"
        assert t.indoor == "O"

    def test_challenger(self):
        data = {
            "EventType": "CH",
            "Location": "Lima, Peru",
            "Surface": "Clay",
            "InOutdoor": "O",
        }
        t = Tournament.from_overview_data(data, tournament_id="1200", year=2022)
        assert t.circuit == Circuit.chal

    def test_unknown_event_type_raises(self):
        data = {"EventType": "UNKNOWN", "Location": "Nowhere"}
        with pytest.raises(ValueError, match="Unknown EventType"):
            Tournament.from_overview_data(data, tournament_id="999", year=2023)

    def test_empty_surface_becomes_none(self):
        data = {
            "EventType": "GS",
            "Location": "Melbourne, Australia",
            "Surface": "",
            "InOutdoor": "",
        }
        t = Tournament.from_overview_data(data, tournament_id="580", year=2023)
        assert t.surface is None
        assert t.indoor is None
