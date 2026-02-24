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
        assert t.logging_id == "ATP Australian Open 2023 (580)"

    def test_logging_id_uses_city(self):
        t = Tournament(
            tournament_id="999",
            year=2023,
            circuit=Circuit.chal,
            location="Buenos Aires, Argentina",
        )
        assert "Buenos Aires" in t.logging_id


class TestTournamentName:
    def test_city_fallback(self):
        t = Tournament(
            tournament_id="339",
            year=2026,
            circuit=Circuit.tour,
            location="Brisbane, Australia",
        )
        assert t.name == "Brisbane"

    def test_grand_slam_lookup(self):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
        )
        assert t.name == "Australian Open"

    def test_davis_cup(self):
        t = Tournament(
            tournament_id="8099",
            year=2026,
            circuit=Circuit.tour,
            location="Multiple Locations",
        )
        assert t.name == "Davis Cup Finals"

    def test_multiple_locations_without_lookup_raises(self):
        t = Tournament(
            tournament_id="9999",
            year=2026,
            circuit=Circuit.tour,
            location="Multiple Locations",
        )
        with pytest.raises(ValueError, match="Unable to determine"):
            _ = t.name

    def test_logging_id_uses_name(self):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
        )
        assert "Australian Open" in t.logging_id


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


class TestUrlSlug:
    def test_simple_city(self):
        t = Tournament(
            tournament_id="339",
            year=2026,
            circuit=Circuit.tour,
            location="Brisbane, Australia",
        )
        assert t.url_slug == "brisbane"

    def test_multi_word_name(self):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
        )
        assert t.url_slug == "australian-open"

    def test_strips_accents(self):
        t = Tournament(
            tournament_id="999",
            year=2026,
            circuit=Circuit.tour,
            location="Zürich, Switzerland",
        )
        assert t.url_slug == "zurich"

    def test_strips_apostrophes(self):
        t = Tournament(
            tournament_id="999",
            year=2026,
            circuit=Circuit.tour,
            location="Queen's Club, UK",
        )
        assert t.url_slug == "queens-club"


class TestScoresUrlPrefix:
    def test_archive(self):
        t = Tournament(
            tournament_id="580",
            year=2023,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
            is_archive=True,
        )
        assert t.scores_url_prefix == "archive"

    def test_active_tour(self):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
            is_archive=False,
        )
        assert t.scores_url_prefix == "current"

    def test_active_challenger(self):
        t = Tournament(
            tournament_id="1796",
            year=2026,
            circuit=Circuit.chal,
            location="St. Brieuc, France",
            is_archive=False,
        )
        assert t.scores_url_prefix == "current-challenger"

    def test_archive_challenger(self):
        t = Tournament(
            tournament_id="1796",
            year=2023,
            circuit=Circuit.chal,
            location="St. Brieuc, France",
            is_archive=True,
        )
        assert t.scores_url_prefix == "archive"
