"""Tests for TournamentDiscovery."""

from pathlib import Path
from unittest.mock import patch

import pytest

from mvp.atptour.discovery import TournamentDiscovery
from mvp.common.enums import Circuit

DATA_ROOT = Path("C:/Users/hopfi/projects/mvp/data")


@pytest.fixture
def discovery():
    return TournamentDiscovery(data_root=DATA_ROOT)


class TestGetArchiveTournaments:
    def test_tour_2023(self, discovery):
        results = discovery.get_archive_tournaments(2023, circuit=Circuit.tour)
        assert len(results) > 0
        assert all(c == Circuit.tour for _, _, c in results)
        assert all(y == 2023 for _, y, _ in results)
        # Australian Open (580) should be present
        tids = {tid for tid, _, _ in results}
        assert "580" in tids

    def test_challenger_2023(self, discovery):
        results = discovery.get_archive_tournaments(2023, circuit=Circuit.chal)
        assert len(results) > 0
        assert all(c == Circuit.chal for _, _, c in results)

    def test_both_circuits(self, discovery):
        results = discovery.get_archive_tournaments(2023)
        circuits = {c for _, _, c in results}
        assert Circuit.tour in circuits
        assert Circuit.chal in circuits

    def test_deduplicates(self, discovery):
        results = discovery.get_archive_tournaments(2023)
        assert len(results) == len(set(results))

    def test_returns_str_tids(self, discovery):
        results = discovery.get_archive_tournaments(2023, circuit=Circuit.tour)
        assert all(isinstance(tid, str) for tid, _, _ in results)

    def test_invalid_circuit_raises(self, discovery):
        with pytest.raises(ValueError, match="No archive filter"):
            discovery.get_archive_tournaments(2023, circuit=Circuit.team)

    def test_fetches_missing_html(self, tmp_path):
        """When archive HTML doesn't exist, fetch and save it."""
        disc = TournamentDiscovery(data_root=tmp_path)
        fake_html = """
        <html><body>
        <a class="tournament__profile" href="/en/tournaments/test/999/overview"></a>
        </body></html>
        """
        with patch.object(disc, "fetch_html", return_value=fake_html) as mock_fetch:
            results = disc.get_archive_tournaments(2023, circuit=Circuit.tour)

        mock_fetch.assert_called_once()
        assert ("999", 2023, Circuit.tour) in results
        # HTML should have been saved to disk
        saved_path = (
            tmp_path / "raw" / "atptour" / "results_archive" / "atpgs" / "2023.html"
        )
        assert saved_path.exists()


class TestGetActiveTournaments:
    def test_returns_str_tids(self, tmp_path):
        discovery = TournamentDiscovery(data_root=tmp_path)
        mock_data = {
            "Data": {
                "LiveMatchesTournamentsOrdered": [
                    {"EventId": 580, "EventYear": 2026},
                    {"EventId": 339, "EventYear": 2026},
                ]
            }
        }
        with patch.object(discovery, "fetch_json", return_value=mock_data):
            results = discovery.get_active_tournaments()
        assert len(results) >= 2
        assert all(isinstance(tid, str) for tid, _ in results)
        assert ("580", 2026) in results

    def test_fetches_both_circuits(self, tmp_path):
        discovery = TournamentDiscovery(data_root=tmp_path)
        mock_data = {
            "Data": {"LiveMatchesTournamentsOrdered": []}
        }
        with patch.object(
            discovery, "fetch_json", return_value=mock_data
        ) as mock_fetch:
            discovery.get_active_tournaments()
        assert mock_fetch.call_count == 2

    def test_type_error_on_non_int_event_id(self, tmp_path):
        discovery = TournamentDiscovery(data_root=tmp_path)
        mock_data = {
            "Data": {
                "LiveMatchesTournamentsOrdered": [
                    {"EventId": "not_an_int", "EventYear": 2026},
                ]
            }
        }
        with patch.object(discovery, "fetch_json", return_value=mock_data):
            with pytest.raises(TypeError):
                discovery.get_active_tournaments()

    def test_type_error_on_non_int_event_year(self, tmp_path):
        discovery = TournamentDiscovery(data_root=tmp_path)
        mock_data = {
            "Data": {
                "LiveMatchesTournamentsOrdered": [
                    {"EventId": 580, "EventYear": "not_an_int"},
                ]
            }
        }
        with patch.object(discovery, "fetch_json", return_value=mock_data):
            with pytest.raises(TypeError):
                discovery.get_active_tournaments()
