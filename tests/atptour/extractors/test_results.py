"""Tests for ResultsExtractor."""

from unittest.mock import patch

import pytest

from mvp.atptour.extractors.results import ResultsExtractor
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit


@pytest.fixture
def tournament():
    return Tournament(
        tournament_id="580",
        year=2023,
        circuit=Circuit.tour,
        location="Melbourne, Australia",
        is_archive=True,
    )


@pytest.fixture
def extractor(tmp_path):
    return ResultsExtractor(data_root=tmp_path)


class TestResultsExtractor:
    def test_skips_existing_when_no_refresh(self, extractor, tournament, tmp_path):
        # Create existing HTML files
        path = tmp_path / "raw" / "atptour" / tournament.path / "results_singles.html"
        path.parent.mkdir(parents=True)
        path.write_text("<html></html>", encoding="utf-8")
        dbl_path = (
            tmp_path / "raw" / "atptour" / tournament.path / "results_doubles.html"
        )
        dbl_path.write_text("<html></html>", encoding="utf-8")

        with patch.object(extractor, "fetch_html") as mock_fetch:
            extractor.run(tournament, refresh=False)
        mock_fetch.assert_not_called()

    def test_fetches_when_refresh(self, extractor, tournament):
        with patch.object(extractor, "fetch_html", return_value="<html></html>"):
            extractor.run(tournament, refresh=True)

    def test_url_archive(self, extractor, tournament):
        url = extractor._results_url(tournament)
        assert "/archive/" in url
        assert "/2023/results" in url

    def test_url_active(self, extractor):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
            is_archive=False,
        )
        url = extractor._results_url(t)
        assert "/current/" in url
        assert "/results" in url
        assert "/2026/" not in url
