"""Tests for ScheduleExtractor."""

from unittest.mock import patch

import pytest

from mvp.atptour.extractors.schedule import ScheduleExtractor
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit


@pytest.fixture
def extractor(tmp_path):
    return ScheduleExtractor(data_root=tmp_path)


class TestScheduleExtractor:
    def test_skips_archive(self, extractor):
        t = Tournament(
            tournament_id="580",
            year=2023,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
            is_archive=True,
        )
        result = extractor.run(t)
        assert result is None

    def test_fetches_active(self, extractor, tmp_path):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
            is_archive=False,
        )
        with patch.object(extractor, "fetch_html", return_value="<html>schedule</html>"):
            result = extractor.run(t)
        assert result is not None
        assert result.exists()
        assert "schedule" in str(result)

    def test_saves_with_datetime_stamp(self, extractor, tmp_path):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
            is_archive=False,
        )
        with patch.object(extractor, "fetch_html", return_value="<html></html>"):
            result = extractor.run(t)
        # Filename should have datetime stamp: schedule_YYYYMMDD_HHMMSS.html
        assert "schedule_" in result.stem
        assert result.suffix == ".html"

    def test_url_construction(self, extractor):
        t = Tournament(
            tournament_id="580",
            year=2026,
            circuit=Circuit.tour,
            location="Melbourne, Australia",
            is_archive=False,
        )
        # The URL should use scores_url_prefix and url_slug
        # We can verify by checking fetch_html was called with the right URL
        with patch.object(extractor, "fetch_html", return_value="<html></html>") as mock:
            extractor.run(t)
        url = mock.call_args[0][0]
        assert "/current/" in url
        assert "/australian-open/" in url
        assert "/daily-schedule" in url
