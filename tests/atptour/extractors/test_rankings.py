"""Tests for RankingsExtractor — discovery, filtering, and fetch logic."""

from datetime import date
from unittest.mock import patch

import pytest

from mvp.atptour.extractors.rankings import RankingsExtractor

DISCOVERY_HTML = """\
<div class="atp_filters-dropdown" data-key="DateWeek">
    <select>
        <option value="Current Week">Current Week</option>
        <option value="2026-02-17">2026.02.17</option>
        <option value="2026-02-10">2026.02.10</option>
        <option value="2025-01-06">2025.01.06</option>
    </select>
</div>"""


@pytest.fixture
def extractor(tmp_path):
    return RankingsExtractor(start_year=2025, data_root=tmp_path)


class TestGetAvailableDates:
    def test_parses_dates_sorted(self, extractor):
        dates = extractor._get_available_dates(DISCOVERY_HTML)
        assert dates == [date(2025, 1, 6), date(2026, 2, 10), date(2026, 2, 17)]

    def test_skips_current_week(self, extractor):
        dates = extractor._get_available_dates(DISCOVERY_HTML)
        assert all(isinstance(d, date) for d in dates)
        assert len(dates) == 3

    def test_raises_on_missing_dropdown(self, extractor):
        html = "<html><body>no dropdown</body></html>"
        with pytest.raises(ValueError, match="Could not find DateWeek dropdown"):
            extractor._get_available_dates(html)


class TestGetExistingDates:
    def test_finds_existing_files(self, extractor, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "rankings"
        raw_dir.mkdir(parents=True)
        (raw_dir / "rankings_singles_20260217.html").write_text("<html></html>")
        (raw_dir / "rankings_singles_20260210.html").write_text("<html></html>")
        dates = extractor._get_existing_dates()
        assert dates == {date(2026, 2, 17), date(2026, 2, 10)}

    def test_empty_dir(self, extractor, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "rankings"
        raw_dir.mkdir(parents=True)
        dates = extractor._get_existing_dates()
        assert dates == set()

    def test_no_dir(self, extractor):
        dates = extractor._get_existing_dates()
        assert dates == set()


class TestRun:
    def test_skips_existing_dates(self, extractor, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "rankings"
        raw_dir.mkdir(parents=True)
        (raw_dir / "rankings_singles_20260217.html").write_text("<html></html>")

        discovery_html = """\
<div class="atp_filters-dropdown" data-key="DateWeek">
    <select>
        <option value="Current Week">Current Week</option>
        <option value="2026-02-17">2026.02.17</option>
        <option value="2026-02-10">2026.02.10</option>
    </select>
</div>"""

        with patch.object(
            extractor, "fetch_html", side_effect=[discovery_html, "<html>new</html>"]
        ) as mock:
            extractor.run()
        # Discovery page + 1 missing date (not 2026-02-17 which already exists)
        assert mock.call_count == 2

    def test_filters_by_start_year(self, extractor, tmp_path):
        discovery_html = """\
<div class="atp_filters-dropdown" data-key="DateWeek">
    <select>
        <option value="Current Week">Current Week</option>
        <option value="2026-02-17">2026.02.17</option>
        <option value="2024-01-06">2024.01.06</option>
    </select>
</div>"""

        with patch.object(
            extractor, "fetch_html", side_effect=[discovery_html, "<html></html>"]
        ) as mock:
            extractor.run()
        # Only 2026-02-17 is in range (start_year=2025), 2024-01-06 is excluded
        assert mock.call_count == 2

    def test_saves_html_with_correct_filename(self, extractor, tmp_path):
        discovery_html = """\
<div class="atp_filters-dropdown" data-key="DateWeek">
    <select>
        <option value="Current Week">Current Week</option>
        <option value="2026-02-17">2026.02.17</option>
    </select>
</div>"""

        with patch.object(
            extractor,
            "fetch_html",
            side_effect=[discovery_html, "<html>rankings page</html>"],
        ):
            extractor.run()

        rankings_dir = tmp_path / "raw" / "atptour" / "rankings"
        saved = rankings_dir / "rankings_singles_20260217.html"
        assert saved.exists()
        assert saved.read_text() == "<html>rankings page</html>"

    def test_nothing_to_fetch(self, extractor, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "rankings"
        raw_dir.mkdir(parents=True)
        (raw_dir / "rankings_singles_20260217.html").write_text("<html></html>")

        discovery_html = """\
<div class="atp_filters-dropdown" data-key="DateWeek">
    <select>
        <option value="Current Week">Current Week</option>
        <option value="2026-02-17">2026.02.17</option>
    </select>
</div>"""

        with patch.object(
            extractor, "fetch_html", side_effect=[discovery_html]
        ) as mock:
            extractor.run()
        # Only the discovery page fetched
        assert mock.call_count == 1

    def test_default_start_year(self, tmp_path):
        ext = RankingsExtractor(data_root=tmp_path)
        assert ext.start_year == 2025
