"""Tests for MatchStatsExtractor."""

import json
from unittest.mock import patch

import polars as pl
import pytest
import requests

from mvp.atptour.extractors.match_stats import MatchStatsExtractor
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit


@pytest.fixture
def tournament():
    return Tournament(
        tournament_id="580",
        year=2023,
        circuit=Circuit.tour,
        location="Melbourne, Australia",
    )


@pytest.fixture
def extractor(tmp_path):
    return MatchStatsExtractor(data_root=tmp_path)


class TestMatchStatsExtractor:
    def test_skips_existing_json(self, extractor, tournament, tmp_path):
        # Create staged results parquet with one match_id
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001"]})
        df.write_parquet(stage_dir / "results.parquet")

        # Create existing JSON for that match
        stats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_stats"
        stats_dir.mkdir(parents=True)
        (stats_dir / "ms001.json").write_text("{}")

        with patch.object(extractor, "fetch_json") as mock:
            extractor.run(tournament, refresh=False)
        mock.assert_not_called()

    def test_fetches_missing_json(self, extractor, tournament, tmp_path):
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001"]})
        df.write_parquet(stage_dir / "results.parquet")

        with patch.object(extractor, "fetch_json", return_value={"Match": {}}) as mock:
            extractor.run(tournament, refresh=False)
        mock.assert_called_once()

        # Verify JSON was saved
        saved = tmp_path / "raw" / "atptour" / tournament.path / "match_stats" / "ms001.json"
        assert saved.exists()
        data = json.loads(saved.read_text())
        assert data == {"Match": {}}

    def test_refresh_refetches_existing(self, extractor, tournament, tmp_path):
        # Create staged results parquet
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001"]})
        df.write_parquet(stage_dir / "results.parquet")

        # Create existing JSON
        stats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_stats"
        stats_dir.mkdir(parents=True)
        (stats_dir / "ms001.json").write_text("{}")

        with patch.object(extractor, "fetch_json", return_value={"Match": {}}) as mock:
            extractor.run(tournament, refresh=True)
        mock.assert_called_once()

    def test_session_headers(self, extractor):
        assert extractor.session.headers["Referer"] == "https://www.atptour.com/"
        assert extractor.session.headers["Origin"] == "https://www.atptour.com"

    def test_no_results_parquet(self, extractor, tournament):
        match_ids = extractor._get_match_ids(tournament)
        assert match_ids == []

    def test_null_response_skipped(self, extractor, tournament, tmp_path):
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001"]})
        df.write_parquet(stage_dir / "results.parquet")

        with patch.object(extractor, "fetch_json", return_value=None):
            extractor.run(tournament, refresh=False)

        # Null response should not be saved
        saved = tmp_path / "raw" / "atptour" / tournament.path / "match_stats" / "ms001.json"
        assert not saved.exists()

    def test_request_failure_continues(self, extractor, tournament, tmp_path):
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001", "ms002"]})
        df.write_parquet(stage_dir / "results.parquet")

        def side_effect(url):
            if "ms001" in url:
                raise requests.RequestException("connection error")
            return {"Match": {}}

        with patch.object(extractor, "fetch_json", side_effect=side_effect):
            extractor.run(tournament, refresh=False)

        # ms001 failed, ms002 should still be saved
        assert not (
            tmp_path / "raw" / "atptour" / tournament.path / "match_stats" / "ms001.json"
        ).exists()
        assert (
            tmp_path / "raw" / "atptour" / tournament.path / "match_stats" / "ms002.json"
        ).exists()

    def test_value_error_continues(self, extractor, tournament, tmp_path):
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001", "ms002"]})
        df.write_parquet(stage_dir / "results.parquet")

        def side_effect(url):
            if "ms001" in url:
                raise ValueError("non-JSON response")
            return {"Match": {}}

        with patch.object(extractor, "fetch_json", side_effect=side_effect):
            extractor.run(tournament, refresh=False)

        assert not (
            tmp_path / "raw" / "atptour" / tournament.path / "match_stats" / "ms001.json"
        ).exists()
        assert (
            tmp_path / "raw" / "atptour" / tournament.path / "match_stats" / "ms002.json"
        ).exists()

    def test_url_format(self, extractor, tournament):
        """Verify the Hawkeye URL structure."""
        # Indirectly test by checking fetch_json is called with correct URL
        stage_dir = extractor.build_path("stage", tournament.path)
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001"]})
        df.write_parquet(stage_dir / "results.parquet")

        with patch.object(extractor, "fetch_json", return_value=None) as mock:
            extractor.run(tournament, refresh=False)

        expected_url = (
            "https://www.atptour.com/-/Hawkeye/MatchStats/Complete"
            "/2023/580/ms001"
        )
        mock.assert_called_once_with(expected_url)

    def test_get_match_ids_deduplicates(self, extractor, tournament, tmp_path):
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001", "ms001", "ms002"]})
        df.write_parquet(stage_dir / "results.parquet")

        match_ids = extractor._get_match_ids(tournament)
        assert match_ids == ["ms001", "ms002"]

    def test_get_match_ids_filters_null(self, extractor, tournament, tmp_path):
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["ms001", None]})
        df.write_parquet(stage_dir / "results.parquet")

        match_ids = extractor._get_match_ids(tournament)
        assert match_ids == ["ms001"]
