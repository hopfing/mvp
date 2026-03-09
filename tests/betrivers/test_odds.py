"""Tests for BetRivers odds scraper."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from mvp.betrivers.odds import (
    BetRiversOddsEntry,
    BetRiversOddsScraper,
    _is_atp_challenger,
    _parse_response,
)


class TestCircuitFiltering:
    def test_atp_included(self):
        assert _is_atp_challenger("atp") is True

    def test_challenger_included(self):
        assert _is_atp_challenger("challenger") is True

    def test_challenger_qual_included(self):
        assert _is_atp_challenger("challenger_qual_") is True

    def test_wta_excluded(self):
        assert _is_atp_challenger("wta") is False

    def test_wta_doubles_excluded(self):
        assert _is_atp_challenger("wta_doubles") is False

    def test_atp_doubles_excluded(self):
        assert _is_atp_challenger("atp_doubles") is False

    def test_itf_women_excluded(self):
        assert _is_atp_challenger("itf_women") is False

    def test_itf_women_qual_excluded(self):
        assert _is_atp_challenger("itf_women_qual_") is False

    def test_itf_men_qual_excluded(self):
        assert _is_atp_challenger("itf_men_qual_") is False

    def test_utr_excluded(self):
        assert _is_atp_challenger("utr_pro_tennis_series") is False

    def test_wta125_excluded(self):
        assert _is_atp_challenger("wta125") is False

    def test_unknown_excluded(self):
        assert _is_atp_challenger("some_new_category") is False


SAMPLE_RESPONSE = {
    "events": [
        {
            "event": {
                "id": 1026892922,
                "name": "Jannik Sinner - Carlos Alcaraz",
                "homeName": "Jannik Sinner",
                "awayName": "Carlos Alcaraz",
                "start": "2026-03-10T18:00:00Z",
                "group": "Indian Wells",
                "groupId": 2000070100,
                "path": [
                    {"id": 1, "termKey": "tennis"},
                    {"id": 2, "termKey": "atp"},
                    {"id": 3, "termKey": "indian_wells"},
                ],
                "sport": "TENNIS",
                "state": "NOT_STARTED",
            },
            "betOffers": [
                {
                    "id": 2619505297,
                    "criterion": {"id": 1001159551, "label": "Moneyline"},
                    "eventId": 1026892922,
                    "outcomes": [
                        {
                            "id": 4096271757,
                            "label": "Jannik Sinner",
                            "participant": "Jannik Sinner",
                            "odds": 1456,
                            "type": "OT_ONE",
                            "status": "OPEN",
                        },
                        {
                            "id": 4096271762,
                            "label": "Carlos Alcaraz",
                            "participant": "Carlos Alcaraz",
                            "odds": 2812,
                            "type": "OT_TWO",
                            "status": "OPEN",
                        },
                    ],
                },
            ],
        },
        {
            "event": {
                "id": 1026892923,
                "name": "Player A - Player B",
                "homeName": "Player A",
                "awayName": "Player B",
                "start": "2026-03-10T19:00:00Z",
                "group": "Murcia",
                "groupId": 2000070200,
                "path": [
                    {"id": 1, "termKey": "tennis"},
                    {"id": 2, "termKey": "challenger"},
                    {"id": 3, "termKey": "murcia"},
                ],
                "sport": "TENNIS",
                "state": "NOT_STARTED",
            },
            "betOffers": [
                {
                    "id": 2619505298,
                    "criterion": {"id": 1001159551, "label": "Moneyline"},
                    "eventId": 1026892923,
                    "outcomes": [
                        {
                            "id": 4096271770,
                            "label": "Player A",
                            "participant": "Player A",
                            "odds": 1800,
                            "type": "OT_ONE",
                            "status": "OPEN",
                        },
                        {
                            "id": 4096271771,
                            "label": "Player B",
                            "participant": "Player B",
                            "odds": 1950,
                            "type": "OT_TWO",
                            "status": "OPEN",
                        },
                    ],
                },
            ],
        },
        {
            "event": {
                "id": 1026892924,
                "name": "WTA Player - WTA Player 2",
                "homeName": "WTA Player",
                "awayName": "WTA Player 2",
                "start": "2026-03-10T20:00:00Z",
                "group": "Miami",
                "groupId": 2000070300,
                "path": [
                    {"id": 1, "termKey": "tennis"},
                    {"id": 2, "termKey": "wta"},
                    {"id": 3, "termKey": "miami"},
                ],
                "sport": "TENNIS",
                "state": "NOT_STARTED",
            },
            "betOffers": [
                {
                    "id": 2619505299,
                    "criterion": {"id": 1001159551, "label": "Moneyline"},
                    "eventId": 1026892924,
                    "outcomes": [
                        {
                            "id": 4096271780,
                            "label": "WTA Player",
                            "participant": "WTA Player",
                            "odds": 1500,
                            "type": "OT_ONE",
                            "status": "OPEN",
                        },
                        {
                            "id": 4096271781,
                            "label": "WTA Player 2",
                            "participant": "WTA Player 2",
                            "odds": 2500,
                            "type": "OT_TWO",
                            "status": "OPEN",
                        },
                    ],
                },
            ],
        },
    ],
}


class TestParseResponse:
    def test_filters_to_atp_challenger(self):
        now = datetime.now(timezone.utc)
        entries = _parse_response(SAMPLE_RESPONSE, now)
        assert len(entries) == 4  # 2 per match, 2 matches

    def test_entry_fields(self):
        now = datetime.now(timezone.utc)
        entries = _parse_response(SAMPLE_RESPONSE, now)
        sinner = next(e for e in entries if "Sinner" in e.player_name)
        assert sinner.book == "br"
        assert sinner.br_event_id == "1026892922"
        assert sinner.market == "moneyline"
        assert sinner.odds == 1.456
        assert sinner.side == "OT_ONE"
        assert sinner.opponent_name == "Carlos Alcaraz"
        assert sinner.tournament == "Indian Wells"
        assert sinner.circuit == "atp"
        assert sinner.points is None

    def test_odds_divided_by_1000(self):
        now = datetime.now(timezone.utc)
        entries = _parse_response(SAMPLE_RESPONSE, now)
        alcaraz = next(e for e in entries if "Alcaraz" in e.player_name)
        assert alcaraz.odds == 2.812

    def test_two_entries_per_match(self):
        now = datetime.now(timezone.utc)
        entries = _parse_response(SAMPLE_RESPONSE, now)
        event_ids = {e.br_event_id for e in entries}
        for eid in event_ids:
            match_entries = [e for e in entries if e.br_event_id == eid]
            assert len(match_entries) == 2

    def test_opponent_name_symmetric(self):
        now = datetime.now(timezone.utc)
        entries = _parse_response(SAMPLE_RESPONSE, now)
        atp_entries = [e for e in entries if e.br_event_id == "1026892922"]
        names = {e.player_name for e in atp_entries}
        opponents = {e.opponent_name for e in atp_entries}
        assert names == opponents

    def test_empty_response(self):
        now = datetime.now(timezone.utc)
        entries = _parse_response({"events": []}, now)
        assert entries == []

    def test_event_without_moneyline_skipped(self):
        now = datetime.now(timezone.utc)
        response = {
            "events": [
                {
                    "event": {
                        "id": 999,
                        "homeName": "A",
                        "awayName": "B",
                        "group": "Test",
                        "groupId": 1,
                        "path": [
                            {"termKey": "tennis"},
                            {"termKey": "atp"},
                            {"termKey": "test"},
                        ],
                        "state": "NOT_STARTED",
                    },
                    "betOffers": [
                        {
                            "criterion": {"id": 999999, "label": "Total Games"},
                            "eventId": 999,
                            "outcomes": [],
                        },
                    ],
                },
            ],
        }
        entries = _parse_response(response, now)
        assert entries == []

    def test_event_without_betoffers_skipped(self):
        now = datetime.now(timezone.utc)
        response = {
            "events": [
                {
                    "event": {
                        "id": 888,
                        "homeName": "A",
                        "awayName": "B",
                        "group": "Test",
                        "groupId": 1,
                        "path": [
                            {"termKey": "tennis"},
                            {"termKey": "atp"},
                            {"termKey": "test"},
                        ],
                        "state": "NOT_STARTED",
                    },
                    "betOffers": [],
                },
            ],
        }
        entries = _parse_response(response, now)
        assert entries == []


class TestBetRiversOddsScraper:
    @patch("mvp.betrivers.odds.BaseExtractor._create_session")
    def test_fetch_all_odds(self, mock_create_session):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_RESPONSE
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = BetRiversOddsScraper()
        entries, raw = scraper.fetch_all_odds()

        assert len(entries) == 4  # 2 ATP + 2 Challenger, WTA filtered
        assert raw == SAMPLE_RESPONSE

    @patch("mvp.betrivers.odds.BaseExtractor._create_session")
    def test_fetch_and_save(self, mock_create_session, tmp_path):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_RESPONSE
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = BetRiversOddsScraper(data_root=tmp_path)
        count = scraper.fetch_and_save()

        assert count == 4
        # Check stage parquet was created
        stage_path = tmp_path / "stage" / "betrivers" / "moneyline.parquet"
        assert stage_path.exists()
        df = pl.read_parquet(stage_path)
        assert len(df) == 4
        assert "odds" in df.columns
        assert "br_event_id" in df.columns

    @patch("mvp.betrivers.odds.BaseExtractor._create_session")
    def test_fetch_and_save_appends(self, mock_create_session, tmp_path):
        """Second fetch appends to existing parquet."""
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_RESPONSE
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = BetRiversOddsScraper(data_root=tmp_path)
        scraper.fetch_and_save()
        scraper.fetch_and_save()

        stage_path = tmp_path / "stage" / "betrivers" / "moneyline.parquet"
        df = pl.read_parquet(stage_path)
        assert len(df) == 8  # 4 + 4

    @patch("mvp.betrivers.odds.BaseExtractor._create_session")
    def test_empty_response_returns_zero(self, mock_create_session, tmp_path):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"events": []}
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = BetRiversOddsScraper(data_root=tmp_path)
        count = scraper.fetch_and_save()
        assert count == 0
