"""Tests for DraftKings odds scraper."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from mvp.draftkings.odds import (
    DraftKingsOddsScraper,
    OddsEntry,
    _is_atp_challenger,
    _parse_odds_response,
    fetch_and_save,
)

SAMPLE_INITIAL_STATE = {
    "sports": {
        "data": [
            {
                "nameIdentifier": "tennis",
                "eventGroupInfos": [
                    {"eventGroupId": 100, "eventGroupName": "ATP Miami Open", "nameIdentifier": "atp-miami-open"},
                    {"eventGroupId": 101, "eventGroupName": "Challenger Murcia", "nameIdentifier": "challenger-murcia"},
                    {"eventGroupId": 102, "eventGroupName": "Australian Open Men", "nameIdentifier": "australian-open-men"},
                    {"eventGroupId": 200, "eventGroupName": "WTA Miami Open", "nameIdentifier": "wta-miami-open"},
                    {"eventGroupId": 201, "eventGroupName": "ITF M25 Cairo", "nameIdentifier": "itf-m25-cairo"},
                    {"eventGroupId": 202, "eventGroupName": "UTR Pro Match", "nameIdentifier": "utr-pro-match"},
                    {"eventGroupId": 203, "eventGroupName": "ATP Miami Doubles", "nameIdentifier": "atp-miami-open-doubles"},
                    {"eventGroupId": 204, "eventGroupName": "WTA Women", "nameIdentifier": "atp-rome-women"},
                ],
            },
            {
                "nameIdentifier": "basketball",
                "eventGroupInfos": [
                    {"eventGroupId": 300, "eventGroupName": "NBA", "nameIdentifier": "nba"},
                ],
            },
        ]
    }
}

SAMPLE_ODDS_RESPONSE = {
    "leagues": [
        {"id": "100", "name": "ATP Miami Open"},
    ],
    "events": [
        {"id": "1001", "leagueId": "100"},
        {"id": "1002", "leagueId": "100"},
    ],
    "markets": [
        {"id": "mkt_1001", "eventId": "1001", "leagueId": "100", "subcategoryId": 6364},
        {"id": "mkt_1002", "eventId": "1002", "leagueId": "100", "subcategoryId": 6364},
    ],
    "selections": [
        {
            "id": "0ML1001_1",
            "marketId": "mkt_1001",
            "label": "Sinner, J.",
            "trueOdds": 1.456,
            "displayOdds": {"decimal": "1.46"},
            "outcomeType": "Home",
            "participants": [{"countryCode": "ITA"}],
        },
        {
            "id": "0ML1001_2",
            "marketId": "mkt_1001",
            "label": "Alcaraz, C.",
            "trueOdds": 2.812,
            "displayOdds": {"decimal": "2.81"},
            "outcomeType": "Away",
            "participants": [{"countryCode": "ESP"}],
        },
        {
            "id": "0ML1002_1",
            "marketId": "mkt_1002",
            "label": "Djokovic, N.",
            "trueOdds": 1.234,
            "displayOdds": {"decimal": "1.23"},
            "outcomeType": "Home",
            "participants": [{"countryCode": "SRB"}],
        },
        {
            "id": "0ML1002_2",
            "marketId": "mkt_1002",
            "label": "Medvedev, D.",
            "trueOdds": 4.567,
            "displayOdds": {"decimal": "4.57"},
            "outcomeType": "Away",
            "participants": [{"countryCode": "RUS"}],
        },
    ],
}

SAMPLE_SPREAD_RESPONSE = {
    "leagues": [
        {"id": "101", "name": "Challenger Murcia"},
    ],
    "events": [
        {"id": "2001", "leagueId": "101"},
    ],
    "markets": [
        {"id": "mkt_2001", "eventId": "2001", "leagueId": "101", "subcategoryId": 16089},
    ],
    "selections": [
        {
            "id": "0SP2001_1",
            "marketId": "mkt_2001",
            "label": "Player A",
            "trueOdds": 1.909,
            "outcomeType": "Home",
            "participants": [{"countryCode": "ARG"}],
            "points": -3.5,
        },
        {
            "id": "0SP2001_2",
            "marketId": "mkt_2001",
            "label": "Player B",
            "trueOdds": 1.909,
            "outcomeType": "Away",
            "participants": [{"countryCode": "BRA"}],
            "points": 3.5,
        },
    ],
}


def _make_tennis_html(state: dict) -> str:
    """Wrap state dict in a mock HTML page with __INITIAL_STATE__."""
    return (
        "<html><head><script>"
        f"window.__INITIAL_STATE__ = {json.dumps(state)};"
        "</script></head><body></body></html>"
    )


class TestCircuitFiltering:
    def test_atp_included(self):
        assert _is_atp_challenger("atp-miami-open") is True

    def test_challenger_included(self):
        assert _is_atp_challenger("challenger-murcia") is True

    def test_grand_slams_included(self):
        assert _is_atp_challenger("australian-open-men") is True
        assert _is_atp_challenger("french-open-men") is True
        assert _is_atp_challenger("wimbledon-men") is True
        assert _is_atp_challenger("us-open-men") is True

    def test_wta_excluded(self):
        assert _is_atp_challenger("wta-miami-open") is False

    def test_itf_excluded(self):
        assert _is_atp_challenger("itf-m25-cairo") is False

    def test_utr_excluded(self):
        assert _is_atp_challenger("utr-pro-match") is False

    def test_doubles_excluded(self):
        assert _is_atp_challenger("atp-miami-open-doubles") is False

    def test_women_excluded(self):
        assert _is_atp_challenger("atp-rome-women") is False


class TestParseTennisLeagues:
    @patch("mvp.draftkings.odds.BaseExtractor._create_session")
    def test_filters_to_atp_challenger(self, mock_create_session):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = _make_tennis_html(SAMPLE_INITIAL_STATE)
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = DraftKingsOddsScraper()
        leagues = scraper.fetch_tennis_leagues()

        slugs = {lg["slug"] for lg in leagues}
        assert "atp-miami-open" in slugs
        assert "challenger-murcia" in slugs
        assert "australian-open-men" in slugs
        assert "wta-miami-open" not in slugs
        assert "itf-m25-cairo" not in slugs
        assert "utr-pro-match" not in slugs
        assert "atp-miami-open-doubles" not in slugs
        assert "atp-rome-women" not in slugs

    @patch("mvp.draftkings.odds.BaseExtractor._create_session")
    def test_returns_correct_fields(self, mock_create_session):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = _make_tennis_html(SAMPLE_INITIAL_STATE)
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = DraftKingsOddsScraper()
        leagues = scraper.fetch_tennis_leagues()

        for lg in leagues:
            assert "dk_tournament_id" in lg
            assert "name" in lg
            assert "slug" in lg

    @patch("mvp.draftkings.odds.BaseExtractor._create_session")
    def test_no_initial_state_raises(self, mock_create_session):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = "<html><body>No state here</body></html>"
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = DraftKingsOddsScraper()
        with pytest.raises(ValueError, match="Could not find __INITIAL_STATE__"):
            scraper.fetch_tennis_leagues()


class TestParseLeagueOdds:
    def test_moneyline_entries(self):
        now = datetime.now(timezone.utc)
        entries = _parse_odds_response(SAMPLE_ODDS_RESPONSE, "moneyline", now)

        assert len(entries) == 4

        sinner = next(e for e in entries if "Sinner" in e.player_name)
        assert sinner.book == "dk"
        assert sinner.market == "moneyline"
        assert sinner.odds == 1.456
        assert sinner.country_code == "ITA"
        assert sinner.side == "home"
        assert sinner.opponent_name == "Alcaraz, C."
        assert sinner.points is None
        assert sinner.dk_event_id == "1001"

    def test_selection_grain_two_per_match(self):
        now = datetime.now(timezone.utc)
        entries = _parse_odds_response(SAMPLE_ODDS_RESPONSE, "moneyline", now)

        event_ids = {e.dk_event_id for e in entries}
        for eid in event_ids:
            match_entries = [e for e in entries if e.dk_event_id == eid]
            assert len(match_entries) == 2

    def test_points_none_for_moneyline(self):
        now = datetime.now(timezone.utc)
        entries = _parse_odds_response(SAMPLE_ODDS_RESPONSE, "moneyline", now)

        for e in entries:
            assert e.points is None

    def test_points_present_for_spreads(self):
        now = datetime.now(timezone.utc)
        entries = _parse_odds_response(SAMPLE_SPREAD_RESPONSE, "game_spread", now)

        assert len(entries) == 2
        home = next(e for e in entries if e.side == "home")
        away = next(e for e in entries if e.side == "away")
        assert home.points == -3.5
        assert away.points == 3.5

    def test_empty_response(self):
        now = datetime.now(timezone.utc)
        entries = _parse_odds_response({"events": [], "markets": [], "selections": []}, "moneyline", now)
        assert entries == []

    def test_opponent_name_symmetric(self):
        now = datetime.now(timezone.utc)
        entries = _parse_odds_response(SAMPLE_ODDS_RESPONSE, "moneyline", now)

        match_1001 = [e for e in entries if e.dk_event_id == "1001"]
        names = {e.player_name for e in match_1001}
        opponents = {e.opponent_name for e in match_1001}
        assert names == opponents

    def test_filters_by_subcategory(self):
        """Moneyline parse should ignore spread markets."""
        now = datetime.now(timezone.utc)
        entries = _parse_odds_response(SAMPLE_SPREAD_RESPONSE, "moneyline", now)
        assert entries == []


class TestFetchLeagueOdds:
    @patch("mvp.draftkings.odds.BaseExtractor._create_session")
    def test_returns_entries_and_raw(self, mock_create_session):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_ODDS_RESPONSE
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = DraftKingsOddsScraper()
        entries, raw = scraper.fetch_league_odds("100")

        assert len(entries) == 4
        assert raw == SAMPLE_ODDS_RESPONSE

    @patch("mvp.draftkings.odds.BaseExtractor._create_session")
    def test_empty_response_returns_empty(self, mock_create_session):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"events": [], "markets": [], "selections": []}
        mock_session.get.return_value = mock_resp
        mock_create_session.return_value = mock_session

        scraper = DraftKingsOddsScraper()
        entries, raw = scraper.fetch_league_odds("999")
        assert entries == []


class TestFetchAllOdds:
    @patch("mvp.draftkings.odds.BaseExtractor._create_session")
    def test_end_to_end(self, mock_create_session):
        mock_session = MagicMock()

        tennis_resp = MagicMock()
        tennis_resp.text = _make_tennis_html(SAMPLE_INITIAL_STATE)

        odds_resp = MagicMock()
        odds_resp.json.return_value = SAMPLE_ODDS_RESPONSE

        mock_session.get.side_effect = [tennis_resp, odds_resp, odds_resp, odds_resp]
        mock_create_session.return_value = mock_session

        scraper = DraftKingsOddsScraper()
        entries, raw = scraper.fetch_all_odds(market="moneyline")

        assert len(entries) == 12
        assert len(raw) == 3

    def test_invalid_market_raises(self):
        scraper = DraftKingsOddsScraper()
        with pytest.raises(ValueError, match="Unknown market"):
            scraper.fetch_all_odds(market="invalid")
