"""Tests for BetMGM odds scraper."""

from datetime import datetime, timezone

import pytest


def _make_fixture(
    fixture_id=19186550,
    p1_name="Carlos Alcaraz (ESP)",
    p2_name="Casper Ruud (NOR)",
    p1_short="C. Alcaraz",
    p2_short="C. Ruud",
    p1_odds=1.08,
    p2_odds=7.25,
    competition_id=6,
    competition_name="ATP",
    tournament_name="ATP Masters Indian Wells (USA) - Hard",
    tournament_id=123,
    stage="PreMatch",
    is_open=True,
):
    return {
        "id": fixture_id,
        "name": {"value": f"{p1_name} - {p2_name}"},
        "startDate": "2026-03-12T17:00:00Z",
        "stage": stage,
        "sport": {"id": 5, "name": {"value": "Tennis"}},
        "competition": {"id": competition_id, "name": {"value": competition_name}},
        "tournament": {"id": tournament_id, "name": {"value": tournament_name}},
        "participants": [
            {"participantId": 1, "name": {"value": p1_name, "short": p1_short}},
            {"participantId": 2, "name": {"value": p2_name, "short": p2_short}},
        ],
        "games": [
            {
                "name": {"value": "Match winner"},
                "results": [
                    {"id": -1, "odds": p1_odds, "name": {"value": p1_short}},
                    {"id": -2, "odds": p2_odds, "name": {"value": p2_short}},
                ],
                "isMain": True,
            },
            {
                "name": {"value": "Set betting"},
                "results": [{"id": -3, "odds": 2.5, "name": {"value": "2-0"}}],
                "isMain": False,
            },
        ],
        "isOpenForBetting": is_open,
    }


class TestStripCountryCode:
    def test_strips_parenthetical(self):
        from mvp.betmgm.odds import _strip_country_code

        assert _strip_country_code("Carlos Alcaraz (ESP)") == "Carlos Alcaraz"

    def test_no_country_code(self):
        from mvp.betmgm.odds import _strip_country_code

        assert _strip_country_code("Carlos Alcaraz") == "Carlos Alcaraz"

    def test_handles_empty(self):
        from mvp.betmgm.odds import _strip_country_code

        assert _strip_country_code("") == ""


class TestParseFixtures:
    def test_parses_moneyline_odds(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([_make_fixture()], now)

        assert len(entries) == 2
        assert entries[0].book == "mgm"
        assert entries[0].odds == 1.08
        assert entries[1].odds == 7.25

    def test_strips_country_code_from_name(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([_make_fixture()], now)

        assert entries[0].player_name == "Carlos Alcaraz"
        assert entries[1].player_name == "Casper Ruud"

    def test_maps_competition_to_circuit(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        atp = _parse_fixtures([_make_fixture(competition_id=6, competition_name="ATP")], now)
        chal = _parse_fixtures([_make_fixture(competition_id=10, competition_name="Challenger")], now)

        assert atp[0].circuit == "atp"
        assert chal[0].circuit == "challenger"

    def test_filters_non_atp_challenger(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([
            _make_fixture(competition_id=7, competition_name="WTA"),
        ], now)

        assert len(entries) == 0

    def test_filters_doubles(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([
            _make_fixture(
                p1_name="A. Erler/A. Vavassori",
                p2_name="Y. Bhambri/A. Goransson",
                p1_short="A. Erler/A. Vavassori",
                p2_short="Y. Bhambri/A. Goransson",
            ),
        ], now)

        assert len(entries) == 0

    def test_maps_event_status(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        pre = _parse_fixtures([_make_fixture(stage="PreMatch")], now)
        live = _parse_fixtures([_make_fixture(stage="Live")], now)

        assert pre[0].event_status == "NOT_STARTED"
        assert live[0].event_status == "STARTED"

    def test_sets_opponent_name(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([_make_fixture()], now)

        assert entries[0].opponent_name == "Casper Ruud"
        assert entries[1].opponent_name == "Carlos Alcaraz"

    def test_sets_event_id(self):
        from mvp.betmgm.odds import _parse_fixtures

        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([_make_fixture(fixture_id=12345)], now)

        assert entries[0].mgm_event_id == "12345"

    def test_skips_fixture_without_match_winner(self):
        from mvp.betmgm.odds import _parse_fixtures

        fixture = _make_fixture()
        fixture["games"] = [{"name": {"value": "Set betting"}, "results": [], "isMain": False}]
        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([fixture], now)

        assert len(entries) == 0

    def test_skips_fixture_with_fewer_than_2_results(self):
        from mvp.betmgm.odds import _parse_fixtures

        fixture = _make_fixture()
        fixture["games"][0]["results"] = [{"id": -1, "odds": 1.5, "name": {"value": "C. Alcaraz"}}]
        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        entries = _parse_fixtures([fixture], now)

        assert len(entries) == 0
