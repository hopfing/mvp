"""Unit tests for mvp.caesars.odds parser helpers."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mvp.caesars.odds import (
    OddsEntry,
    _strip_pipes,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    with (FIXTURES_DIR / name).open() as f:
        return json.load(f)


class TestStripPipes:
    def test_removes_leading_and_trailing_pipes(self):
        assert _strip_pipes("|Carlos Alcaraz|") == "Carlos Alcaraz"

    def test_returns_empty_string_for_empty_input(self):
        assert _strip_pipes("") == ""

    def test_returns_empty_string_for_none_input(self):
        assert _strip_pipes(None) == ""

    def test_preserves_name_without_pipes(self):
        assert _strip_pipes("Carlos Alcaraz") == "Carlos Alcaraz"

    def test_strips_only_leading_and_trailing_pipes_not_internal(self):
        # Caesars wraps edges only; any internal pipes are pathological
        # and we preserve them rather than mangle the name.
        assert _strip_pipes("|Foo|Bar|") == "Foo|Bar"


class TestOddsEntry:
    def test_dataclass_fields(self):
        entry = OddsEntry(
            book="czr",
            czr_event_id="event-uuid",
            market="moneyline",
            czr_selection_id="selection-uuid",
            player_name="Carlos Alcaraz",
            country_code="",
            side="home",
            odds=1.02857,
            points=None,
            tournament="ATP Monte Carlo",
            czr_tournament_id="atp-mc-uuid",
            opponent_name="Tomas Martin Etcheverry",
            event_status="NOT_STARTED",
            fetched_at=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        )
        assert entry.book == "czr"
        assert entry.odds == 1.02857
        assert entry.country_code == ""
        assert entry.points is None


from mvp.caesars.odds import _is_atp_or_challenger


class TestIsAtpOrChallenger:
    def test_includes_atp(self):
        assert _is_atp_or_challenger("ATP") is True

    def test_includes_challenger(self):
        assert _is_atp_or_challenger("Challenger") is True

    def test_excludes_wta(self):
        assert _is_atp_or_challenger("WTA") is False

    def test_excludes_atp_doubles(self):
        assert _is_atp_or_challenger("ATP Doubles") is False

    def test_excludes_challenger_doubles(self):
        assert _is_atp_or_challenger("Challenger Doubles") is False

    def test_excludes_utr(self):
        assert _is_atp_or_challenger("UTR") is False

    def test_excludes_empty_string(self):
        assert _is_atp_or_challenger("") is False

    def test_excludes_none(self):
        assert _is_atp_or_challenger(None) is False


from mvp.caesars.odds import _extract_competitions


class TestExtractCompetitions:
    def test_returns_atp_and_challenger_only(self):
        data = _load_fixture("tabs_top_level.json")
        result = _extract_competitions(data)
        names = {c["name"] for c in result}
        assert names == {"ATP Monte Carlo", "ATP Challenger Madrid"}

    def test_excludes_wta_doubles_utr(self):
        data = _load_fixture("tabs_top_level.json")
        result = _extract_competitions(data)
        names = {c["name"] for c in result}
        assert "WTA Madrid" not in names
        assert "ATP Monte Carlo Doubles" not in names
        assert "UTR Women's Singles" not in names

    def test_each_entry_has_required_keys(self):
        data = _load_fixture("tabs_top_level.json")
        result = _extract_competitions(data)
        for entry in result:
            assert "czr_competition_id" in entry
            assert "name" in entry
            assert "collection_name" in entry
            assert entry["czr_competition_id"]  # non-empty

    def test_returns_empty_list_on_no_eligible_competitions(self):
        data = {"competitions": [
            {"id": "x", "name": "WTA X", "collectionName": "WTA", "events": []},
        ]}
        result = _extract_competitions(data)
        assert result == []

    def test_returns_empty_list_on_missing_competitions_key(self):
        assert _extract_competitions({}) == []


from mvp.caesars.odds import _derive_event_status


class TestDeriveEventStatus:
    def test_in_play_when_live_template_present(self):
        event = {"started": True, "active": True}
        markets = [{"templateId": "_7cMatch_20Betting_20Live_7c", "tradedInPlay": True}]
        assert _derive_event_status(event, markets) == "IN_PLAY"

    def test_in_play_when_market_traded_in_play_true(self):
        event = {"started": True, "active": True}
        markets = [{"templateId": "_7cMatch_20Betting_7c", "tradedInPlay": True}]
        assert _derive_event_status(event, markets) == "IN_PLAY"

    def test_not_started_when_not_started_and_active(self):
        event = {"started": False, "active": True}
        markets = [{"templateId": "_7cMatch_20Betting_7c", "tradedInPlay": False}]
        assert _derive_event_status(event, markets) == "NOT_STARTED"

    def test_finished_when_not_started_and_not_active(self):
        event = {"started": False, "active": False}
        markets = [{"templateId": "_7cMatch_20Betting_7c", "tradedInPlay": False}]
        assert _derive_event_status(event, markets) == "FINISHED"

    def test_finished_when_started_but_no_live_markets(self):
        event = {"started": True, "active": True}
        markets = [{"templateId": "_7cMatch_20Betting_7c", "tradedInPlay": False}]
        assert _derive_event_status(event, markets) == "FINISHED"

    def test_not_started_with_empty_markets_list(self):
        event = {"started": False, "active": True}
        assert _derive_event_status(event, []) == "NOT_STARTED"


from mvp.caesars.odds import _parse_competition_response


class TestParseCompetitionResponse:
    @pytest.fixture
    def fetched_at(self):
        return datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC)

    @pytest.fixture
    def parsed(self, fetched_at):
        data = _load_fixture("competition_monte_carlo.json")
        return _parse_competition_response(data, fetched_at)

    def test_emits_two_rows_per_valid_event(self, parsed):
        # 3 events in fixture: Alcaraz (valid pre-match ML), Bergs
        # (valid in-play ML), Placeholder (placeholder — skipped).
        # Expected: 2 events × 2 selections = 4 rows.
        assert len(parsed) == 4

    def test_all_rows_are_moneyline(self, parsed):
        for entry in parsed:
            assert entry.market == "moneyline"

    def test_all_rows_have_book_czr(self, parsed):
        for entry in parsed:
            assert entry.book == "czr"

    def test_strips_pipes_from_player_and_opponent(self, parsed):
        for entry in parsed:
            assert "|" not in entry.player_name
            assert "|" not in entry.opponent_name

    def test_reads_decimal_odds_for_alcaraz(self, parsed):
        alcaraz = next(e for e in parsed if e.player_name == "Carlos Alcaraz")
        assert alcaraz.odds == 1.02857
        assert alcaraz.side == "home"

    def test_reads_decimal_odds_for_etcheverry(self, parsed):
        etche = next(e for e in parsed if e.player_name == "Tomas Martin Etcheverry")
        assert etche.odds == 11.0
        assert etche.side == "away"

    def test_opponent_is_other_selection_name(self, parsed):
        alcaraz = next(e for e in parsed if e.player_name == "Carlos Alcaraz")
        assert alcaraz.opponent_name == "Tomas Martin Etcheverry"
        etche = next(e for e in parsed if e.player_name == "Tomas Martin Etcheverry")
        assert etche.opponent_name == "Carlos Alcaraz"

    def test_bergs_zverev_derived_as_in_play(self, parsed):
        bergs = next(e for e in parsed if e.player_name == "Zizou Bergs")
        assert bergs.event_status == "IN_PLAY"

    def test_alcaraz_derived_as_not_started(self, parsed):
        alcaraz = next(e for e in parsed if e.player_name == "Carlos Alcaraz")
        assert alcaraz.event_status == "NOT_STARTED"

    def test_skips_placeholder_markets(self, parsed):
        names = {e.player_name for e in parsed}
        assert "Placeholder Player A" not in names
        assert "Placeholder Player B" not in names

    def test_tournament_field_is_competition_name(self, parsed):
        for entry in parsed:
            assert entry.tournament == "ATP Monte Carlo"

    def test_tournament_id_populated(self, parsed):
        for entry in parsed:
            assert entry.czr_tournament_id == "atp-mc-uuid"

    def test_country_code_always_empty(self, parsed):
        for entry in parsed:
            assert entry.country_code == ""

    def test_points_always_none_for_moneyline(self, parsed):
        for entry in parsed:
            assert entry.points is None

    def test_fetched_at_preserved_on_all_rows(self, parsed, fetched_at):
        for entry in parsed:
            assert entry.fetched_at == fetched_at

    def test_skips_non_match_event_type(self, fetched_at):
        data = {
            "competitions": [{
                "id": "c1", "name": "ATP Test", "collectionName": "ATP",
                "events": [{
                    "id": "e1", "name": "A vs B", "type": "OUTRIGHT",
                    "competitionId": "c1", "competitionName": "ATP Test",
                    "started": False, "active": True,
                    "keyMarketGroups": [{"markets": [{
                        "templateId": "_7cMatch_20Betting_7c",
                        "placeholder": False,
                        "selections": [
                            {"id": "s1", "name": "|A|", "type": "home",
                             "active": True, "price": {"d": 1.5}},
                            {"id": "s2", "name": "|B|", "type": "away",
                             "active": True, "price": {"d": 2.5}},
                        ],
                    }]}],
                }],
            }],
        }
        result = _parse_competition_response(data, fetched_at)
        assert result == []

    def test_handles_malformed_price(self, fetched_at):
        data = {
            "competitions": [{
                "id": "c1", "name": "ATP Test", "collectionName": "ATP",
                "events": [{
                    "id": "e1", "name": "A vs B", "type": "MATCH",
                    "competitionId": "c1", "competitionName": "ATP Test",
                    "started": False, "active": True,
                    "keyMarketGroups": [{"markets": [{
                        "templateId": "_7cMatch_20Betting_7c",
                        "placeholder": False,
                        "selections": [
                            {"id": "s1", "name": "|A|", "type": "home",
                             "active": True, "price": {"d": None}},
                            {"id": "s2", "name": "|B|", "type": "away",
                             "active": True, "price": {"d": 2.5}},
                        ],
                    }]}],
                }],
            }],
        }
        result = _parse_competition_response(data, fetched_at)
        # Malformed price → bad selection dropped, but the other
        # selection's opponent_name would be empty. Simpler rule: if
        # either selection is malformed, drop the whole pair. Test
        # asserts that rule.
        assert result == []

    def test_skips_markets_with_fewer_than_two_selections(self, fetched_at):
        data = {
            "competitions": [{
                "id": "c1", "name": "ATP Test", "collectionName": "ATP",
                "events": [{
                    "id": "e1", "name": "A vs B", "type": "MATCH",
                    "competitionId": "c1", "competitionName": "ATP Test",
                    "started": False, "active": True,
                    "keyMarketGroups": [{"markets": [{
                        "templateId": "_7cMatch_20Betting_7c",
                        "placeholder": False,
                        "selections": [
                            {"id": "s1", "name": "|A|", "type": "home",
                             "active": True, "price": {"d": 1.5}},
                        ],
                    }]}],
                }],
            }],
        }
        result = _parse_competition_response(data, fetched_at)
        assert result == []
