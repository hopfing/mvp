"""Tests for FanDuel odds scraper helpers and parser.

Fixtures in tests/fanduel/fixtures/ are real responses captured during the
2026-04-08 validation pass:
  - sport_page.json: SPORT page response (32 competitions, all tennis)
  - competition_page_monte_carlo.json: ATP Monte Carlo 2026 (singles + doubles
    + tournament-winner futures)
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mvp.fanduel.odds import (
    _ACCESS_KEY_PATTERNS,
    _derive_event_status,
    _extract_access_key,
    _extract_competitions,
    _is_atp_challenger,
    _is_doubles,
    _is_outright,
    _parse_competition_response,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# -- Circuit classification --


class TestIsAtpChallenger:
    @pytest.mark.parametrize("name", [
        "ATP Monte Carlo 2026",
        "ATP Madrid 2026",
        "Monza Challenger 2026",
        "Wuning Challenger 2026",
        "Madrid Challenger 2026",
        "Campinas Challenger 2026",
        "Men's Wimbledon 2026",
        "Men's Australian Open 2027",
        "Men's French Open 2026",
        "Men's US Open 2026",
    ])
    def test_includes(self, name):
        assert _is_atp_challenger(name) is True

    @pytest.mark.parametrize("name", [
        "ITF Sharm El Sheikh",
        "ITF USA Futures",
        "WTA Linz 2026",
        "WTA Madrid II 2026",
        "Women's Wimbledon 2026",
        "Women's Australian Open 2027",
        "Mens UTR Pro Series Australia",
        "Ladies UTR Pro Series Australia",
        "Billie Jean King Cup",
        "",
    ])
    def test_excludes(self, name):
        assert _is_atp_challenger(name) is False


# -- Doubles detection --


class TestIsDoubles:
    @pytest.mark.parametrize("name", [
        "M Berrettini / A Vavassori v H Heliovaara / H Patten",
        "K Krawietz / T Puetz v A Goransson / E King",
        "C Harrison / N Skupski v M Arevalo / M Pavic",
    ])
    def test_doubles(self, name):
        assert _is_doubles(name) is True

    @pytest.mark.parametrize("name", [
        "Matteo Berrettini v Joao Fonseca",
        "Carlos Alcaraz v Tomas Martin Etcheverry",
        "Jannik Sinner v Tomas Machac",
        "",
    ])
    def test_singles(self, name):
        assert _is_doubles(name) is False


# -- Outright filter --


class TestIsOutright:
    def test_event_name_matches_competition(self):
        assert _is_outright("ATP Monte Carlo 2026", "ATP Monte Carlo 2026") is True

    def test_event_name_with_padding_matches(self):
        # FanDuel sometimes prefixes outright event names with whitespace
        assert _is_outright("   ATP Monte Carlo 2026", "ATP Monte Carlo 2026") is True

    def test_match_event_is_not_outright(self):
        assert _is_outright(
            "Matteo Berrettini v Joao Fonseca", "ATP Monte Carlo 2026",
        ) is False

    def test_event_without_v_separator_is_outright(self):
        assert _is_outright("Tournament Winner Futures", "ATP Madrid 2026") is True


# -- Event status mapping --


class TestDeriveEventStatus:
    def test_open_not_in_play(self):
        assert _derive_event_status("OPEN", False) == "NOT_STARTED"

    def test_in_play_overrides_status(self):
        assert _derive_event_status("OPEN", True) == "IN_PLAY"

    def test_in_play_with_suspended_status(self):
        assert _derive_event_status("SUSPENDED", True) == "IN_PLAY"

    def test_closed_not_in_play(self):
        assert _derive_event_status("CLOSED", False) == "FINISHED"

    def test_suspended_not_in_play(self):
        assert _derive_event_status("SUSPENDED", False) == "FINISHED"

    def test_empty_status(self):
        assert _derive_event_status("", False) == "FINISHED"


# -- Access key extraction (regex robustness) --


class TestExtractAccessKey:
    def test_primary_pattern(self):
        snippet = (
            'l.SB_STUBS_REMOTE="SB_STUBS_REMOTE";'
            'const d="FhMFpcPWXMeyZxOx",u=(e,t)=>e,m="645b09da6f"'
        )
        assert _extract_access_key(snippet) == "FhMFpcPWXMeyZxOx"

    def test_renamed_minified_var(self):
        # Minified var name should not be hardcoded — webpack can rename it.
        snippet = (
            'l.SB_STUBS_REMOTE="SB_STUBS_REMOTE";'
            'const ZZ="ABCDEFGHIJKL1234"'
        )
        assert _extract_access_key(snippet) == "ABCDEFGHIJKL1234"

    def test_fallback_pattern_via_fanduel_object(self):
        # Primary anchor (immediate adjacency) shouldn't match here, but the
        # fallback pattern should still find the key bracketed between
        # SB_STUBS_REMOTE and [a.FANDUEL].
        snippet = (
            'SB_STUBS_REMOTE="x"; some.other.code(); '
            'const k="DEADBEEF12345678"; '
            'function E(e){return {[a.FANDUEL]:"other"}}'
        )
        assert _extract_access_key(snippet) == "DEADBEEF12345678"

    def test_missing_raises(self):
        with pytest.raises(ValueError, match="Could not extract"):
            _extract_access_key("var foo = 'bar';")

    def test_pattern_count_is_two(self):
        # Sanity: primary + fallback. Adding more patterns is fine but should
        # be intentional.
        assert len(_ACCESS_KEY_PATTERNS) == 2


# -- Competition extraction from SPORT page --


class TestExtractCompetitions:
    def test_returns_only_atp_challenger_men_slams(self):
        data = _load_fixture("sport_page.json")
        comps = _extract_competitions(data)
        names = [c["name"] for c in comps]

        # Things we expect to find in the captured fixture
        assert "ATP Monte Carlo 2026" in names
        assert "Monza Challenger 2026" in names
        assert "Men's Wimbledon 2026" in names
        assert "Men's French Open 2026" in names

        # Things that must be filtered out
        for n in names:
            assert not n.startswith("ITF")
            assert not n.startswith("WTA")
            assert not n.startswith("Women's")
            assert "UTR Pro Series" not in n
            assert "Billie Jean King Cup" not in n

    def test_each_entry_has_id_and_name(self):
        data = _load_fixture("sport_page.json")
        comps = _extract_competitions(data)
        assert len(comps) > 0
        for c in comps:
            assert c["fd_competition_id"]
            assert c["name"]
            assert isinstance(c["fd_competition_id"], str)


# -- Per-competition response parser --


class TestParseCompetitionResponse:
    def setup_method(self):
        self.data = _load_fixture("competition_page_monte_carlo.json")
        self.fetched = datetime(2026, 4, 8, 21, 30, tzinfo=UTC)
        self.entries = _parse_competition_response(
            self.data, "ATP Monte Carlo 2026", self.fetched,
        )

    def test_returns_some_entries(self):
        assert len(self.entries) > 0

    def test_two_entries_per_match(self):
        # Each moneyline market has two runners, so total entries must be even
        assert len(self.entries) % 2 == 0

    def test_outright_filtered(self):
        # The futures "ATP Monte Carlo 2026" event has no " v " separator
        # and must be excluded.
        for e in self.entries:
            assert e.tournament == "ATP Monte Carlo 2026"
            # No entry should be for the tournament-winner futures
            # (the competition name itself never appears as a player name)
            assert e.player_name != "ATP Monte Carlo 2026"
            assert e.player_name != "   ATP Monte Carlo 2026"

    def test_doubles_filtered(self):
        for e in self.entries:
            assert " / " not in e.player_name
            assert " / " not in e.opponent_name

    def test_includes_known_singles_match(self):
        # From the validation pass: Berrettini v Fonseca was on the slate
        names = {e.player_name for e in self.entries}
        assert "Matteo Berrettini" in names
        assert "Joao Fonseca" in names

    def test_decimal_odds_in_reasonable_range(self):
        for e in self.entries:
            assert 1.01 <= e.odds <= 50.0, f"odds out of range: {e.odds} ({e.player_name})"

    def test_two_runners_per_event_have_each_other_as_opponent(self):
        by_event: dict[str, list] = {}
        for e in self.entries:
            by_event.setdefault(e.fd_event_id, []).append(e)
        for event_id, group in by_event.items():
            assert len(group) == 2
            a, b = group
            assert a.opponent_name == b.player_name
            assert b.opponent_name == a.player_name

    def test_side_normalized_lowercase(self):
        for e in self.entries:
            assert e.side in {"home", "away"}

    def test_event_status_present(self):
        for e in self.entries:
            assert e.event_status in {"NOT_STARTED", "IN_PLAY", "FINISHED"}

    def test_book_field_set(self):
        for e in self.entries:
            assert e.book == "fd"

    def test_market_field_set(self):
        for e in self.entries:
            assert e.market == "moneyline"

    def test_competition_id_propagated(self):
        for e in self.entries:
            assert e.fd_tournament_id == "12793721"

    def test_fetched_at_propagated(self):
        for e in self.entries:
            assert e.fetched_at == self.fetched

    def test_country_code_blank(self):
        # FanDuel doesn't expose runner country in the payload
        for e in self.entries:
            assert e.country_code == ""

    def test_points_none_for_moneyline(self):
        for e in self.entries:
            assert e.points is None
