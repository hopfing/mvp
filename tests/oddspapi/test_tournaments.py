"""oddspapi tournamentName -> our tournament_id.

Tournament identity is the constraint the date was standing in for. `_match_tournament`
in the shared mapper cannot supply it: of 184 oddspapi names, zero match one of ours
exactly, so its substring comparison no-ops and leaves every meeting of the pair in
play.
"""

from __future__ import annotations

import polars as pl
import pytest

from mvp.oddspapi import tournaments


def _rows(records):
    return pl.DataFrame(records, schema={
        "tournament_id": pl.Utf8, "tournament_name": pl.Utf8,
        "city": pl.Utf8, "circuit": pl.Utf8, "year": pl.Int64,
    })


OURS = _rows([
    # Two events in one city, separated by circuit — the common case.
    {"tournament_id": "1536", "tournament_name": "Madrid 1", "city": "Madrid",
     "circuit": "tour", "year": 2026},
    {"tournament_id": "9687", "tournament_name": "Madrid 2", "city": "Madrid",
     "circuit": "chal", "year": 2026},
    # Slams, matched on name rather than city.
    {"tournament_id": "520", "tournament_name": "Roland Garros", "city": "Paris",
     "circuit": "tour", "year": 2026},
    {"tournament_id": "540", "tournament_name": "Wimbledon", "city": "London",
     "circuit": "tour", "year": 2026},
    # Same city AND circuit as Wimbledon: only the unsuffixed name separates them.
    {"tournament_id": "311", "tournament_name": "London", "city": "London",
     "circuit": "tour", "year": 2026},
    # Same city, same circuit, numbered on both sides.
    {"tournament_id": "2831", "tournament_name": "Oeiras 1", "city": "Oeiras",
     "circuit": "chal", "year": 2026},
    {"tournament_id": "2833", "tournament_name": "Oeiras 2", "city": "Oeiras",
     "circuit": "chal", "year": 2026},
    # Numbered ours, Roman numeral theirs.
    {"tournament_id": "7736", "tournament_name": "Shymkent 1", "city": "Shymkent",
     "circuit": "chal", "year": 2026},
    {"tournament_id": "9192", "tournament_name": "Shymkent 2", "city": "Shymkent",
     "circuit": "chal", "year": 2026},
    # Apostrophe on our side only.
    {"tournament_id": "440", "tournament_name": "'s-Hertogenbosch",
     "city": "'s-Hertogenbosch", "circuit": "tour", "year": 2026},
    {"tournament_id": "2941", "tournament_name": "Lincoln", "city": "Lincoln",
     "circuit": "chal", "year": 2026},
    # A prior season, to prove the year is part of the key.
    {"tournament_id": "9999", "tournament_name": "Madrid 1", "city": "Madrid",
     "circuit": "tour", "year": 2025},
])


@pytest.fixture
def index():
    return tournaments.TournamentIndex(OURS)


class TestParseName:
    @pytest.mark.parametrize("name,expected", [
        ("ATP Miami, USA Men Singles", ("miami", None)),
        ("ATP Challenger Oeiras 1, Portugal Men Singles", ("oeiras", 1)),
        ("Wimbledon Men Singles", ("wimbledon", None)),
        # A comma before the suffix — 'san diego, usa' came out as the city before.
        ("ATP Challenger San Diego, USA, Men Singles", ("san diego", None)),
        ("ATP Challenger Abidjan 2, Cote d Ivoire, Men Singles", ("abidjan", 2)),
        # Roman numeral against our 'Shymkent 2'.
        ("ATP Challenger Shymkent II, Kazakhstan Men Singles", ("shymkent", 2)),
        # Parenthetical state.
        ("ATP Challenger Lincoln (NE), USA Men Singles", ("lincoln", None)),
        ("ATP S-Hertogenbosch, Netherlands Men Singles", ("s hertogenbosch", None)),
    ])
    def test_parses(self, name, expected):
        assert tournaments.parse_name(name) == expected


class TestLookup:
    def test_circuit_separates_two_events_in_one_city(self, index):
        """Our 'Madrid 1' is tour and 'Madrid 2' is challenger, so the suffix is the
        circuit — which oddspapi supplies as categoryName."""
        tour, how = index.lookup("ATP Madrid, Spain Men Singles", "tour", 2026)
        chal, _ = index.lookup(
            "ATP Challenger Madrid, Spain Men Singles", "chal", 2026,
        )
        assert (tour, how) == (frozenset({"1536"}), "city")
        assert chal == frozenset({"9687"})

    def test_name_match_wins_for_the_majors(self, index):
        got, how = index.lookup("Wimbledon Men Singles", "tour", 2026)
        assert (got, how) == (frozenset({"540"}), "name")

    def test_unsuffixed_name_separates_same_city_same_circuit(self, index):
        """Queen's and Wimbledon are both city=London, circuit=tour. 'ATP London'
        does not match a name, so the city path must prefer our unsuffixed event."""
        got, how = index.lookup("ATP London, Great Britain Men Singles", "tour", 2026)
        assert (got, how) == (frozenset({"311"}), "city")

    def test_sequence_number_separates_repeats(self, index):
        one, _ = index.lookup("ATP Challenger Oeiras 1, Portugal Men Singles",
                              "chal", 2026)
        two, _ = index.lookup("ATP Challenger Oeiras 2, Portugal Men Singles",
                              "chal", 2026)
        assert (one, two) == (frozenset({"2831"}), frozenset({"2833"}))

    def test_roman_numeral_sequence(self, index):
        got, _ = index.lookup("ATP Challenger Shymkent II, Kazakhstan Men Singles",
                              "chal", 2026)
        assert got == frozenset({"9192"})

    def test_apostrophe_is_ignored(self, index):
        got, _ = index.lookup("ATP S-Hertogenbosch, Netherlands Men Singles",
                              "tour", 2026)
        assert got == frozenset({"440"})

    def test_unnumbered_repeat_narrows_rather_than_guessing(self, index):
        """oddspapi says 'Shymkent' with no number and we hold two. Picking one would
        be a coin flip; narrowing to the city's two still beats every meeting ever."""
        got, how = index.lookup("ATP Challenger Shymkent, Kazakhstan Men Singles",
                                "chal", 2026)
        assert how == "narrowed"
        assert got == frozenset({"7736", "9192"})

    def test_year_is_part_of_the_key(self, index):
        got, _ = index.lookup("ATP Madrid, Spain Men Singles", "tour", 2025)
        assert got == frozenset({"9999"})

    def test_unknown_city_is_a_miss_not_a_guess(self, index):
        got, how = index.lookup("ATP Challenger Montemar, Spain Men Singles",
                                "chal", 2026)
        assert (got, how) == (frozenset(), "miss")

    def test_missing_category_cannot_use_the_city_path(self, index):
        """No categoryName means no circuit, and circuit is what separates the two
        events a city hosts."""
        got, how = index.lookup("ATP Madrid, Spain Men Singles", None, 2026)
        assert (got, how) == (frozenset(), "miss")


class TestResolveFixtures:
    def _fixture(self, fid, name, category="Challenger", start="2026-05-01T10:00:00Z"):
        return {"fixtureId": fid, "tournamentName": name, "categoryName": category,
                "startTime": start, "trueStartTime": None}

    def test_reports_pinned_and_narrowed_separately(self, index, monkeypatch):
        monkeypatch.setattr(tournaments, "_load_aliases", lambda: {})
        ids, rep = tournaments.resolve_fixtures([
            self._fixture("f1", "ATP Challenger Oeiras 1, Portugal Men Singles"),
            self._fixture("f2", "ATP Challenger Shymkent, Kazakhstan Men Singles"),
            self._fixture("f3", "ATP Challenger Montemar, Spain Men Singles"),
        ], index=index)
        assert ids["f1"] == frozenset({"2831"})
        assert ids["f2"] == frozenset({"7736", "9192"})
        assert "f3" not in ids
        assert (rep.pinned, rep.narrowed) == (1, 1)
        assert sum(rep.unresolved.values()) == 1

    def test_alias_takes_precedence(self, index, monkeypatch):
        monkeypatch.setattr(tournaments, "_load_aliases", lambda: {"french open": "520"})
        ids, rep = tournaments.resolve_fixtures(
            [self._fixture("f1", "French Open Men Singles", category="ATP")],
            index=index,
        )
        assert ids["f1"] == frozenset({"520"})
        assert rep.by_alias == 1

    def test_shipped_aliases_are_wellformed(self):
        """The file is hand-maintained, so a typo should fail here rather than
        silently drop a tournament."""
        aliases = tournaments._load_aliases()
        assert aliases
        assert all(v.isdigit() for v in aliases.values())
        assert "french open" in aliases
