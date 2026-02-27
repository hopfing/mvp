"""Tests for ResultsParser — singles and doubles HTML parsing."""

import logging

import pytest

from mvp.atptour.parsers.results import ResultsParser

# ---------------------------------------------------------------------------
# HTML fixture helpers
# ---------------------------------------------------------------------------


def _singles_stats_item(
    player_id: str = "d643",
    name: str = "Novak Djokovic",
    seed_entry: str = "(4)",
    country: str = "srb",
    is_winner: bool = True,
    scores: list[tuple[int, int | None]] | None = None,
) -> str:
    """Build a singles stats-item div.

    scores: list of (games, tiebreak_or_None) per set.
    """
    winner_div = (
        '<div class="winner"><span class="icon-checkmark"></span></div>'
        if is_winner
        else ""
    )
    seed_span = f"<span>{seed_entry}</span>" if seed_entry else ""
    flag = (
        f'<svg class="atp-flag">'
        f'<use href="/assets/flags.svg#flag-{country}"></use></svg>'
        if country
        else ""
    )

    score_items = '<div class="score-item"></div>'  # empty leader
    if scores:
        for games, tb in scores:
            tb_span = f"<span>{tb}</span>" if tb is not None else ""
            score_items += (
                f'<div class="score-item"><span>{games}</span>{tb_span}</div>'
            )

    return f"""
    <div class="stats-item">
      <div class="player-info">
        <div class="profile">
          <img class="player-image" alt="Player-Photo-{player_id}" src="" />
        </div>
        <div class="country">{flag}</div>
        <div class="name">
          <a href="/en/players/test/{player_id}/overview">{name}</a>
          {seed_span}
        </div>
        {winner_div}
      </div>
      <div class="scores">{score_items}</div>
    </div>
    """


def _doubles_stats_item(
    player1_id: str = "a853",
    player1_name: str = "Player One",
    player1_country: str = "esa",
    player2_id: str = "r513",
    player2_name: str = "Player Two",
    player2_country: str = "ned",
    seed_entry: str = "(3)",
    is_winner: bool = True,
    scores: list[tuple[int, int | None]] | None = None,
) -> str:
    """Build a doubles stats-item div."""
    winner_div = (
        '<div class="winner"><span class="icon-checkmark"></span></div>'
        if is_winner
        else ""
    )
    seed_span = f"<span>{seed_entry}</span>" if seed_entry else ""

    score_items = '<div class="score-item"></div>'
    if scores:
        for games, tb in scores:
            tb_span = f"<span>{tb}</span>" if tb is not None else ""
            score_items += (
                f'<div class="score-item"><span>{games}</span>{tb_span}</div>'
            )

    return f"""
    <div class="stats-item">
      <div class="player-info">
        <div class="profiles">
          <div class="profile">
            <img class="player-image" alt="Player-Photo-{player1_id}" />
          </div>
          <div class="profile">
            <img class="player-image" alt="Player-Photo-{player2_id}" />
          </div>
        </div>
        <div class="countries">
          <div class="country">
            <svg class="atp-flag">
              <use href="/assets/flags.svg#flag-{player1_country}"></use>
            </svg>
          </div>
          <div class="country">
            <svg class="atp-flag">
              <use href="/assets/flags.svg#flag-{player2_country}"></use>
            </svg>
          </div>
        </div>
        <div class="players">
          <div class="names">
            <div class="name">
              <a href="/en/players/p1/{player1_id}/overview">{player1_name}</a>
              {seed_span}
            </div>
            <div class="name">
              <a href="/en/players/p2/{player2_id}/overview">{player2_name}</a>
              {seed_span}
            </div>
          </div>
        </div>
        {winner_div}
      </div>
      <div class="scores">{score_items}</div>
    </div>
    """


def _match_div(
    round_text: str = "Finals",
    duration: str = "02:56",
    stats_items: str = "",
    match_id: str = "ms001",
    include_footer: bool = True,
) -> str:
    """Build a complete match div wrapping stats items."""
    footer = ""
    if include_footer:
        footer = f"""
        <div class="match-footer">
          <div class="match-cta">
            <a href="/en/scores/match-stats/archive/2023/580/{match_id}">Stats</a>
          </div>
        </div>
        """
    return f"""
    <div class="match">
      <div class="match-header">
        <span><strong>{round_text} - </strong></span>
        <span>{duration}</span>
      </div>
      <div class="match-content"><div class="match-stats">
        {stats_items}
      </div></div>
      {footer}
    </div>
    """


def _wrap_html(body: str) -> str:
    """Wrap body content in a minimal HTML document."""
    return f"<html><body>{body}</body></html>"


def _singles_match_html(
    round_text: str = "Finals",
    duration: str = "02:56",
    match_id: str = "ms001",
    player_kwargs: dict | None = None,
    opp_kwargs: dict | None = None,
) -> str:
    """Build a complete single-match HTML page for singles tests."""
    p_kw = player_kwargs or {}
    o_kw = opp_kwargs or {
        "player_id": "s0ag",
        "name": "Stefanos Tsitsipas",
        "seed_entry": "(5)",
        "country": "gre",
        "is_winner": False,
    }
    stats = _singles_stats_item(**p_kw) + _singles_stats_item(**o_kw)
    return _wrap_html(_match_div(round_text, duration, stats, match_id))


def _doubles_match_html(
    round_text: str = "Finals",
    duration: str = "01:30",
    match_id: str = "md001",
    team1_kwargs: dict | None = None,
    team2_kwargs: dict | None = None,
) -> str:
    """Build a complete single-match HTML page for doubles tests."""
    t1_kw = team1_kwargs or {}
    t2_kw = team2_kwargs or {
        "player1_id": "b123",
        "player1_name": "Opp One",
        "player1_country": "fra",
        "player2_id": "c456",
        "player2_name": "Opp Two",
        "player2_country": "ger",
        "seed_entry": "(7)",
        "is_winner": False,
    }
    stats = _doubles_stats_item(**t1_kw) + _doubles_stats_item(**t2_kw)
    return _wrap_html(_match_div(round_text, duration, stats, match_id))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parser():
    return ResultsParser()


# ---------------------------------------------------------------------------
# Tests: parse_singles
# ---------------------------------------------------------------------------


class TestParseSingles:
    def test_basic_match(self, parser):
        html = _singles_match_html(
            player_kwargs={
                "player_id": "d643",
                "name": "Novak Djokovic",
                "seed_entry": "(4)",
                "country": "srb",
                "is_winner": True,
                "scores": [(6, None), (7, None), (7, None)],
            },
            opp_kwargs={
                "player_id": "s0ag",
                "name": "Stefanos Tsitsipas",
                "seed_entry": "(5)",
                "country": "gre",
                "is_winner": False,
                "scores": [(3, None), (6, 4), (6, 3)],
            },
        )
        result = parser.parse_singles(html)
        assert len(result) == 1
        m = result[0]
        assert m["round_text"] == "Finals"
        assert m["match_id"] == "ms001"
        assert m["duration_text"] == "02:56"
        assert m["player_id"] == "d643"
        assert m["player_name"] == "Novak Djokovic"
        assert m["player_seed_entry"] == "(4)"
        assert m["player_country"] == "srb"
        assert m["player_won"] is True
        assert m["opp_id"] == "s0ag"
        assert m["opp_name"] == "Stefanos Tsitsipas"
        assert m["opp_seed_entry"] == "(5)"
        assert m["opp_country"] == "gre"
        assert m["player_scores"] == [6, 7, 7]
        assert m["opp_scores"] == [3, 6, 6]
        assert m["result_type"] == "completed"

    def test_tiebreak_assignment(self, parser):
        """TB value appears on set loser; winner derived as max(7, loser+2)."""
        html = _singles_match_html(
            player_kwargs={
                "scores": [(7, None), (6, 5)],
                "is_winner": True,
            },
            opp_kwargs={
                "player_id": "x123",
                "name": "Opponent",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(6, 3), (7, None)],
            },
        )
        result = parser.parse_singles(html)
        assert len(result) == 1
        m = result[0]
        # Set 1: opp has raw tb=3, player wins -> player_tb = max(7, 3+2) = 7
        assert m["player_tiebreaks"][0] == 7
        assert m["opp_tiebreaks"][0] == 3
        # Set 2: player has raw tb=5, opp wins -> opp_tb = max(7, 5+2) = 7
        assert m["player_tiebreaks"][1] == 5
        assert m["opp_tiebreaks"][1] == 7

    def test_high_tiebreak_values(self, parser):
        """When loser TB >= 6, winner TB = loser + 2 (not capped at 7)."""
        html = _singles_match_html(
            player_kwargs={
                "scores": [(7, None)],
                "is_winner": True,
            },
            opp_kwargs={
                "player_id": "x123",
                "name": "Opponent",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(6, 10)],
            },
        )
        result = parser.parse_singles(html)
        m = result[0]
        assert m["player_tiebreaks"][0] == 12  # max(7, 10+2) = 12
        assert m["opp_tiebreaks"][0] == 10

    def test_no_tiebreak_set(self, parser):
        html = _singles_match_html(
            player_kwargs={"scores": [(6, None), (6, None)]},
            opp_kwargs={
                "player_id": "x123",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(3, None), (4, None)],
            },
        )
        result = parser.parse_singles(html)
        m = result[0]
        assert m["player_tiebreaks"] == [None, None]
        assert m["opp_tiebreaks"] == [None, None]

    def test_walkover_result_type(self, parser):
        """No scores at all = walkover."""
        html = _singles_match_html(
            player_kwargs={"scores": None},
            opp_kwargs={
                "player_id": "x123",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": None,
            },
        )
        result = parser.parse_singles(html)
        m = result[0]
        assert m["result_type"] == "walkover"
        assert m["player_scores"] == []
        assert m["opp_scores"] == []

    def test_retirement_result_type(self, parser):
        """Last set max < 6 = retirement."""
        html = _singles_match_html(
            player_kwargs={"scores": [(6, None), (3, None)]},
            opp_kwargs={
                "player_id": "x123",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(4, None), (1, None)],
            },
        )
        result = parser.parse_singles(html)
        assert result[0]["result_type"] == "retirement"

    def test_completed_result_type(self, parser):
        html = _singles_match_html(
            player_kwargs={"scores": [(6, None), (6, None)]},
            opp_kwargs={
                "player_id": "x123",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(4, None), (3, None)],
            },
        )
        result = parser.parse_singles(html)
        assert result[0]["result_type"] == "completed"

    def test_empty_html(self, parser):
        html = _wrap_html("")
        assert parser.parse_singles(html) == []

    def test_no_match_divs(self, parser):
        html = _wrap_html("<div>nothing here</div>")
        assert parser.parse_singles(html) == []

    def test_multiple_matches(self, parser):
        match1 = _match_div(
            round_text="Semifinals",
            duration="01:45",
            stats_items=(
                _singles_stats_item(player_id="a1", name="P1")
                + _singles_stats_item(
                    player_id="b1", name="P2", is_winner=False
                )
            ),
            match_id="ms010",
        )
        match2 = _match_div(
            round_text="Quarterfinals",
            duration="02:10",
            stats_items=(
                _singles_stats_item(player_id="c1", name="P3")
                + _singles_stats_item(
                    player_id="d1", name="P4", is_winner=False
                )
            ),
            match_id="ms020",
        )
        html = _wrap_html(match1 + match2)
        result = parser.parse_singles(html)
        assert len(result) == 2
        assert result[0]["round_text"] == "Semifinals"
        assert result[0]["match_id"] == "ms010"
        assert result[1]["round_text"] == "Quarterfinals"
        assert result[1]["match_id"] == "ms020"

    def test_no_seed_entry(self, parser):
        html = _singles_match_html(
            player_kwargs={"seed_entry": "", "scores": [(6, None)]},
            opp_kwargs={
                "player_id": "x1",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(4, None)],
            },
        )
        result = parser.parse_singles(html)
        assert result[0]["player_seed_entry"] == ""
        assert result[0]["opp_seed_entry"] == ""

    def test_no_footer_match_id_is_none(self, parser):
        stats = _singles_stats_item() + _singles_stats_item(
            player_id="x1", name="Opp", is_winner=False
        )
        match = _match_div(stats_items=stats, include_footer=False)
        html = _wrap_html(match)
        result = parser.parse_singles(html)
        assert result[0]["match_id"] is None

    def test_duration_text_none_when_missing(self, parser):
        """When header has only one span, duration_text is None."""
        stats = _singles_stats_item() + _singles_stats_item(
            player_id="x1", name="Opp", is_winner=False
        )
        html = _wrap_html(f"""
        <div class="match">
          <div class="match-header">
            <span><strong>Finals - </strong></span>
          </div>
          <div class="match-content"><div class="match-stats">
            {stats}
          </div></div>
        </div>
        """)
        result = parser.parse_singles(html)
        assert result[0]["duration_text"] is None

    def test_duration_text_none_when_empty(self, parser):
        """When second span is empty, duration_text is None."""
        stats = _singles_stats_item() + _singles_stats_item(
            player_id="x1", name="Opp", is_winner=False
        )
        html = _wrap_html(f"""
        <div class="match">
          <div class="match-header">
            <span><strong>Finals - </strong></span>
            <span></span>
          </div>
          <div class="match-content"><div class="match-stats">
            {stats}
          </div></div>
        </div>
        """)
        result = parser.parse_singles(html)
        assert result[0]["duration_text"] is None


# ---------------------------------------------------------------------------
# Tests: skip logging (singles)
# ---------------------------------------------------------------------------


class TestSinglesSkipLogging:
    def test_skip_missing_header(self, parser, caplog):
        html = _wrap_html('<div class="match"><div>no header</div></div>')
        with caplog.at_level(logging.WARNING):
            result = parser.parse_singles(html)
        assert result == []
        assert "missing match-header" in caplog.text

    def test_skip_missing_strong(self, parser, caplog):
        html = _wrap_html("""
        <div class="match">
          <div class="match-header"><span>no strong</span></div>
        </div>
        """)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_singles(html)
        assert result == []
        assert "missing strong tag" in caplog.text

    def test_skip_fewer_than_2_stats_items(self, parser, caplog):
        html = _wrap_html("""
        <div class="match">
          <div class="match-header">
            <span><strong>Finals - </strong></span>
            <span>01:00</span>
          </div>
          <div class="match-content"><div class="match-stats">
            <div class="stats-item"></div>
          </div></div>
        </div>
        """)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_singles(html)
        assert result == []
        assert "fewer than 2 stats-items" in caplog.text

    def test_skip_missing_player_link(self, parser, caplog):
        """Player without <a> tag (e.g., WTA player in United Cup)."""
        no_link_item = """
        <div class="stats-item">
          <div class="player-info">
            <div class="profile">
              <img class="player-image" alt="Player-Photo-x1" />
            </div>
            <div class="country"></div>
            <div class="name"><span>No Link Player</span></div>
          </div>
          <div class="scores"></div>
        </div>
        """
        valid_item = _singles_stats_item(
            player_id="v1", name="Valid", is_winner=False
        )
        match = _match_div(stats_items=no_link_item + valid_item)
        html = _wrap_html(match)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_singles(html)
        assert result == []
        assert "missing player link" in caplog.text

    def test_skip_missing_opponent_link(self, parser, caplog):
        """Opponent without <a> tag."""
        valid_item = _singles_stats_item(player_id="v1", name="Valid")
        no_link_item = """
        <div class="stats-item">
          <div class="player-info">
            <div class="profile">
              <img class="player-image" alt="Player-Photo-x1" />
            </div>
            <div class="country"></div>
            <div class="name"><span>No Link Player</span></div>
          </div>
          <div class="scores"></div>
        </div>
        """
        match = _match_div(stats_items=valid_item + no_link_item)
        html = _wrap_html(match)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_singles(html)
        assert result == []
        assert "missing opponent link" in caplog.text


# ---------------------------------------------------------------------------
# Tests: round text parsing
# ---------------------------------------------------------------------------


class TestParseRoundText:
    def test_strips_venue_suffix(self, parser):
        html = _singles_match_html(round_text="Finals - Rod Laver Arena")
        result = parser.parse_singles(html)
        assert result[0]["round_text"] == "Finals"

    def test_strips_trailing_dash(self, parser):
        html = _singles_match_html(round_text="Semifinals -")
        result = parser.parse_singles(html)
        assert result[0]["round_text"] == "Semifinals"

    def test_strips_day_suffix(self, parser):
        html = _singles_match_html(round_text="Round Robin Day 3 -")
        result = parser.parse_singles(html)
        assert result[0]["round_text"] == "Round Robin"

    def test_day_suffix_case_insensitive(self, parser):
        html = _singles_match_html(round_text="Round Robin day 2 -")
        result = parser.parse_singles(html)
        assert result[0]["round_text"] == "Round Robin"


# ---------------------------------------------------------------------------
# Tests: score parsing
# ---------------------------------------------------------------------------


class TestParseScores:
    def test_normal_three_set_match(self, parser):
        html = _singles_match_html(
            player_kwargs={"scores": [(6, None), (7, None), (6, None)]},
            opp_kwargs={
                "player_id": "x1",
                "name": "O",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(4, None), (5, None), (3, None)],
            },
        )
        result = parser.parse_singles(html)
        assert result[0]["player_scores"] == [6, 7, 6]
        assert result[0]["opp_scores"] == [4, 5, 3]

    def test_five_set_match(self, parser):
        html = _singles_match_html(
            player_kwargs={
                "scores": [(6, None), (4, None), (6, None), (3, None), (7, None)]
            },
            opp_kwargs={
                "player_id": "x1",
                "name": "O",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(4, None), (6, None), (3, None), (6, None), (6, 5)],
            },
        )
        result = parser.parse_singles(html)
        assert result[0]["player_scores"] == [6, 4, 6, 3, 7]
        assert result[0]["opp_scores"] == [4, 6, 3, 6, 6]

    def test_no_scores_div(self, parser):
        """stats-item with no scores div returns empty lists."""
        # Directly test the static method
        from bs4 import BeautifulSoup

        html = '<div class="stats-item"><div class="player-info"></div></div>'
        soup = BeautifulSoup(html, "lxml")
        div = soup.find("div", class_="stats-item")
        games, tbs = ResultsParser._parse_scores(div)
        assert games == []
        assert tbs == []


# ---------------------------------------------------------------------------
# Tests: _assign_tiebreaks
# ---------------------------------------------------------------------------


class TestAssignTiebreaks:
    def test_no_tiebreaks(self):
        p_tb, o_tb = ResultsParser._assign_tiebreaks(
            [6, 6], [None, None], [3, 4], [None, None]
        )
        assert p_tb == [None, None]
        assert o_tb == [None, None]

    def test_player_is_set_loser(self):
        """Player has raw TB (they lost the set TB)."""
        p_tb, o_tb = ResultsParser._assign_tiebreaks(
            [6, 6], [5, None], [7, 3], [None, None]
        )
        assert p_tb == [5, None]
        assert o_tb == [7, None]  # max(7, 5+2) = 7

    def test_opp_is_set_loser(self):
        """Opponent has raw TB (they lost the set TB)."""
        p_tb, o_tb = ResultsParser._assign_tiebreaks(
            [7], [None], [6], [4]
        )
        assert p_tb == [max(7, 4 + 2)]  # 7
        assert o_tb == [4]

    def test_high_tiebreak(self):
        """loser_tb >= 6 means winner_tb = loser_tb + 2."""
        p_tb, o_tb = ResultsParser._assign_tiebreaks(
            [7], [None], [6], [8]
        )
        assert p_tb == [10]  # max(7, 8+2) = 10
        assert o_tb == [8]

    def test_both_have_values_warning(self, caplog):
        """Both sides with TB values logs a warning and preserves both."""
        with caplog.at_level(logging.WARNING):
            p_tb, o_tb = ResultsParser._assign_tiebreaks(
                [7], [5], [6], [3]
            )
        assert p_tb == [5]
        assert o_tb == [3]
        assert "Both players have tiebreak values" in caplog.text

    def test_mixed_sets(self):
        """Multi-set match with TB only in one set."""
        p_tb, o_tb = ResultsParser._assign_tiebreaks(
            [6, 7, 6],
            [None, None, None],
            [4, 6, 3],
            [None, 5, None],
        )
        assert p_tb == [None, max(7, 5 + 2), None]
        assert o_tb == [None, 5, None]

    def test_raw_tb_shorter_than_games(self):
        """When raw TB list is shorter than games, treat missing as None."""
        p_tb, o_tb = ResultsParser._assign_tiebreaks(
            [6, 7],
            [None],  # only 1 element for 2 sets
            [4, 6],
            [None, 3],
        )
        # Set 1: both None -> None, None
        # Set 2: p_raw out of range (None), o_raw=3 -> p=max(7,3+2)=7, o=3
        assert p_tb == [None, 7]
        assert o_tb == [None, 3]


# ---------------------------------------------------------------------------
# Tests: match ID parsing
# ---------------------------------------------------------------------------


class TestParseMatchId:
    def test_match_stats_url(self, parser):
        html = _singles_match_html(match_id="ms001")
        result = parser.parse_singles(html)
        assert result[0]["match_id"] == "ms001"

    def test_stats_centre_url(self, parser):
        """The 'stats-centre' URL variant also works."""
        stats = _singles_stats_item() + _singles_stats_item(
            player_id="x1", name="Opp", is_winner=False
        )
        html = _wrap_html(f"""
        <div class="match">
          <div class="match-header">
            <span><strong>Finals - </strong></span>
            <span>01:00</span>
          </div>
          <div class="match-content"><div class="match-stats">
            {stats}
          </div></div>
          <div class="match-footer">
            <div class="match-cta">
              <a href="/en/scores/stats-centre/live/2023/580/ms005">Stats</a>
            </div>
          </div>
        </div>
        """)
        result = parser.parse_singles(html)
        assert result[0]["match_id"] == "ms005"

    def test_no_stats_link(self, parser):
        """Footer with no match-stats or stats-centre link returns None."""
        stats = _singles_stats_item() + _singles_stats_item(
            player_id="x1", name="Opp", is_winner=False
        )
        html = _wrap_html(f"""
        <div class="match">
          <div class="match-header">
            <span><strong>Finals - </strong></span>
            <span>01:00</span>
          </div>
          <div class="match-content"><div class="match-stats">
            {stats}
          </div></div>
          <div class="match-footer">
            <div class="match-cta">
              <a href="/en/players/head-to-head">H2H</a>
            </div>
          </div>
        </div>
        """)
        result = parser.parse_singles(html)
        assert result[0]["match_id"] is None

    def test_trailing_slash_stripped(self, parser):
        """Trailing slash in URL doesn't affect match ID extraction."""
        from bs4 import BeautifulSoup

        footer_html = """
        <div class="match-footer">
          <div class="match-cta">
            <a href="/en/scores/match-stats/archive/2023/580/ms099/">Stats</a>
          </div>
        </div>
        """
        soup = BeautifulSoup(footer_html, "lxml")
        footer = soup.find("div", class_="match-footer")
        assert ResultsParser._parse_match_id(footer) == "ms099"


# ---------------------------------------------------------------------------
# Tests: result type derivation
# ---------------------------------------------------------------------------


class TestDeriveResultType:
    def test_completed(self):
        assert ResultsParser._derive_result_type([6, 7], [4, 5]) == "completed"

    def test_completed_with_tiebreak_set(self):
        assert ResultsParser._derive_result_type([6, 7, 7], [4, 6, 6]) == "completed"

    def test_retirement(self):
        assert ResultsParser._derive_result_type([6, 3], [4, 1]) == "retirement"

    def test_retirement_last_set_5(self):
        assert ResultsParser._derive_result_type([6, 5], [4, 3]) == "retirement"

    def test_walkover(self):
        assert ResultsParser._derive_result_type([], []) == "walkover"


# ---------------------------------------------------------------------------
# Tests: parse_doubles
# ---------------------------------------------------------------------------


class TestParseDoubles:
    def test_basic_doubles_match(self, parser):
        html = _doubles_match_html(
            team1_kwargs={
                "player1_id": "a853",
                "player1_name": "Marcelo Arevalo",
                "player1_country": "esa",
                "player2_id": "r513",
                "player2_name": "Jean-Julien Rojer",
                "player2_country": "ned",
                "seed_entry": "(3)",
                "is_winner": True,
                "scores": [(6, None), (7, None)],
            },
            team2_kwargs={
                "player1_id": "b123",
                "player1_name": "Ivan Dodig",
                "player1_country": "cro",
                "player2_id": "c456",
                "player2_name": "Austin Krajicek",
                "player2_country": "usa",
                "seed_entry": "(7)",
                "is_winner": False,
                "scores": [(4, None), (6, 5)],
            },
        )
        result = parser.parse_doubles(html)
        assert len(result) == 1
        m = result[0]
        assert m["round_text"] == "Finals"
        assert m["match_id"] == "md001"
        assert m["duration_text"] == "01:30"
        assert m["player_id"] == "a853"
        assert m["player_name"] == "Marcelo Arevalo"
        assert m["partner_id"] == "r513"
        assert m["partner_name"] == "Jean-Julien Rojer"
        assert m["player_country"] == "esa"
        assert m["partner_country"] == "ned"
        assert m["player_seed_entry"] == "(3)"
        assert m["player_won"] is True
        assert m["opp_id"] == "b123"
        assert m["opp_name"] == "Ivan Dodig"
        assert m["opp_partner_id"] == "c456"
        assert m["opp_partner_name"] == "Austin Krajicek"
        assert m["opp_country"] == "cro"
        assert m["opp_partner_country"] == "usa"
        assert m["opp_seed_entry"] == "(7)"
        assert m["player_scores"] == [6, 7]
        assert m["opp_scores"] == [4, 6]
        assert m["result_type"] == "completed"

    def test_doubles_tiebreaks(self, parser):
        html = _doubles_match_html(
            team1_kwargs={"scores": [(7, None)], "is_winner": True},
            team2_kwargs={
                "player1_id": "b1",
                "player1_name": "O1",
                "player1_country": "fra",
                "player2_id": "c1",
                "player2_name": "O2",
                "player2_country": "ger",
                "seed_entry": "",
                "is_winner": False,
                "scores": [(6, 4)],
            },
        )
        result = parser.parse_doubles(html)
        m = result[0]
        assert m["player_tiebreaks"] == [max(7, 4 + 2)]  # 7
        assert m["opp_tiebreaks"] == [4]

    def test_doubles_walkover(self, parser):
        html = _doubles_match_html(
            team1_kwargs={"scores": None},
            team2_kwargs={
                "player1_id": "b1",
                "player1_name": "O1",
                "player1_country": "fra",
                "player2_id": "c1",
                "player2_name": "O2",
                "player2_country": "ger",
                "seed_entry": "",
                "is_winner": False,
                "scores": None,
            },
        )
        result = parser.parse_doubles(html)
        assert result[0]["result_type"] == "walkover"

    def test_empty_html(self, parser):
        html = _wrap_html("")
        assert parser.parse_doubles(html) == []


# ---------------------------------------------------------------------------
# Tests: doubles skip logging
# ---------------------------------------------------------------------------


class TestDoublesSkipLogging:
    def test_skip_missing_header(self, parser, caplog):
        html = _wrap_html('<div class="match"><div>no header</div></div>')
        with caplog.at_level(logging.WARNING):
            result = parser.parse_doubles(html)
        assert result == []
        assert "missing match-header" in caplog.text

    def test_skip_missing_strong(self, parser, caplog):
        html = _wrap_html("""
        <div class="match">
          <div class="match-header"><span>no strong</span></div>
        </div>
        """)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_doubles(html)
        assert result == []
        assert "missing strong tag" in caplog.text

    def test_skip_fewer_than_2_stats_items(self, parser, caplog):
        html = _wrap_html("""
        <div class="match">
          <div class="match-header">
            <span><strong>Finals - </strong></span>
            <span>01:00</span>
          </div>
          <div class="match-content"><div class="match-stats">
            <div class="stats-item"></div>
          </div></div>
        </div>
        """)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_doubles(html)
        assert result == []
        assert "fewer than 2 stats-items" in caplog.text

    def test_skip_missing_profiles_div(self, parser, caplog):
        """Team without profiles div (e.g., United Cup mixed doubles)."""
        # Use a singles-style item as team1 (no profiles div)
        no_profiles_item = _singles_stats_item(player_id="x1", name="Solo")
        valid_team = _doubles_stats_item(is_winner=False)
        match = _match_div(stats_items=no_profiles_item + valid_team)
        html = _wrap_html(match)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_doubles(html)
        assert result == []
        assert "missing team1 data" in caplog.text

    def test_skip_missing_team2_data(self, parser, caplog):
        """Team2 without valid doubles structure."""
        valid_team = _doubles_stats_item(is_winner=True)
        no_profiles_item = _singles_stats_item(
            player_id="x1", name="Solo", is_winner=False
        )
        match = _match_div(stats_items=valid_team + no_profiles_item)
        html = _wrap_html(match)
        with caplog.at_level(logging.WARNING):
            result = parser.parse_doubles(html)
        assert result == []
        assert "missing team2 data" in caplog.text


# ---------------------------------------------------------------------------
# Tests: _parse_player static method
# ---------------------------------------------------------------------------


class TestParsePlayer:
    def test_no_country_flag(self, parser):
        html = _singles_match_html(
            player_kwargs={"country": "", "scores": [(6, None)]},
            opp_kwargs={
                "player_id": "x1",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(4, None)],
            },
        )
        result = parser.parse_singles(html)
        # Empty country flag means no use tag with #flag- pattern
        assert result[0]["player_country"] == ""

    def test_winner_detection(self, parser):
        html = _singles_match_html(
            player_kwargs={"is_winner": True, "scores": [(6, None)]},
            opp_kwargs={
                "player_id": "x1",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": False,
                "scores": [(4, None)],
            },
        )
        result = parser.parse_singles(html)
        assert result[0]["player_won"] is True

    def test_loser_detection(self, parser):
        html = _singles_match_html(
            player_kwargs={"is_winner": False, "scores": [(4, None)]},
            opp_kwargs={
                "player_id": "x1",
                "name": "Opp",
                "seed_entry": "",
                "country": "usa",
                "is_winner": True,
                "scores": [(6, None)],
            },
        )
        result = parser.parse_singles(html)
        assert result[0]["player_won"] is False


# ---------------------------------------------------------------------------
# Tests: _parse_team static method
# ---------------------------------------------------------------------------


class TestParseTeam:
    def test_fewer_than_2_imgs_returns_none(self):
        """Team with only 1 player image returns None."""
        from bs4 import BeautifulSoup

        html = """
        <div class="stats-item">
          <div class="player-info">
            <div class="profiles">
              <div class="profile">
                <img class="player-image" alt="Player-Photo-a1" />
              </div>
            </div>
            <div class="players">
              <div class="names">
                <div class="name"><a href="/p1">P1</a></div>
                <div class="name"><a href="/p2">P2</a></div>
              </div>
            </div>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        div = soup.find("div", class_="stats-item")
        assert ResultsParser._parse_team(div) is None

    def test_fewer_than_2_a_tags_returns_none(self):
        """Team with fewer than 2 name links returns None."""
        from bs4 import BeautifulSoup

        html = """
        <div class="stats-item">
          <div class="player-info">
            <div class="profiles">
              <div class="profile">
                <img class="player-image" alt="Player-Photo-a1" />
              </div>
              <div class="profile">
                <img class="player-image" alt="Player-Photo-b1" />
              </div>
            </div>
            <div class="players">
              <div class="names">
                <div class="name"><a href="/p1">P1</a></div>
                <div class="name"><span>No Link</span></div>
              </div>
            </div>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        div = soup.find("div", class_="stats-item")
        assert ResultsParser._parse_team(div) is None

    def test_missing_countries_defaults_to_empty(self):
        """Team without countries div gets empty country strings."""
        from bs4 import BeautifulSoup

        html = """
        <div class="stats-item">
          <div class="player-info">
            <div class="profiles">
              <div class="profile">
                <img class="player-image" alt="Player-Photo-a1" />
              </div>
              <div class="profile">
                <img class="player-image" alt="Player-Photo-b1" />
              </div>
            </div>
            <div class="players">
              <div class="names">
                <div class="name"><a href="/p1">P1</a></div>
                <div class="name"><a href="/p2">P2</a></div>
              </div>
            </div>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        div = soup.find("div", class_="stats-item")
        result = ResultsParser._parse_team(div)
        assert result is not None
        assert result["players"][0]["country"] == ""
        assert result["players"][1]["country"] == ""


class TestParseTournamentDates:
    """Tests for _parse_tournament_dates."""

    def test_parses_date_range(self):
        """Parses '2-8 May, 2022' format correctly."""
        from datetime import date

        from bs4 import BeautifulSoup

        html = """
        <div class="date-location">
          <span>Salvador De Bahia, Brazil</span>
          |
          <span>2-8 May, 2022</span>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        start, end = ResultsParser()._parse_tournament_dates(soup)
        assert start == date(2022, 5, 2)
        assert end == date(2022, 5, 8)

    def test_parses_single_day(self):
        """Parses '8 May, 2022' as start=end."""
        from datetime import date

        from bs4 import BeautifulSoup

        html = """
        <div class="date-location">
          <span>City, Country</span>
          |
          <span>8 May, 2022</span>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        start, end = ResultsParser()._parse_tournament_dates(soup)
        assert start == date(2022, 5, 8)
        assert end == date(2022, 5, 8)

    def test_parses_jan_format(self):
        """Parses '4-9 Jan, 2022' format."""
        from datetime import date

        from bs4 import BeautifulSoup

        html = """
        <div class="date-location">
          <span>City</span>
          |
          <span>4-9 Jan, 2022</span>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        start, end = ResultsParser()._parse_tournament_dates(soup)
        assert start == date(2022, 1, 4)
        assert end == date(2022, 1, 9)

    def test_missing_date_location_returns_none(self):
        """Missing date-location div returns (None, None)."""
        from bs4 import BeautifulSoup

        html = "<div>No date here</div>"
        soup = BeautifulSoup(html, "lxml")
        start, end = ResultsParser()._parse_tournament_dates(soup)
        assert start is None
        assert end is None

    def test_missing_date_span_returns_none(self):
        """Only one span (no date) returns (None, None)."""
        from bs4 import BeautifulSoup

        html = """
        <div class="date-location">
          <span>City, Country</span>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        start, end = ResultsParser()._parse_tournament_dates(soup)
        assert start is None
        assert end is None

    def test_dates_in_parsed_matches(self):
        """Tournament dates appear in parsed match dicts."""
        from datetime import date

        # Build a minimal match div
        stats = _singles_stats_item() + _singles_stats_item(
            player_id="s0ag", name="Opponent", is_winner=False
        )
        match_content = _match_div("Finals", "02:00", stats, "ms001")

        html = f"""
        <html>
        <div class="date-location">
          <span>City</span>
          |
          <span>2-8 May, 2022</span>
        </div>
        {match_content}
        </html>
        """
        matches = ResultsParser().parse_singles(html)
        assert len(matches) == 1
        assert matches[0]["tournament_start_date"] == date(2022, 5, 2)
        assert matches[0]["tournament_end_date"] == date(2022, 5, 8)
