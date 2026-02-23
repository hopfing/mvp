"""Tests for ResultsTransformer — raw HTML to staged parquet."""

from pathlib import Path

import polars as pl

from mvp.atptour.tournament import Tournament
from mvp.atptour.transformers.results import ResultsTransformer
from mvp.common.enums import Circuit

# ---------------------------------------------------------------------------
# HTML fixture helpers
# ---------------------------------------------------------------------------


def _player_div(
    pid: str,
    name: str,
    country: str,
    *,
    seed: str = "",
    winner: bool = False,
    scores: str = "",
) -> str:
    """Build a singles player stats-item div."""
    winner_html = (
        '<div class="winner">'
        '<span class="icon-checkmark"></span></div>'
        if winner
        else ""
    )
    seed_html = f"<span>{seed}</span>" if seed else ""
    return (
        '<div class="stats-item"><div class="player-info">'
        f'<div class="profile">'
        f'<img class="player-image" alt="Player-Photo-{pid}" />'
        f'</div>'
        f'<div class="country">'
        f'<svg><use href="flags.svg#flag-{country}"></use></svg>'
        f'</div>'
        f'<div class="name">'
        f'<a href="/en/players/x/{pid}/overview">{name}</a>'
        f'{seed_html}</div>'
        f'{winner_html}'
        f'</div><div class="scores">{scores}</div></div>'
    )


def _scores_html(sets: list[tuple[int, int | None]]) -> str:
    """Build score-item divs from (games, tiebreak|None) tuples."""
    out = '<div class="score-item"></div>'
    for games, tb in sets:
        tb_span = f"<span>{tb}</span>" if tb is not None else ""
        out += (
            f'<div class="score-item">'
            f"<span>{games}</span>{tb_span}</div>"
        )
    return out


def _match_html(
    round_text: str,
    player_html: str,
    opp_html: str,
    *,
    duration: str = "",
    match_id: str | None = None,
) -> str:
    """Wrap player divs in a complete match div."""
    footer = ""
    if match_id:
        footer = (
            '<div class="match-footer"><div class="match-cta">'
            '<a href="/en/scores/match-stats/'
            f'archive/2023/580/{match_id}">Stats</a>'
            "</div></div>"
        )
    return (
        '<div class="match">'
        '<div class="match-header">'
        f"<span><strong>{round_text} - </strong></span>"
        f"<span>{duration}</span></div>"
        '<div class="match-content"><div class="match-stats">'
        f"{player_html}{opp_html}"
        f"</div></div>{footer}</div>"
    )


def _doubles_team_div(
    p1_id: str,
    p1_name: str,
    p1_country: str,
    p2_id: str,
    p2_name: str,
    p2_country: str,
    *,
    seed: str = "",
    winner: bool = False,
    scores: str = "",
) -> str:
    """Build a doubles team stats-item div."""
    winner_html = (
        '<div class="winner">'
        '<span class="icon-checkmark"></span></div>'
        if winner
        else ""
    )
    seed_html = f"<span>{seed}</span>" if seed else ""
    return (
        '<div class="stats-item"><div class="player-info">'
        '<div class="profiles">'
        '<div class="profile">'
        f'<img class="player-image" alt="Player-Photo-{p1_id}" />'
        '</div><div class="profile">'
        f'<img class="player-image" alt="Player-Photo-{p2_id}" />'
        "</div></div>"
        '<div class="countries">'
        '<div class="country"><svg>'
        f'<use href="flags.svg#flag-{p1_country}"></use>'
        '</svg></div><div class="country"><svg>'
        f'<use href="flags.svg#flag-{p2_country}"></use>'
        "</svg></div></div>"
        '<div class="players"><div class="names">'
        '<div class="name">'
        f'<a href="/en/players/x/{p1_id}/overview">{p1_name}</a>'
        f"{seed_html}</div>"
        '<div class="name">'
        f'<a href="/en/players/x/{p2_id}/overview">{p2_name}</a>'
        "</div></div></div>"
        f"{winner_html}"
        f'</div><div class="scores">{scores}</div></div>'
    )


# Pre-built HTML fixtures

_DJOK_SCORES = _scores_html([(6, None), (7, None), (7, None)])
_TSIT_SCORES = _scores_html([(3, None), (6, 4), (6, 5)])

SINGLES_HTML = (
    "<html><body>"
    + _match_html(
        "Finals",
        _player_div(
            "D643", "Novak Djokovic", "srb",
            seed="(4)", winner=True, scores=_DJOK_SCORES,
        ),
        _player_div(
            "TE51", "Stefanos Tsitsipas", "gre",
            seed="(3)", scores=_TSIT_SCORES,
        ),
        duration="02:56",
        match_id="ms001",
    )
    + "</body></html>"
)

DOUBLES_HTML = (
    "<html><body>"
    + _match_html(
        "Finals",
        _doubles_team_div(
            "A853", "Marcelo Arevalo", "esa",
            "R513", "Jean-Julien Rojer", "ned",
            seed="(3)", winner=True,
            scores=_scores_html([(6, None), (7, None)]),
        ),
        _doubles_team_div(
            "B123", "Ivan Dodig", "cro",
            "C456", "Austin Krajicek", "usa",
            seed="(7)",
            scores=_scores_html([(4, None), (6, 5)]),
        ),
        duration="01:30",
        match_id="md001",
    )
    + "</body></html>"
)

WALKOVER_SINGLES_HTML = (
    "<html><body>"
    + _match_html(
        "Round of 32",
        _player_div(
            "D643", "Novak Djokovic", "srb", winner=True,
        ),
        _player_div("TE51", "Stefanos Tsitsipas", "gre"),
    )
    + "</body></html>"
)


def _walkover_placeholder_html(
    pid1: str,
    name1: str,
    country1: str,
    pid2: str = "0",
    name2: str = "Bye",
    country2: str = "",
) -> str:
    """Build walkover HTML with a placeholder opponent."""
    return _match_html(
        "Round of 32",
        _player_div(pid1, name1, country1, winner=True),
        _player_div(pid2, name2, country2),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_html(
    tmp_path: Path,
    tournament: Tournament,
    filename: str,
    html: str,
):
    raw_dir = tmp_path / "raw" / "atptour" / tournament.path
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / filename).write_text(html, encoding="utf-8")


def _make_tournament(**kwargs) -> Tournament:
    defaults = {
        "tournament_id": "580",
        "year": 2023,
        "circuit": Circuit.tour,
        "location": "Melbourne, Australia",
    }
    defaults.update(kwargs)
    return Tournament(**defaults)


# ---------------------------------------------------------------------------
# Tests: singles transform
# ---------------------------------------------------------------------------


class TestTransformSingles:
    def test_produces_parquet(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 1
        assert paths[0].exists()
        assert paths[0].name == "results.parquet"

    def test_correct_record_count(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 1

    def test_p1_p2_mapping(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["p1_id"] == "D643"
        assert row["p1_name"] == "Novak Djokovic"
        assert row["p2_id"] == "TE51"
        assert row["p2_name"] == "Stefanos Tsitsipas"

    def test_winner_id(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        # Djokovic won (player_won=True), winner_id = p1_id
        assert row["winner_id"] == "D643"

    def test_match_uid_computed(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["match_uid"] is not None
        assert row["match_uid"] == "2023_580_SGL_F_D643_TE51"

    def test_scores_flattened(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        # Djokovic: 6-3 7-6(4) 7-6(5)
        assert row["p1_set1_games"] == 6
        assert row["p1_set2_games"] == 7
        assert row["p1_set3_games"] == 7
        assert row["p1_set4_games"] is None
        assert row["p1_set5_games"] is None

        # Tsitsipas: 3, 6, 6
        assert row["p2_set1_games"] == 3
        assert row["p2_set2_games"] == 6
        assert row["p2_set3_games"] == 6
        assert row["p2_set4_games"] is None
        assert row["p2_set5_games"] is None

        # Tiebreaks: Tsitsipas had 6 games in sets 2-3 (loser)
        # p2_set2_tiebreak=4, p1_set2_tiebreak=max(7,4+2)=7
        assert row["p1_set2_tiebreak"] == 7
        assert row["p2_set2_tiebreak"] == 4
        # p2_set3_tiebreak=5, p1_set3_tiebreak=max(7,5+2)=7
        assert row["p1_set3_tiebreak"] == 7
        assert row["p2_set3_tiebreak"] == 5

        assert row["p1_set1_tiebreak"] is None
        assert row["p2_set1_tiebreak"] is None

    def test_metadata_fields(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["tournament_id"] == "580"
        assert row["year"] == 2023
        assert row["circuit"] == "tour"
        assert row["draw_type"] == "singles"
        assert row["result_type"] == "completed"
        assert "results_singles.html" in row["source_file"]
        assert row["parsed_at"] is not None

    def test_seed_entry_parsed(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["p1_seed"] == 4
        assert row["p1_entry"] is None
        assert row["p2_seed"] == 3
        assert row["p2_entry"] is None

    def test_country_uppercased(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["p1_country"] == "SRB"
        assert row["p2_country"] == "GRE"

    def test_duration_parsed(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        # 02:56 = 2*3600 + 56*60 = 10560
        assert row["duration_seconds"] == 10560

    def test_round_normalized(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["round"] == "F"

    def test_no_html_returns_empty(self, tmp_path):
        t = _make_tournament()
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert paths == []

    def test_empty_html_returns_empty(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_singles.html",
            "<html><body></body></html>",
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert paths == []

    def test_walkover_no_scores(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_singles.html",
            WALKOVER_SINGLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["result_type"] == "walkover"
        assert row["duration_seconds"] is None
        assert row["p1_set1_games"] is None
        assert row["p2_set1_games"] is None

    def test_singles_partner_fields_null(self, tmp_path):
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", SINGLES_HTML)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["p1_partner_id"] is None
        assert row["p1_partner_name"] is None
        assert row["p1_partner_country"] is None
        assert row["p2_partner_id"] is None
        assert row["p2_partner_name"] is None
        assert row["p2_partner_country"] is None


# ---------------------------------------------------------------------------
# Tests: doubles transform
# ---------------------------------------------------------------------------


class TestTransformDoubles:
    def test_doubles_produces_parquet(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 1
        df = pl.read_parquet(paths[0])
        assert len(df) == 1

    def test_doubles_p1_p2_mapping(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["p1_id"] == "A853"
        assert row["p1_name"] == "Marcelo Arevalo"
        assert row["p1_partner_id"] == "R513"
        assert row["p1_partner_name"] == "Jean-Julien Rojer"
        assert row["p2_id"] == "B123"
        assert row["p2_name"] == "Ivan Dodig"
        assert row["p2_partner_id"] == "C456"
        assert row["p2_partner_name"] == "Austin Krajicek"

    def test_doubles_winner_id(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["winner_id"] == "A853"

    def test_doubles_match_uid(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["match_uid"] is not None
        assert "DBL" in row["match_uid"]
        expected = "2023_580_DBL_F_A853_B123_C456_R513"
        assert row["match_uid"] == expected

    def test_doubles_draw_type(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["draw_type"] == "doubles"

    def test_doubles_country_uppercased(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["p1_country"] == "ESA"
        assert row["p1_partner_country"] == "NED"
        assert row["p2_country"] == "CRO"
        assert row["p2_partner_country"] == "USA"


# ---------------------------------------------------------------------------
# Tests: unified singles + doubles output
# ---------------------------------------------------------------------------


class TestUnifiedOutput:
    def test_singles_and_doubles_in_one_parquet(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_singles.html", SINGLES_HTML,
        )
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 1
        df = pl.read_parquet(paths[0])
        assert len(df) == 2
        draw_types = df["draw_type"].to_list()
        assert "singles" in draw_types
        assert "doubles" in draw_types


# ---------------------------------------------------------------------------
# Tests: dedup
# ---------------------------------------------------------------------------


class TestDedup:
    def test_duplicate_match_deduped(self, tmp_path):
        """Duplicate match divs => same match_uid => dedup to 1."""
        inner = SINGLES_HTML.replace(
            "<html><body>", ""
        ).replace("</body></html>", "")
        double_html = (
            "<html><body>" + inner + inner + "</body></html>"
        )
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_singles.html", double_html,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 1

    def test_null_match_uid_not_deduped(self, tmp_path):
        """Walkover with placeholder IDs: null uid, not deduped."""
        wo1 = _walkover_placeholder_html(
            "D643", "Novak Djokovic", "srb",
        )
        wo2 = _walkover_placeholder_html(
            "TE51", "Stefanos Tsitsipas", "gre",
        )
        html = "<html><body>" + wo1 + wo2 + "</body></html>"
        t = _make_tournament()
        _write_html(tmp_path, t, "results_singles.html", html)
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 2
        null_count = df.filter(
            pl.col("match_uid").is_null()
        ).height
        assert null_count == 2


# ---------------------------------------------------------------------------
# Tests: United Cup skip
# ---------------------------------------------------------------------------


class TestUnitedCupSkip:
    def test_united_cup_skips_doubles(self, tmp_path):
        """tid=9900 (United Cup) skips doubles processing."""
        t = _make_tournament(tournament_id="9900")
        _write_html(
            tmp_path, t, "results_singles.html", SINGLES_HTML,
        )
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        draw_types = df["draw_type"].to_list()
        assert all(d == "singles" for d in draw_types)

    def test_non_united_cup_processes_doubles(self, tmp_path):
        """Non-9900 tournaments process both draws."""
        t = _make_tournament(tournament_id="580")
        _write_html(
            tmp_path, t, "results_singles.html", SINGLES_HTML,
        )
        _write_html(
            tmp_path, t, "results_doubles.html", DOUBLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        draw_types = df["draw_type"].to_list()
        assert "singles" in draw_types
        assert "doubles" in draw_types


# ---------------------------------------------------------------------------
# Tests: _flatten_scores
# ---------------------------------------------------------------------------


class TestFlattenScores:
    def test_three_set_match(self):
        match = {
            "player_scores": [6, 7, 7],
            "opp_scores": [3, 6, 6],
            "player_tiebreaks": [None, 7, 7],
            "opp_tiebreaks": [None, 4, 5],
        }
        flat = ResultsTransformer._flatten_scores(match)
        assert flat["p1_set1_games"] == 6
        assert flat["p1_set2_games"] == 7
        assert flat["p1_set3_games"] == 7
        assert flat["p1_set4_games"] is None
        assert flat["p1_set5_games"] is None
        assert flat["p2_set1_games"] == 3
        assert flat["p2_set2_games"] == 6
        assert flat["p2_set3_games"] == 6
        assert flat["p2_set4_games"] is None
        assert flat["p2_set5_games"] is None
        assert flat["p1_set1_tiebreak"] is None
        assert flat["p1_set2_tiebreak"] == 7
        assert flat["p1_set3_tiebreak"] == 7
        assert flat["p2_set2_tiebreak"] == 4
        assert flat["p2_set3_tiebreak"] == 5

    def test_five_set_match(self):
        match = {
            "player_scores": [6, 4, 6, 3, 7],
            "opp_scores": [4, 6, 3, 6, 6],
            "player_tiebreaks": [None, None, None, None, 7],
            "opp_tiebreaks": [None, None, None, None, 5],
        }
        flat = ResultsTransformer._flatten_scores(match)
        assert flat["p1_set5_games"] == 7
        assert flat["p2_set5_games"] == 6
        assert flat["p1_set5_tiebreak"] == 7
        assert flat["p2_set5_tiebreak"] == 5

    def test_walkover_empty_scores(self):
        match = {
            "player_scores": [],
            "opp_scores": [],
            "player_tiebreaks": [],
            "opp_tiebreaks": [],
        }
        flat = ResultsTransformer._flatten_scores(match)
        for i in range(1, 6):
            assert flat[f"p1_set{i}_games"] is None
            assert flat[f"p2_set{i}_games"] is None
            assert flat[f"p1_set{i}_tiebreak"] is None
            assert flat[f"p2_set{i}_tiebreak"] is None

    def test_two_set_match(self):
        match = {
            "player_scores": [6, 6],
            "opp_scores": [3, 4],
            "player_tiebreaks": [None, None],
            "opp_tiebreaks": [None, None],
        }
        flat = ResultsTransformer._flatten_scores(match)
        assert flat["p1_set1_games"] == 6
        assert flat["p1_set2_games"] == 6
        assert flat["p1_set3_games"] is None
        assert flat["p2_set1_games"] == 3
        assert flat["p2_set2_games"] == 4
        assert flat["p2_set3_games"] is None


# ---------------------------------------------------------------------------
# Tests: match_id passthrough
# ---------------------------------------------------------------------------


class TestMatchId:
    def test_match_id_passed_through(self, tmp_path):
        t = _make_tournament()
        _write_html(
            tmp_path, t, "results_singles.html", SINGLES_HTML,
        )
        xf = ResultsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["match_id"] == "ms001"
