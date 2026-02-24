"""Tests for ScheduleTransformer — raw HTML to staged parquet."""

from datetime import date, datetime
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

from mvp.atptour.transformers.schedule import (
    ScheduleTransformer,
    _normalize_score,
    _parse_schedule_html,
    _parse_snapshot_timestamp,
)
from mvp.common.enums import Circuit


def _flag(code):
    return (
        f'<svg class="atp-flag">'
        f'<use href="/assets/flags.svg#flag-{code}" />'
        f"</svg>"
    )


def _player_link(slug, pid, first, last):
    return (
        f'<a href="/en/players/{slug}/{pid}/overview">'
        f"<span>{first}</span> {last}</a>"
    )


def _singles_div(
    matchdate="2026-02-07",
    dt_attr="2026-02-07 14:00:00",
    suffix="Not Before",
    displaytime="Not Before 3:00 PM",
    court=None,
    round_str="SF",
    p1_flag="fra",
    p1_slug="adrian-mannarino",
    p1_id="me82",
    p1_first="A.",
    p1_last="Mannarino",
    p1_rank="(1)",
    p2_flag="usa",
    p2_slug="martin-damm",
    p2_id="d0dt",
    p2_first="M.",
    p2_last="Damm",
    p2_rank="(Q)",
    status="Vs",
    score="&ndash;&ndash;&ndash;",
):
    court_html = ""
    if court:
        court_html = (
            f"<span><strong>{court}</strong></span>"
        )
    return (
        f'<div class="schedule"'
        f' data-matchdate="{matchdate}"'
        f' data-datetime="{dt_attr}"'
        f' data-suffix="{suffix}"'
        f' data-displaytime="{displaytime}">'
        f'<div class="schedule-header">'
        f'<div class="schedule-location-timestamp">'
        f"{court_html}"
        f'<span class="matchtime">{displaytime}</span>'
        f"</div>"
        f'<div class="schedule-type">{round_str}</div>'
        f"</div>"
        f'<div class="schedule-content">'
        f'<div class="schedule-type">{round_str}</div>'
        f'<div class="schedule-players">'
        f'<div class="player">'
        f'<div class="country">{_flag(p1_flag)}</div>'
        f'<div class="name">'
        f"{_player_link(p1_slug, p1_id, p1_first, p1_last)}"
        f'<div class="rank"><span>{p1_rank}</span></div>'
        f"</div></div>"
        f'<div class="status">{status}</div>'
        f'<div class="opponent">'
        f'<div class="country">{_flag(p2_flag)}</div>'
        f'<div class="name">'
        f'<div class="rank"><span>{p2_rank}</span></div>'
        f"{_player_link(p2_slug, p2_id, p2_first, p2_last)}"
        f"</div></div>"
        f"</div>"
        f'<div class="schedule-cta">'
        f'<span class="schedule-cta-score">{score}</span>'
        f"</div></div></div>"
    )


def _doubles_div(
    matchdate="2026-02-07",
    dt_attr="2026-02-07 11:30:00",
    suffix="Starts At",
    displaytime="Starts At 12:30 PM",
    court="Court Patrice Dominguez",
    round_str="SF",
    status="Defeats",
    score="76<sup>6</sup> 61",
):
    court_html = ""
    if court:
        court_html = (
            f"<span><strong>{court}</strong></span>"
        )
    p1_names = (
        '<div class="names">'
        '<div class="name">'
        + _player_link(
            "constantin-frantzen", "f09r", "C.", "Frantzen"
        )
        + "</div>"
        '<div class="name">'
        + _player_link(
            "robin-haase", "h756", "R.", "Haase"
        )
        + "</div></div>"
    )
    p2_names = (
        '<div class="names">'
        '<div class="name">'
        + _player_link(
            "jakob-schnaitter", "sy05", "J.", "Schnaitter"
        )
        + "</div>"
        '<div class="name">'
        + _player_link(
            "mark-wallner", "w0e2", "M.", "Wallner"
        )
        + "</div></div>"
    )
    return (
        f'<div class="schedule"'
        f' data-matchdate="{matchdate}"'
        f' data-datetime="{dt_attr}"'
        f' data-suffix="{suffix}"'
        f' data-displaytime="{displaytime}">'
        f'<div class="schedule-header">'
        f'<div class="schedule-location-timestamp">'
        f"{court_html}"
        f'<span class="matchtime">{displaytime}</span>'
        f"</div>"
        f'<div class="schedule-type">{round_str}</div>'
        f"</div>"
        f'<div class="schedule-content">'
        f'<div class="schedule-type">{round_str}</div>'
        f'<div class="schedule-players">'
        f'<div class="player">'
        f'<div class="profiles">'
        f'<div class="profile"><img src="p1a"/></div>'
        f'<div class="profile"><img src="p1b"/></div>'
        f"</div>"
        f'<div class="countries">'
        f'<div class="country">{_flag("ger")}</div>'
        f'<div class="country">{_flag("ned")}</div>'
        f"</div>"
        f'<div class="players">'
        f"{p1_names}"
        f'<div class="rank"><span>(3)</span></div>'
        f"</div></div>"
        f'<div class="status">{status}</div>'
        f'<div class="opponent">'
        f'<div class="profiles">'
        f'<div class="profile"><img src="p2a"/></div>'
        f'<div class="profile"><img src="p2b"/></div>'
        f"</div>"
        f'<div class="countries">'
        f'<div class="country">{_flag("ger")}</div>'
        f'<div class="country">{_flag("aut")}</div>'
        f"</div>"
        f'<div class="players">'
        f'<div class="rank"><span>(2)</span></div>'
        f"{p2_names}"
        f"</div></div>"
        f"</div>"
        f'<div class="schedule-cta">'
        f'<span class="schedule-cta-score">{score}</span>'
        f"</div></div></div>"
    )


def _wrap(*divs):
    return "<html><body>" + "".join(divs) + "</body></html>"


FIXTURE_SINGLES = _wrap(_singles_div())

FIXTURE_DOUBLES = _wrap(_doubles_div())

FIXTURE_NO_COURT = _wrap(
    _singles_div(
        court=None,
        round_str="F",
        p1_rank="",
        p2_rank="",
    )
)

FIXTURE_FOLLOWED_BY = _wrap(
    _singles_div(
        dt_attr="",
        suffix="Followed By",
        displaytime="Followed By",
        court="Center Court",
        round_str="QF",
        p1_flag="esp",
        p1_slug="carlos-alcaraz",
        p1_id="a0e2",
        p1_first="C.",
        p1_last="Alcaraz",
        p1_rank="(1)",
        p2_flag="ita",
        p2_slug="jannik-sinner",
        p2_id="s0ag",
        p2_first="J.",
        p2_last="Sinner",
        p2_rank="(2)",
    )
)


def _score_html(inner):
    return (
        '<span class="schedule-cta-score">'
        f"{inner}</span>"
    )


def _parse_score_span(inner):
    soup = BeautifulSoup(_score_html(inner), "lxml")
    return soup.select_one("span.schedule-cta-score")


def _parse_fixture(html, **kwargs):
    defaults = {
        "tournament_id": "339",
        "year": 2026,
        "circuit": Circuit.tour,
        "snapshot_timestamp": datetime(2026, 2, 7, 14, 0, 0),
        "source_file": "test.html",
        "parsed_at": datetime(2026, 2, 24),
    }
    defaults.update(kwargs)
    return _parse_schedule_html(html, **defaults)


class TestParseSnapshotTimestamp:
    def test_basic(self):
        ts = "schedule_20260207_140000"
        result = _parse_snapshot_timestamp(ts)
        assert result == datetime(2026, 2, 7, 14, 0, 0)

    def test_midnight(self):
        ts = "schedule_20260101_000000"
        result = _parse_snapshot_timestamp(ts)
        assert result == datetime(2026, 1, 1, 0, 0, 0)

    def test_end_of_day(self):
        ts = "schedule_20260207_235959"
        result = _parse_snapshot_timestamp(ts)
        assert result == datetime(2026, 2, 7, 23, 59, 59)


class TestNormalizeScore:
    def test_tiebreak(self):
        span = _parse_score_span("76<sup>6</sup> 61")
        assert _normalize_score(span) == "76(6) 61"

    def test_multiple_tiebreaks(self):
        inner = (
            "67<sup>4</sup> "
            "76<sup>5</sup> "
            "76<sup>3</sup>"
        )
        span = _parse_score_span(inner)
        assert _normalize_score(span) == "67(4) 76(5) 76(3)"

    def test_no_tiebreak(self):
        span = _parse_score_span("64 75")
        assert _normalize_score(span) == "64 75"

    def test_ndash_returns_none(self):
        span = _parse_score_span("\u2013\u2013\u2013")
        assert _normalize_score(span) is None

    def test_empty_returns_none(self):
        span = _parse_score_span("")
        assert _normalize_score(span) is None


class TestParseSinglesMatch:
    def test_parse_singles(self):
        records = _parse_fixture(FIXTURE_SINGLES)
        assert len(records) == 1
        r = records[0]
        assert r.tournament_id == "339"
        assert r.year == 2026
        assert r.circuit == Circuit.tour
        assert r.match_date == date(2026, 2, 7)
        assert r.scheduled_datetime == datetime(
            2026, 2, 7, 14, 0, 0
        )
        assert r.time_suffix == "Not Before"
        assert r.display_time == "Not Before 3:00 PM"
        assert r.court_name is None  # default has no court
        assert r.round == "SF"
        assert r.p1_id == "ME82"
        assert r.p1_name == "A. Mannarino"
        assert r.p1_country == "FRA"
        assert r.p1_seed_entry == "(1)"
        assert r.p2_id == "D0DT"
        assert r.p2_name == "M. Damm"
        assert r.p2_country == "USA"
        assert r.p2_seed_entry == "(Q)"
        assert r.status == "Vs"
        assert r.score is None


class TestParseDoublesMatch:
    def test_parse_doubles(self):
        records = _parse_fixture(
            FIXTURE_DOUBLES,
            snapshot_timestamp=datetime(
                2026, 2, 7, 11, 30, 0
            ),
        )
        assert len(records) == 1
        r = records[0]
        assert r.p1_id == "F09R"
        assert r.p1_name == "C. Frantzen / R. Haase"
        assert r.p1_country == "GER"
        assert r.p1_seed_entry == "(3)"
        assert r.p2_id == "SY05"
        assert r.p2_name == (
            "J. Schnaitter / M. Wallner"
        )
        assert r.p2_country == "GER"
        assert r.p2_seed_entry == "(2)"
        assert r.status == "Defeats"
        assert r.score == "76(6) 61"
        assert r.court_name == "Court Patrice Dominguez"


class TestParseScoreWithTiebreak:
    def test_tiebreak_in_context(self):
        records = _parse_fixture(FIXTURE_DOUBLES)
        assert records[0].score == "76(6) 61"


class TestParseUnplayedScore:
    def test_ndash_score(self):
        records = _parse_fixture(FIXTURE_SINGLES)
        assert records[0].score is None


class TestParseCourtName:
    def test_court_present(self):
        html = _wrap(
            _singles_div(court="Center Court")
        )
        records = _parse_fixture(html)
        assert records[0].court_name == "Center Court"

    def test_no_court(self):
        records = _parse_fixture(FIXTURE_NO_COURT)
        assert records[0].court_name is None


class TestParseSeedEntry:
    def test_seed_number(self):
        records = _parse_fixture(FIXTURE_SINGLES)
        assert records[0].p1_seed_entry == "(1)"

    def test_qualifier_entry(self):
        records = _parse_fixture(FIXTURE_SINGLES)
        assert records[0].p2_seed_entry == "(Q)"

    def test_empty_seed(self):
        records = _parse_fixture(FIXTURE_NO_COURT)
        assert records[0].p1_seed_entry is None
        assert records[0].p2_seed_entry is None


class TestParseFollowedBy:
    def test_followed_by_null_datetime(self):
        records = _parse_fixture(FIXTURE_FOLLOWED_BY)
        assert len(records) == 1
        r = records[0]
        assert r.scheduled_datetime is None
        assert r.time_suffix == "Followed By"
        assert r.display_time == "Followed By"


def _make_tournament():
    """Create a test Tournament object."""
    from mvp.atptour.tournament import Tournament

    return Tournament(
        tournament_id="339",
        year=2026,
        circuit=Circuit.tour,
        location="Marseille, France",
    )


def _write_schedule_html(
    tmp_path: Path, filename: str, html: str
) -> None:
    raw_dir = (
        tmp_path
        / "raw"
        / "atptour"
        / "tournaments"
        / "tour"
        / "339"
        / "2026"
        / "schedule"
    )
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / filename).write_text(
        html, encoding="utf-8"
    )


class TestFullTransformerRun:
    def test_produces_parquet(self, tmp_path):
        _write_schedule_html(
            tmp_path,
            "schedule_20260207_140000.html",
            FIXTURE_SINGLES,
        )
        tournament = _make_tournament()
        xf = ScheduleTransformer(
            tournament, data_root=tmp_path
        )
        result = xf.run()
        assert result is not None
        assert result.name == "schedule.parquet"
        df = pl.read_parquet(result)
        assert len(df) == 1

    def test_field_values_in_parquet(self, tmp_path):
        _write_schedule_html(
            tmp_path,
            "schedule_20260207_140000.html",
            FIXTURE_SINGLES,
        )
        tournament = _make_tournament()
        xf = ScheduleTransformer(
            tournament, data_root=tmp_path
        )
        result = xf.run()
        df = pl.read_parquet(result)
        row = df.row(0, named=True)
        assert row["tournament_id"] == "339"
        assert row["p1_id"] == "ME82"
        assert row["p2_id"] == "D0DT"
        assert row["round"] == "SF"

    def test_multiple_matches(self, tmp_path):
        html = _wrap(
            _singles_div(),
            _singles_div(round_str="F"),
        )
        _write_schedule_html(
            tmp_path,
            "schedule_20260207_140000.html",
            html,
        )
        tournament = _make_tournament()
        xf = ScheduleTransformer(
            tournament, data_root=tmp_path
        )
        result = xf.run()
        df = pl.read_parquet(result)
        assert len(df) == 2

    def test_multiple_files(self, tmp_path):
        _write_schedule_html(
            tmp_path,
            "schedule_20260207_140000.html",
            FIXTURE_SINGLES,
        )
        _write_schedule_html(
            tmp_path,
            "schedule_20260208_100000.html",
            FIXTURE_NO_COURT,
        )
        tournament = _make_tournament()
        xf = ScheduleTransformer(
            tournament, data_root=tmp_path
        )
        result = xf.run()
        df = pl.read_parquet(result)
        assert len(df) == 2

    def test_snapshot_timestamps_differ(self, tmp_path):
        _write_schedule_html(
            tmp_path,
            "schedule_20260207_140000.html",
            FIXTURE_SINGLES,
        )
        _write_schedule_html(
            tmp_path,
            "schedule_20260208_100000.html",
            FIXTURE_NO_COURT,
        )
        tournament = _make_tournament()
        xf = ScheduleTransformer(
            tournament, data_root=tmp_path
        )
        result = xf.run()
        df = pl.read_parquet(result)
        timestamps = df["snapshot_timestamp"].to_list()
        assert timestamps[0] != timestamps[1]


class TestEmptyScheduleDir:
    def test_no_files_returns_none(self, tmp_path):
        tournament = _make_tournament()
        xf = ScheduleTransformer(
            tournament, data_root=tmp_path
        )
        result = xf.run()
        assert result is None

    def test_empty_dir_returns_none(self, tmp_path):
        raw_dir = (
            tmp_path
            / "raw"
            / "atptour"
            / "tournaments"
            / "tour"
            / "339"
            / "2026"
            / "schedule"
        )
        raw_dir.mkdir(parents=True, exist_ok=True)
        tournament = _make_tournament()
        xf = ScheduleTransformer(
            tournament, data_root=tmp_path
        )
        result = xf.run()
        assert result is None
