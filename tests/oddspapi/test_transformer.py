"""raw/oddspapi -> stage/oddspapi/ticks -> aggregate/oddspapi/quotes.parquet.

The ticks tree is the record: every tick captured is staged, with `active` and
`event_status` as columns. Filtering there would make the dropped universe
unrecoverable without a full reparse — the mistake made one layer up, where the
backtest CSV gated at emission and the offered-line universe was lost.

The quotes aggregate is derived: prematch only, reduced to open/formed/close.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from mvp.oddspapi import markets, parser, transformer

UTC = timezone.utc


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "dataroot"
    monkeypatch.setenv("MVP_DATA_ROOT", str(root))
    from mvp.oddspapi import paths

    paths.ensure_dirs()
    return root


def _reference(tmp_root):
    """Two markets: a two-sided total with a handicap, and a moneyline."""
    from mvp.oddspapi import paths

    ref = [
        {
            "marketId": 900, "marketType": "totals-games", "period": "result",
            "handicap": 21.5,
            "outcomes": [{"outcomeId": 9001, "outcomeName": "Over"},
                         {"outcomeId": 9002, "outcomeName": "Under"}],
        },
        {
            "marketId": 121, "marketType": "moneyline", "period": "result",
            "handicap": 0,
            "outcomes": [{"outcomeId": 121, "outcomeName": "1"},
                         {"outcomeId": 122, "outcomeName": "2"}],
        },
    ]
    paths.markets_reference().write_text(json.dumps(ref), encoding="utf-8")


def _tick(ts: datetime, price: float, active: bool = True, limit=None):
    return {"createdAt": ts.isoformat().replace("+00:00", "Z"), "price": price,
            "limit": limit, "active": active, "exchangeMeta": None}


def _write_raw(fixture_id: str, ticks_by_market: dict, book: str = "pinnacle",
               group: int = 0):
    from mvp.oddspapi import paths

    markets_payload = {}
    for mid, per_outcome in ticks_by_market.items():
        markets_payload[str(mid)] = {
            "outcomes": {
                str(oid): {"players": {"0": arr}}
                for oid, arr in per_outcome.items()
            }
        }
    payload = {"fixtureId": fixture_id, "bookmakers": {book: {"markets": markets_payload}}}
    d = paths.historical_dirs()[group]
    d.mkdir(parents=True, exist_ok=True)
    (d / f"historical_{fixture_id}.json").write_text(json.dumps(payload), encoding="utf-8")


def _touch_raw(fixture_id: str, ahead: float = 10.0, group: int = 0):
    """Make a rewritten capture unambiguously newer than its staged parquet.

    Filesystem timestamp granularity, not the code, is what would otherwise make
    this flaky.
    """
    from mvp.oddspapi import paths

    p = paths.historical_dirs()[group] / f"historical_{fixture_id}.json"
    t = p.stat().st_mtime + ahead
    os.utime(p, (t, t))


def _ticks(market: str | None = None) -> pl.DataFrame:
    from mvp.oddspapi import paths

    df = pl.concat([pl.read_parquet(p)
                    for p in sorted(paths.ticks_dir().glob("*/*.parquet"))])
    return df.filter(pl.col("market") == market) if market else df


def _quotes(market: str | None = None) -> pl.DataFrame:
    from mvp.oddspapi import paths

    df = pl.read_parquet(paths.quotes_path())
    return df.filter(pl.col("market") == market) if market else df


def _snapshots(book: str, market: str) -> pl.DataFrame:
    from mvp.oddspapi import paths

    return pl.read_parquet(paths.market_path(book, market))


def _fake_crosswalk(monkeypatch, fixture_id, start, true_start):
    xw = pl.DataFrame([{
        "fixture_id": fixture_id, "match_uid": "2026_1_SGL_R32_AA_BB",
        "p1_id": "AA", "p2_id": "BB",
        "p1_book_name": "A A", "p2_book_name": "B B",
        "start_time": start, "true_start_time": true_start, "status_id": 2,
    }], schema={
        "fixture_id": pl.Utf8, "match_uid": pl.Utf8, "p1_id": pl.Utf8,
        "p2_id": pl.Utf8, "p1_book_name": pl.Utf8, "p2_book_name": pl.Utf8,
        "start_time": pl.Datetime("us", "UTC"),
        "true_start_time": pl.Datetime("us", "UTC"), "status_id": pl.Int64,
    })
    monkeypatch.setattr(transformer.matcher, "build", lambda *a, **k: xw)


SCHED = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
TRUE = datetime(2026, 6, 1, 13, 0, tzinfo=UTC)     # an hour late


def _fake_crosswalk_many(monkeypatch, fixture_ids):
    xw = pl.DataFrame([
        {
            "fixture_id": fid, "match_uid": f"2026_1_SGL_R32_{fid}",
            "p1_id": "AA", "p2_id": "BB",
            "p1_book_name": "A A", "p2_book_name": "B B",
            "start_time": SCHED, "true_start_time": TRUE, "status_id": 2,
        }
        for fid in fixture_ids
    ], schema={
        "fixture_id": pl.Utf8, "match_uid": pl.Utf8, "p1_id": pl.Utf8,
        "p2_id": pl.Utf8, "p1_book_name": pl.Utf8, "p2_book_name": pl.Utf8,
        "start_time": pl.Datetime("us", "UTC"),
        "true_start_time": pl.Datetime("us", "UTC"), "status_id": pl.Int64,
    })
    monkeypatch.setattr(transformer.matcher, "build", lambda *a, **k: xw)


class TestParser:
    def test_extracts_every_market_and_outcome(self, data_root, tmp_path):
        _reference(data_root)
        _write_raw("f1", {
            900: {9001: [_tick(SCHED - timedelta(hours=2), 1.9)],
                  9002: [_tick(SCHED - timedelta(hours=2), 2.0)]},
            121: {121: [_tick(SCHED - timedelta(hours=2), 1.5)],
                  122: [_tick(SCHED - timedelta(hours=2), 2.6)]},
        })
        from mvp.oddspapi import paths

        f = next(paths.historical_dirs()[0].glob("*.json"))
        fid, ticks = parser.parse_file(f, markets.load_index())
        assert fid == "f1"
        assert len(ticks) == 4
        assert {t.stage_name for t in ticks} == {"total_games", "moneyline"}
        assert {t.side for t in ticks} == {"Over", "Under", "1", "2"}

    def test_unreadable_file_is_skipped_not_raised(self, data_root):
        _reference(data_root)
        from mvp.oddspapi import paths

        p = paths.historical_dirs()[0] / "historical_broken.json"
        p.write_text("{not json", encoding="utf-8")
        assert parser.parse_file(p, markets.load_index()) == (None, [])

    def test_handicap_comes_from_the_reference_not_the_tick(self, data_root):
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        from mvp.oddspapi import paths

        f = next(paths.historical_dirs()[0].glob("*.json"))
        _, ticks = parser.parse_file(f, markets.load_index())
        assert {t.points for t in ticks} == {21.5}


class TestEventStatus:
    """Derived on the ticks, and it decides what the reduction can see."""

    def _run(self, monkeypatch, true_start):
        _fake_crosswalk(monkeypatch, "f1", SCHED, true_start)
        return transformer.transform()

    def test_boundary_is_the_true_start_not_the_schedule(self, data_root, monkeypatch):
        """48% of fixtures start >5 min late; cutting on the schedule files
        genuinely prematch quotes as in-play, which would drop them from quotes."""
        _reference(data_root)
        # 12:30 is after the scheduled start but before the real one.
        _write_raw("f1", {900: {
            9001: [_tick(datetime(2026, 6, 1, 12, 30, tzinfo=UTC), 1.9)],
            9002: [_tick(datetime(2026, 6, 1, 12, 30, tzinfo=UTC), 2.0)],
        }})
        self._run(monkeypatch, TRUE)
        assert len(_quotes("total_games")) == 2

    def test_falls_back_to_scheduled_start_when_true_is_missing(
        self, data_root, monkeypatch
    ):
        _reference(data_root)
        _write_raw("f1", {900: {
            9001: [_tick(datetime(2026, 6, 1, 12, 30, tzinfo=UTC), 1.9)],
            9002: [_tick(datetime(2026, 6, 1, 12, 30, tzinfo=UTC), 2.0)],
        }})
        self._run(monkeypatch, None)
        # In-play by the scheduled boundary, so nothing reaches the aggregate —
        # but the ticks survive.
        assert len(_quotes()) == 0
        assert len(_ticks("total_games")) == 2


class TestNothingIsFilteredFromTheRecord:
    def test_in_play_ticks_are_staged(self, data_root, monkeypatch):
        """The chain prices from any score, so in-play is the market it has the
        strongest claim on. Dropping it here costs a reparse to undo."""
        _reference(data_root)
        _write_raw("f1", {900: {
            9001: [_tick(SCHED - timedelta(hours=1), 1.9),
                   _tick(TRUE + timedelta(minutes=30), 3.4)],
            9002: [_tick(SCHED - timedelta(hours=1), 2.0),
                   _tick(TRUE + timedelta(minutes=30), 1.3)],
        }})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()
        assert len(_ticks("total_games")) == 4

    def test_inactive_quotes_are_staged_as_a_column(self, data_root, monkeypatch):
        _reference(data_root)
        _write_raw("f1", {900: {
            9001: [_tick(SCHED - timedelta(hours=1), 1.9, active=False)],
            9002: [_tick(SCHED - timedelta(hours=1), 2.0, active=True)],
        }})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()
        assert sorted(_ticks("total_games")["active"].to_list()) == [False, True]

    def test_ticks_carry_no_match_uid(self, data_root, monkeypatch):
        """Matching at reduce time, not parse time: a one-line alias addition must
        not cost a 35 GB reparse, and unresolved fixtures must stay recoverable."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()
        assert "match_uid" not in _ticks().columns


class TestQuotes:
    """The reduction: prematch only, open/formed/close per line and side."""

    def _two_book_total(self, data_root, monkeypatch):
        """Two entry books plus the reference book, quoting at three instants."""
        _reference(data_root)
        t0 = SCHED - timedelta(hours=3)
        t1 = SCHED - timedelta(hours=2)
        t2 = SCHED - timedelta(hours=1)
        # betrivers is an entry book in group 0 alongside pinnacle.
        _write_raw("f1", {900: {9001: [_tick(t0, 1.80), _tick(t2, 1.95)],
                                9002: [_tick(t0, 2.05), _tick(t2, 1.90)]}},
                   book="betrivers", group=0)
        # A second entry book, in the other capture group, joining later.
        _write_raw("f1", {900: {9001: [_tick(t1, 1.85), _tick(t2, 2.00)],
                                9002: [_tick(t1, 2.00), _tick(t2, 1.85)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        return transformer.transform()

    def test_one_row_per_line_side_and_role(self, data_root, monkeypatch):
        self._two_book_total(data_root, monkeypatch)
        q = _quotes("total_games")
        assert set(q["side"].to_list()) == {"Over", "Under"}
        assert q["role"].unique().to_list() == ["entry"]
        assert q["points"].unique().to_list() == [21.5]
        assert len(q) == 2

    def test_open_is_the_earliest_instant_close_the_latest(
        self, data_root, monkeypatch
    ):
        """Only betrivers is on the board at the open, so the opening price is
        its own — not a blend with a book that had not posted yet."""
        self._two_book_total(data_root, monkeypatch)
        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["best_opening_odds"] == 1.80
        assert over["best_closing_odds"] == 2.00     # max across both at the last instant
        assert over["n_books"] == 2

    def test_formed_needs_two_entry_books(self, data_root, monkeypatch):
        """Formed is the first shoppable moment; one book on the board is not it.

        betrivers quotes at t0 and does not move again until t2, so the bucket where
        fanduel arrives contains no betrivers TICK — only its standing price. That
        bucket is the first two-books-deep moment, and reading it correctly is what
        _snapshots_from_ticks exists for.
        """
        self._two_book_total(data_root, monkeypatch)
        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["formed_odds"] == 1.85          # max(1.80 standing, 1.85 new)

    def test_reference_and_entry_books_stay_separate(self, data_root, monkeypatch):
        """Pinnacle is the de-vig reference for CLV, not a bettable price — maxing
        it together with retail would quote a number no bet can be placed at."""
        _reference(data_root)
        t0 = SCHED - timedelta(hours=2)
        _write_raw("f1", {900: {9001: [_tick(t0, 1.75)], 9002: [_tick(t0, 2.10)]}},
                   book="pinnacle", group=0)
        _write_raw("f1", {900: {9001: [_tick(t0, 1.95)], 9002: [_tick(t0, 1.90)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        q = _quotes("total_games")
        assert sorted(q["role"].unique().to_list()) == ["entry", "reference"]
        ref = q.filter((pl.col("role") == "reference") & (pl.col("side") == "Over"))
        ent = q.filter((pl.col("role") == "entry") & (pl.col("side") == "Over"))
        assert ref["best_opening_odds"].item() == 1.75
        assert ent["best_opening_odds"].item() == 1.95
        # A lone reference book still gets a formed price; requiring two would null
        # every one of them.
        assert ref["formed_odds"].item() == 1.75


class TestSnapshotLayer:
    """Per-book prices at every instant, kept rather than spent on the reduction.

    quotes.parquet answers "what was the best price" at three moments. It cannot
    answer WHICH book held it or what the board looked like at any other moment,
    because best-across spends book identity and open/formed/close spend the rest.
    Both questions decide real bets, so the layer they are answerable from is the
    one that gets persisted; quotes stays derived.
    """

    def _two_book_total(self, data_root, monkeypatch):
        return TestQuotes()._two_book_total(data_root, monkeypatch)

    def test_one_file_per_book_and_market(self, data_root, monkeypatch):
        self._two_book_total(data_root, monkeypatch)
        for book in ("betrivers", "fanduel"):
            snaps = _snapshots(book, "total_games")
            assert snaps["book"].unique().to_list() == [book]
            assert snaps["market"].unique().to_list() == ["total_games"]

    def test_columns_are_the_scraper_stage_contract(self, data_root, monkeypatch):
        """`stage/betrivers/total_games.parquet` reads the same, minus the event id
        the crosswalk already resolved."""
        self._two_book_total(data_root, monkeypatch)
        assert _snapshots("betrivers", "total_games").columns == [
            "match_uid", "p1_id", "p2_id", "market", "points", "side", "book",
            "odds", "fetched_at", "event_status",
        ]

    def test_the_best_price_is_attributable_to_a_book(self, data_root, monkeypatch):
        """The question quotes cannot answer. Both books are on the board at the
        close at different prices; the aggregate keeps 2.00 and n_books=2, which
        does not tell you where to place the bet."""
        self._two_book_total(data_root, monkeypatch)
        close = SCHED - timedelta(hours=1)
        at_close = {
            book: _snapshots(book, "total_games").filter(
                (pl.col("side") == "Over") & (pl.col("fetched_at") == close)
            )["odds"].item()
            for book in ("betrivers", "fanduel")
        }
        assert at_close == {"betrivers": 1.95, "fanduel": 2.00}
        assert _quotes("total_games").filter(
            pl.col("side") == "Over"
        )["best_closing_odds"].item() == 2.00

    def test_a_standing_price_is_present_where_it_did_not_tick(
        self, data_root, monkeypatch
    ):
        """betrivers quotes at t0 and does not move again until t2, but it was on
        the board throughout — the fill is what makes `n_books` mean books quoting
        rather than books that happened to move."""
        self._two_book_total(data_root, monkeypatch)
        middle = SCHED - timedelta(hours=2)
        row = _snapshots("betrivers", "total_games").filter(
            (pl.col("side") == "Over") & (pl.col("fetched_at") == middle)
        )
        assert row["odds"].item() == 1.80

    def test_in_play_is_absent_here_and_present_in_the_ticks(
        self, data_root, monkeypatch
    ):
        """Prematch only, like the reduction it feeds. The ticks stay the record."""
        _reference(data_root)
        _write_raw("f1", {900: {
            9001: [_tick(SCHED - timedelta(hours=1), 1.9),
                   _tick(TRUE + timedelta(minutes=30), 3.4)],
            9002: [_tick(SCHED - timedelta(hours=1), 2.0),
                   _tick(TRUE + timedelta(minutes=30), 1.3)],
        }}, book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        assert len(_ticks("total_games")) == 4
        snaps = _snapshots("fanduel", "total_games")
        assert snaps["event_status"].unique().to_list() == ["NOT_STARTED"]
        assert len(snaps) == 2

    def test_rerunning_replaces_the_layer_rather_than_appending(
        self, data_root, monkeypatch
    ):
        """The reduce rebuilds every match each run, so parts left by a previous
        run would land in the next one's files as duplicates."""
        self._two_book_total(data_root, monkeypatch)
        before = len(_snapshots("betrivers", "total_games"))
        transformer.transform()
        assert len(_snapshots("betrivers", "total_games")) == before

    def test_non_line_markets_reduce_too(self, data_root, monkeypatch):
        """Moneyline carries handicap 0.0 in the reference, so `points` is 0.0 and
        not null on real data — but the joins key on it, and polars drops null keys,
        so they pass nulls_equal for any market whose reference omits a handicap."""
        _reference(data_root)
        t0, t1 = SCHED - timedelta(hours=2), SCHED - timedelta(hours=1)
        _write_raw("f1", {121: {121: [_tick(t0, 1.50)], 122: [_tick(t0, 2.60)]}},
                   book="betrivers", group=0)
        _write_raw("f1", {121: {121: [_tick(t1, 1.55)], 122: [_tick(t1, 2.50)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        q = _quotes("moneyline").filter(pl.col("role") == "entry")
        assert len(q) == 2
        assert q["points"].unique().to_list() == [0.0]
        assert q["formed_odds"].null_count() == 0
        assert q["n_books"].to_list() == [2, 2]

    def test_null_points_market_keeps_its_formed_price(self, data_root, monkeypatch):
        """The nulls_equal guard, exercised directly: a reference entry with no
        handicap yields null points, and a null-dropping join would have returned
        formed_odds as null while group_by grouped the nulls together."""
        from mvp.oddspapi import paths

        paths.markets_reference().write_text(json.dumps([{
            "marketId": 700, "marketType": "winner-set1", "period": "result",
            "outcomes": [{"outcomeId": 7001, "outcomeName": "1"},
                         {"outcomeId": 7002, "outcomeName": "2"}],
        }]), encoding="utf-8")
        t0, t1 = SCHED - timedelta(hours=2), SCHED - timedelta(hours=1)
        _write_raw("f1", {700: {7001: [_tick(t0, 1.60)], 7002: [_tick(t0, 2.40)]}},
                   book="betrivers", group=0)
        _write_raw("f1", {700: {7001: [_tick(t1, 1.65)], 7002: [_tick(t1, 2.30)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        q = _quotes().filter(pl.col("role") == "entry")
        assert q["points"].unique().to_list() == [None]
        assert q["formed_odds"].null_count() == 0
        assert q["n_books"].to_list() == [2, 2]

    def test_in_play_ticks_do_not_reach_quotes(self, data_root, monkeypatch):
        _reference(data_root)
        _write_raw("f1", {900: {
            9001: [_tick(SCHED - timedelta(hours=1), 1.9),
                   _tick(TRUE + timedelta(minutes=30), 3.4)],
            9002: [_tick(SCHED - timedelta(hours=1), 2.0),
                   _tick(TRUE + timedelta(minutes=30), 1.3)],
        }}, book="betrivers")
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["best_closing_odds"] == 1.9      # not the 3.4 in-play price


class TestIncremental:
    def test_second_run_skips_already_parsed_files(self, data_root, monkeypatch):
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)

        first = transformer.transform()
        assert first.files_parsed == 1 and first.files_skipped == 0

        second = transformer.transform()
        assert second.files_parsed == 0 and second.files_skipped == 1

    def test_quotes_are_identical_across_runs(self, data_root, monkeypatch):
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}},
                   book="betrivers")
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)

        transformer.transform()
        a = _quotes()
        transformer.transform()
        assert a.equals(_quotes())

    def test_raw_newer_than_staged_is_reparsed(self, data_root, monkeypatch):
        """Staging is per raw file, so a rewritten capture overwrites its own
        staged file — it cannot double-count, so re-staging is the right answer."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.7), _tick(SCHED, 1.8)],
                                9002: [_tick(SCHED, 2.2)]}})
        _touch_raw("f1")
        again = transformer.transform()
        assert again.files_parsed == 1 and again.files_skipped == 0
        assert sorted(_ticks("total_games")["odds"].to_list()) == [1.7, 1.8, 2.2]

    def test_schema_hash_change_restages_the_tree(self, data_root, monkeypatch):
        """mtime cannot see a parser change — the raw file did not move. The
        marker is read once per run rather than per file (233s at 16.5k files)."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        from mvp.oddspapi import paths

        assert paths.ticks_schema_marker().read_text(encoding="utf-8") == (
            transformer.TICKS_SCHEMA_HASH
        )
        paths.ticks_schema_marker().write_text("stale_hash", encoding="utf-8")
        again = transformer.transform()
        assert again.schema_restage is True
        assert again.files_parsed == 1 and again.files_skipped == 0

    def test_staged_files_carry_the_hash_in_their_own_metadata(
        self, data_root, monkeypatch
    ):
        """The marker is a fast path, not the only record — each file stays
        self-describing, which is what is_schema_current reads."""
        import pyarrow.parquet as pq

        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        from mvp.oddspapi import paths

        p = next(paths.ticks_dir().glob("*/*.parquet"))
        meta = pq.read_metadata(p).metadata
        assert meta[transformer.SCHEMA_META_KEY.encode()].decode() == (
            transformer.TICKS_SCHEMA_HASH
        )

    def test_missing_marker_falls_back_to_per_file_metadata(
        self, data_root, monkeypatch
    ):
        """Absent marker must not read as "tree is current". It would skip every
        file and then stamp the tree with a hash it was never written under —
        certifying 16.5k stale files that are then never re-parsed.
        """
        import pyarrow.parquet as pq

        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        from mvp.oddspapi import paths

        staged = next(paths.ticks_dir().glob("*/*.parquet"))
        # A tree written by an older layout: no marker, and file metadata that does
        # not carry this hash.
        paths.ticks_schema_marker().unlink()
        pl.read_parquet(staged).write_parquet(staged, metadata={"parser_version": "1"})

        again = transformer.transform()
        assert again.files_parsed == 1 and again.files_skipped == 0
        meta = pq.read_metadata(staged).metadata
        assert meta[transformer.SCHEMA_META_KEY.encode()].decode() == (
            transformer.TICKS_SCHEMA_HASH
        )

    def test_missing_marker_does_not_force_a_needless_reparse(
        self, data_root, monkeypatch
    ):
        """The marker is a cache. Losing it when the files themselves are current
        should cost a metadata read, not a full reparse."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        from mvp.oddspapi import paths

        paths.ticks_schema_marker().unlink()
        again = transformer.transform()
        assert again.files_parsed == 0 and again.files_skipped == 1
        assert paths.ticks_schema_marker().exists()

    def test_empty_capture_group_does_not_prune_its_staged_half(
        self, data_root, monkeypatch
    ):
        """ensure_dirs() creates every historical dir, so an unsynced or renamed
        capture group looks empty rather than missing — and pruning on that would
        delete ~8k staged files before anything was parsed."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}},
                   book="betrivers", group=0)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.95)], 9002: [_tick(SCHED, 1.9)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        from mvp.oddspapi import paths

        assert len(list(paths.ticks_dir().glob("*/*.parquet"))) == 2
        # Group 1's capture has not synced this run.
        (paths.historical_dirs()[1] / "historical_f1.json").unlink()
        again = transformer.transform()

        assert again.files_pruned == 0
        assert again.files_orphan_kept == 1
        assert len(list(paths.ticks_dir().glob("*/*.parquet"))) == 2
        # And the surviving staged half still contributes to the aggregate.
        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["n_books"] == 2

    def test_orphaned_staged_file_is_pruned(self, data_root, monkeypatch):
        """A staged file whose raw capture is gone must not keep feeding the
        aggregate — it would be a row nothing on disk can explain."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _write_raw("f2", {900: {9001: [_tick(SCHED, 1.5)], 9002: [_tick(SCHED, 2.4)]}})
        _fake_crosswalk_many(monkeypatch, ["f1", "f2"])
        transformer.transform()

        from mvp.oddspapi import paths

        (paths.historical_dirs()[0] / "historical_f2.json").unlink()
        again = transformer.transform()
        assert again.files_pruned == 1
        assert _quotes()["match_uid"].unique().to_list() == ["2026_1_SGL_R32_f1"]

    def test_refresh_reparses_everything(self, data_root, monkeypatch):
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()
        again = transformer.transform(refresh=True)
        assert again.files_parsed == 1 and again.files_skipped == 0


class TestMatchingIsIndependentOfParsing:
    """Parsing is expensive and rare; matching is cheap and improves constantly.

    Baking match_uid into the staged ticks would mean a one-line alias addition
    costs a 35 GB reparse — and would discard the ticks of anything unresolved, so
    they could never be recovered at all.
    """

    def _setup(self, data_root, n=5):
        """n fixtures, so one unresolved is a realistic minority not 100%."""
        _reference(data_root)
        for i in range(n):
            _write_raw(f"f{i}", {900: {9001: [_tick(SCHED, 1.9)],
                                       9002: [_tick(SCHED, 2.0)]}})

    def test_unmatched_ticks_survive_in_the_ticks_tree(self, data_root, monkeypatch):
        self._setup(data_root)
        _fake_crosswalk_many(monkeypatch, [f"f{i}" for i in range(4)])  # f4 unresolved
        transformer.transform()

        assert "f4" in _ticks()["fixture_id"].unique().to_list()
        assert "2026_1_SGL_R32_f4" not in _quotes()["match_uid"].unique().to_list()

    def test_a_later_alias_recovers_them_without_reparsing(
        self, data_root, monkeypatch
    ):
        self._setup(data_root)
        _fake_crosswalk_many(monkeypatch, [f"f{i}" for i in range(4)])
        first = transformer.transform()
        assert first.files_parsed == 5
        assert first.fixtures_unmatched == 1

        # The matcher improves — same staged ticks, no reparse.
        _fake_crosswalk_many(monkeypatch, [f"f{i}" for i in range(5)])
        second = transformer.transform()
        assert second.files_parsed == 0
        assert second.files_skipped == 5
        assert second.fixtures_unmatched == 0
        assert "2026_1_SGL_R32_f4" in _quotes()["match_uid"].unique().to_list()

    def test_batching_does_not_split_a_fixture(self, data_root, monkeypatch):
        """A price series lives inside one fixture, so batching by fixture is
        exact — but only if every file of a fixture lands in the same batch."""
        _reference(data_root)
        t0, t1 = SCHED - timedelta(hours=2), SCHED - timedelta(hours=1)
        for i in range(6):
            _write_raw(f"f{i}", {900: {9001: [_tick(t0, 1.80)],
                                       9002: [_tick(t0, 2.05)]}},
                       book="betrivers", group=0)
            _write_raw(f"f{i}", {900: {9001: [_tick(t1, 1.95)],
                                       9002: [_tick(t1, 1.90)]}},
                       book="fanduel", group=1)
        _fake_crosswalk_many(monkeypatch, [f"f{i}" for i in range(6)])
        monkeypatch.setattr(transformer, "REDUCE_BATCH", 2)
        transformer.transform()

        q = _quotes("total_games").filter(pl.col("side") == "Over")
        assert len(q) == 6
        assert q["n_books"].unique().to_list() == [2]
        assert q["best_opening_odds"].unique().to_list() == [1.80]


class TestGuards:
    def test_unmatched_fixtures_over_threshold_refuse_to_write(
        self, data_root, monkeypatch
    ):
        """A broken matcher should fail loudly, not look like a thin season."""
        _reference(data_root)
        for i in range(4):
            _write_raw(f"f{i}", {900: {9001: [_tick(SCHED, 1.9)],
                                       9002: [_tick(SCHED, 2.0)]}})
        _fake_crosswalk(monkeypatch, "nonexistent", SCHED, TRUE)
        with pytest.raises(RuntimeError, match="did not resolve to a match_uid"):
            transformer.transform()

    def test_unclassified_book_is_refused(self, data_root, monkeypatch):
        """An unknown book defaulting to entry would let the backtest price bets
        at a book that cannot be reached."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}},
                   book="some_new_book")
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        with pytest.raises(ValueError, match="has no role"):
            transformer.transform()


class TestSchemasCannotDrift:
    """SCHEMA_HASH comes from the pydantic models, but the parquet files are shaped
    by the polars dicts. If they diverge, a column can be added with the hash
    unchanged — no re-stage, a mixed-width tree, and the first batch that straddles
    the change dies in pl.read_parquet.
    """

    def test_tick_schema_matches_its_record(self):
        from mvp.oddspapi.schemas.ticks import TickRecord

        assert list(transformer.TICK_SCHEMA) == list(TickRecord.model_fields)

    def test_quote_schema_matches_its_record(self):
        from mvp.oddspapi.schemas.quotes import QuoteRecord

        assert list(transformer.QUOTE_SCHEMA) == list(QuoteRecord.model_fields)

    def test_written_quotes_match_the_declared_dtypes(self, data_root, monkeypatch):
        """n_unique() returns UInt32 while the schema declares Int64, so a populated
        file and an empty one would not concatenate."""
        _reference(data_root)
        _write_raw("f1", {900: {9001: [_tick(SCHED, 1.9)], 9002: [_tick(SCHED, 2.0)]}},
                   book="betrivers")
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        q = _quotes()
        assert q.height > 0
        assert dict(q.schema) == transformer.QUOTE_SCHEMA


class TestForwardFillBounds:
    def test_withdrawn_quote_does_not_reach_the_close(self, data_root, monkeypatch):
        """active=False is a withdrawal; carrying it forward as a live price would
        quote a book that has left the market."""
        _reference(data_root)
        t0 = SCHED - timedelta(hours=3)
        t1 = SCHED - timedelta(hours=2)
        t2 = SCHED - timedelta(hours=1)
        _write_raw("f1", {900: {9001: [_tick(t0, 2.50), _tick(t1, 2.50, active=False)],
                                9002: [_tick(t0, 1.55), _tick(t1, 1.55, active=False)]}},
                   book="betrivers", group=0)
        _write_raw("f1", {900: {9001: [_tick(t0, 1.90), _tick(t2, 1.95)],
                                9002: [_tick(t0, 2.00), _tick(t2, 1.90)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["best_opening_odds"] == 2.50    # both live at t0
        assert over["best_closing_odds"] == 1.95    # betrivers withdrew, so not 2.50

    def test_standing_price_reaches_the_close(self, data_root, monkeypatch):
        """The case the fill exists for: a book that quoted once and never moved is
        still on the board at the off, so the close is best-across-books."""
        _reference(data_root)
        t0, t2 = SCHED - timedelta(hours=3), SCHED - timedelta(hours=1)
        _write_raw("f1", {900: {9001: [_tick(t0, 2.10)], 9002: [_tick(t0, 1.75)]}},
                   book="betrivers", group=0)
        _write_raw("f1", {900: {9001: [_tick(t2, 1.95)], 9002: [_tick(t2, 1.90)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["best_closing_odds"] == 2.10    # betrivers' standing price wins
        assert over["n_books"] == 2

    def test_carry_is_capped(self, data_root, monkeypatch):
        """A book that stops sending ticks without going inactive would otherwise
        have a two-day-old price counted at the close — measured tail is 44.8 h."""
        _reference(data_root)
        stale = SCHED - timedelta(hours=30)
        late = SCHED - timedelta(hours=1)
        _write_raw("f1", {900: {9001: [_tick(stale, 2.60)], 9002: [_tick(stale, 1.50)]}},
                   book="betrivers", group=0)
        _write_raw("f1", {900: {9001: [_tick(late, 1.95)], 9002: [_tick(late, 1.90)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["best_opening_odds"] == 2.60    # real at the time it was posted
        assert over["best_closing_odds"] == 1.95    # 29h stale, so not 2.60

    def test_carry_within_the_cap_is_kept(self, data_root, monkeypatch):
        _reference(data_root)
        early = SCHED - timedelta(hours=3)
        late = SCHED - timedelta(hours=1)
        assert (late - early).total_seconds() / 60 <= transformer.MAX_CARRY_MIN
        _write_raw("f1", {900: {9001: [_tick(early, 2.60)], 9002: [_tick(early, 1.50)]}},
                   book="betrivers", group=0)
        _write_raw("f1", {900: {9001: [_tick(late, 1.95)], 9002: [_tick(late, 1.90)]}},
                   book="fanduel", group=1)
        _fake_crosswalk(monkeypatch, "f1", SCHED, TRUE)
        transformer.transform()

        over = _quotes("total_games").filter(pl.col("side") == "Over").row(0, named=True)
        assert over["best_closing_odds"] == 2.60


class TestOneMatchManyFixtures:
    """fixture -> match_uid is not injective: 38 match_uids carry more than one
    captured fixture (superseded events, one with three). Batching by fixture would
    emit the same quote key from two batches, each with a partial open/close.
    """

    def _two_fixtures_one_match(self, monkeypatch):
        xw = pl.DataFrame([
            {"fixture_id": fid, "match_uid": "2026_1_SGL_R32_AA_BB",
             "p1_id": p1, "p2_id": p2, "p1_book_name": "A A", "p2_book_name": "B B",
             "start_time": SCHED, "true_start_time": TRUE, "status_id": 2}
            # Opposite participant order, which is what duplicates the ids join.
            for fid, p1, p2 in (("f1", "AA", "BB"), ("f2", "BB", "AA"))
        ], schema={
            "fixture_id": pl.Utf8, "match_uid": pl.Utf8, "p1_id": pl.Utf8,
            "p2_id": pl.Utf8, "p1_book_name": pl.Utf8, "p2_book_name": pl.Utf8,
            "start_time": pl.Datetime("us", "UTC"),
            "true_start_time": pl.Datetime("us", "UTC"), "status_id": pl.Int64,
        })
        monkeypatch.setattr(transformer.matcher, "build", lambda *a, **k: xw)

    def test_one_row_per_quote_key(self, data_root, monkeypatch):
        _reference(data_root)
        t0, t1 = SCHED - timedelta(hours=3), SCHED - timedelta(hours=1)
        _write_raw("f1", {900: {9001: [_tick(t0, 1.90)], 9002: [_tick(t0, 2.00)]}},
                   book="betrivers", group=0)
        _write_raw("f2", {900: {9001: [_tick(t1, 1.95)], 9002: [_tick(t1, 1.90)]}},
                   book="fanduel", group=1)
        self._two_fixtures_one_match(monkeypatch)
        # One match per batch, so the two fixtures would land apart if batching were
        # keyed on fixture.
        monkeypatch.setattr(transformer, "REDUCE_BATCH", 1)
        report = transformer.transform()

        assert report.matches_multi_fixture == 1
        q = _quotes("total_games")
        key = ["match_uid", "market", "points", "side", "role"]
        assert q.height == q.unique(subset=key).height
        over = q.filter(pl.col("side") == "Over").row(0, named=True)
        assert over["best_opening_odds"] == 1.90     # from f1, the earlier fixture
        assert over["best_closing_odds"] == 1.95     # from f2, the later one
        assert over["n_books"] == 2
    def test_known_markets_keep_friendly_names(self):
        assert markets.stage_name("totals-games", "result") == "total_games"
        assert markets.stage_name("spreads-games", "result") == "game_spread"
        assert markets.stage_name("moneyline", "result") == "moneyline"

    def test_unknown_markets_are_normalised_not_dropped(self):
        assert markets.stage_name("teamtotals-games-team1", "result") == (
            "teamtotals_games_team1_result"
        )
        assert markets.stage_name("totals-aces", "p3") == "totals_aces_p3"
