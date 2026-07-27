"""raw/oddspapi -> stage/oddspapi/ticks -> aggregate/oddspapi/quotes.parquet.

Reshapes captured ticks to the stage contract:

    price -> odds        createdAt -> fetched_at        handicap -> points

Three layers, each with a different lifetime:

  PARSE     one raw JSON -> one parquet under stage/oddspapi/ticks/<group>/,
            keyed by fixture_id, no match resolution. Expensive (~10 min over
            35 GB) and rare, skipped per file on the rule atptour stages on
            (`atptour/pipeline.py:131-148`): staged file missing, raw newer than
            staged, or the parser's schema hash changed.
  SNAPSHOT  staged ticks -> stage/oddspapi/snapshots/<book>/<market>.parquet, one
            row per (match_uid, market, points, side, book, 15-min bucket). The
            capture records CHANGES, so this forward-fills each book's standing
            price into the buckets it was on the board for — see
            `_snapshots_from_ticks`. Column-for-column a scraper's stage file,
            which is the point: book identity and the time axis survive here, so
            what price was reachable, where, and when stays answerable.
  REDUCE    snapshots -> aggregate/oddspapi/quotes.parquet, one row per
            (match_uid, market, points, side, role) with the price at open /
            formed / close. Convenient, and lossy by construction: best-across
            spends book identity and three moments spend the rest. Derived, never
            the record. Cheap and frequent — the matcher keeps improving, and
            re-running is a join plus a group_by, not a reparse.

Everything captured is staged. No prematch cut, no `active` cut — those are
columns, and filtering them here would make the dropped universe unrecoverable
without a full reparse. In-play ticks matter in particular: the chain is a
score-state model and prices from any score, so live pricing is the market it has
the strongest claim on. The reduction takes only prematch ticks, but it is derived
— the ticks tree remains the record.

`event_status` is derived, because the ticks carry no match state: a historical
payload is only {fixtureId, bookmakers} and a tick only
{createdAt, price, limit, active, exchangeMeta}. The boundary is the fixture's
`trueStartTime` where known, falling back to the scheduled `startTime` — 48% of
fixtures begin more than 5 minutes late, so cutting on the schedule would file
genuinely prematch quotes as in-play.

Memory is bounded by a batch of fixtures, never by the corpus. A price series is
(book, market, points, side) within one fixture, and a fixture's books span at
most two staged files, so the reduction needs no shuffle — batching by fixture is
sufficient and exact.
"""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from mvp.odds.aggregator import compute_open_close_odds
from mvp.oddspapi import markets, matcher, parser
from mvp.oddspapi.paths import (
    REFERENCE_BOOKS,
    ensure_dirs,
    historical_dirs,
    quotes_path,
    role_of,
    snapshots_dir,
    stage_root,
    ticks_dir,
    ticks_path,
    ticks_schema_marker,
)
from mvp.oddspapi.schemas.quotes import SCHEMA_HASH as QUOTES_SCHEMA_HASH
from mvp.oddspapi.schemas.ticks import SCHEMA_HASH as TICKS_SCHEMA_HASH

logger = logging.getLogger(__name__)

# Fail rather than silently shrink coverage: the original transform printed the
# unmatched count and carried on, so a broken matcher looked like a thin season.
MAX_UNMATCHED_FRACTION = 0.25

PREMATCH = "NOT_STARTED"
IN_PLAY = "IN_PLAY"

SCHEMA_META_KEY = "pydantic_schema_hash"

# Fixtures per reduction batch. compute_open_close_odds groups by match_uid, so
# batching is exact; this only trades memory for the number of passes. ~20k ticks
# per fixture, and the snapshot expansion multiplies that by the books per series,
# so 100 keeps a batch in the low hundreds of MB.
REDUCE_BATCH = 100

TICK_SCHEMA: dict[str, pl.DataType] = {
    "fixture_id": pl.Utf8,
    "book": pl.Utf8,
    "market": pl.Utf8,
    "points": pl.Float64,
    "side": pl.Utf8,
    "odds": pl.Float64,
    "fetched_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "active": pl.Boolean,
    "limit": pl.Float64,
    "exchange_meta": pl.Utf8,
}

QUOTE_SCHEMA: dict[str, pl.DataType] = {
    "match_uid": pl.Utf8,
    "market": pl.Utf8,
    "points": pl.Float64,
    "side": pl.Utf8,
    "role": pl.Utf8,
    "best_opening_odds": pl.Float64,
    "formed_odds": pl.Float64,
    "best_closing_odds": pl.Float64,
    "n_books": pl.Int64,
    "p1_id": pl.Utf8,
    "p2_id": pl.Utf8,
}
QUOTE_SORT = ["match_uid", "market", "points", "side", "role"]

# The scrapers' stage columns, which is the point: `stage/oddspapi/snapshots/<book>/
# total_games.parquet` and `stage/betrivers/total_games.parquet` are the same table.
# `match_uid` stands where a scraper carries its own event id, because the crosswalk
# already resolved identity — a consumer joins the event map for one and not the
# other, and that is the only asymmetry left between them.
SNAPSHOT_SCHEMA: dict[str, pl.DataType] = {
    "match_uid": pl.Utf8,
    "p1_id": pl.Utf8,
    "p2_id": pl.Utf8,
    "market": pl.Utf8,
    "points": pl.Float64,
    "side": pl.Utf8,
    "book": pl.Utf8,
    "odds": pl.Float64,
    "fetched_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "event_status": pl.Utf8,
}
SNAPSHOT_SORT = ["match_uid", "market", "points", "side", "fetched_at"]

XW_COLS = ["fixture_id", "match_uid", "p1_id", "p2_id", "start_time", "true_start_time"]

# A reference book quotes alone, so it never reaches two books deep; requiring
# two would null every formed price on that side.
MIN_BOOKS_FORMED = {"reference": 1, "entry": 2}


@dataclass
class TransformReport:
    files_total: int = 0
    files_parsed: int = 0
    files_skipped: int = 0
    files_unreadable: int = 0
    files_pruned: int = 0
    files_orphan_kept: int = 0
    schema_restage: bool = False
    fixtures_staged: int = 0
    fixtures_empty: int = 0
    fixtures_unmatched: int = 0
    matches_multi_fixture: int = 0
    ticks_parsed: int = 0
    quotes: int = 0
    quotes_by_market: dict[str, int] = field(default_factory=dict)
    quotes_by_role: dict[str, int] = field(default_factory=dict)
    snapshots: int = 0
    snapshots_by_book: dict[str, int] = field(default_factory=dict)

    def summary(self) -> str:
        return (
            f"{self.quotes:,} quotes across {len(self.quotes_by_market)} markets "
            f"from {self.fixtures_staged:,} staged fixtures "
            f"({self.files_parsed} newly parsed, {self.files_skipped} skipped, "
            f"{self.fixtures_empty} staged with no ticks, "
            f"{self.fixtures_unmatched} unmatched)"
        )


def _file_schema_hash(path: Path) -> str | None:
    """The hash a staged file was written under, from its own metadata.

    Same signal `BaseJob.is_schema_current` reads. Only used on the recovery path
    where the tree marker is missing — reading it per file costs 233s at 16.5k
    files, which is why the marker exists.
    """
    # Narrow on purpose: a missing or corrupt file means "re-stage it", but a bug in
    # here must not be swallowed into that same answer. A bare except once turned a
    # NameError into "every file is stale".
    try:
        meta = pq.read_metadata(path).metadata
    except (OSError, pa.ArrowException):
        return None
    if not meta:
        return None
    raw = meta.get(SCHEMA_META_KEY.encode())
    return raw.decode() if raw is not None else None


def _raw_entries() -> list[tuple[str, str, float]]:
    """(group, raw stem, mtime) per capture file, from one directory read.

    os.scandir rather than Path.glob + stat: on Windows the DirEntry carries the
    mtime from the directory read already done, so this is one pass instead of
    16.5k extra stat calls (60s measured).
    """
    out: list[tuple[str, str, float]] = []
    for d in historical_dirs():
        if not d.exists():
            continue
        with os.scandir(d) as it:
            for e in it:
                if e.name.endswith(".json"):
                    out.append((d.name, e.name[: -len(".json")], e.stat().st_mtime))
    return sorted(out)


def _staged_index() -> dict[Path, float]:
    """Staged tick file -> mtime, from one glob of the tree (0.4s for 16.5k)."""
    index: dict[Path, float] = {}
    for group_dir in ticks_dir().iterdir() if ticks_dir().exists() else []:
        if not group_dir.is_dir():
            continue
        with os.scandir(group_dir) as it:
            for e in it:
                if e.name.endswith(".parquet"):
                    index[Path(e.path)] = e.stat().st_mtime
    return index


def _write_ticks(rows: list[dict], staged: Path) -> None:
    """Write one raw file's ticks, atomically so a kill cannot leave a half file.

    Zero-row writes are deliberate: a fixture with no usable quote, or a file that
    would not parse, still gets a staged file, otherwise it is re-parsed forever.
    The schema hash goes in the metadata (BaseJob.save_parquet's convention) so
    each file stays self-describing even though the skip path reads the marker.
    """
    staged.parent.mkdir(parents=True, exist_ok=True)
    tmp = staged.with_suffix(".parquet.tmp")
    try:
        pl.DataFrame(rows, schema=TICK_SCHEMA).write_parquet(
            tmp, metadata={SCHEMA_META_KEY: TICKS_SCHEMA_HASH}
        )
        tmp.replace(staged)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def transform(
    *,
    refresh: bool = False,
    refresh_matcher: bool = False,
) -> TransformReport:
    """Parse new raw files, then rebuild the quotes aggregate.

    `refresh=True` re-parses every raw file regardless of what is staged. Normal
    runs skip per file, so re-running after an alias change re-reduces without
    re-parsing.
    """
    ensure_dirs()
    index = markets.load_index()
    logger.info(
        "markets reference: %d markets in %d groups",
        len(index), len(markets.groups(index)),
    )

    xw = matcher.build(refresh=refresh_matcher)

    raw = _raw_entries()
    if not raw:
        raise FileNotFoundError(
            f"no raw files under {[str(d) for d in historical_dirs()]}. Capture is "
            "out of scope for this job — place the captured tree there first."
        )

    marker = ticks_schema_marker()
    staged_index = _staged_index()
    report = TransformReport(files_total=len(raw))

    # The marker is a fast path, not the source of truth. Absent-but-populated is
    # the dangerous case: treating it as current would skip every file and then
    # stamp the tree with a hash it was never written under, and per-file metadata
    # is the only thing that can still tell the difference.
    verify_per_file = False
    if marker.exists():
        staged_hash = marker.read_text(encoding="utf-8").strip()
        if staged_hash != TICKS_SCHEMA_HASH:
            logger.info(
                "tick schema changed (%s -> %s): re-staging the whole tree",
                staged_hash, TICKS_SCHEMA_HASH,
            )
            report.schema_restage = True
    elif staged_index:
        verify_per_file = True
        logger.info(
            "no %s: verifying %d staged files against their own metadata",
            marker.name, len(staged_index),
        )

    group_dirs = {d.name: d for d in historical_dirs()}
    seen_groups: set[str] = set()
    expected: set[Path] = set()
    pending: list[tuple[Path, Path]] = []
    for group, stem, raw_mtime in raw:
        seen_groups.add(group)
        staged = ticks_path(group, stem)
        expected.add(staged)
        staged_mtime = staged_index.get(staged)
        stale = (
            refresh
            or report.schema_restage
            or staged_mtime is None
            or raw_mtime > staged_mtime
            or (verify_per_file and _file_schema_hash(staged) != TICKS_SCHEMA_HASH)
        )
        if stale:
            pending.append((group_dirs[group] / f"{stem}.json", staged))
        else:
            report.files_skipped += 1

    # Prune only inside groups that actually yielded raw files. ensure_dirs()
    # creates every historical dir, so an empty one is indistinguishable from a
    # missing one — and an unsynced or renamed capture group would otherwise have
    # its whole staged half (~8k files) deleted before anything was parsed.
    for p in set(staged_index) - expected:
        if p.parent.name in seen_groups:
            p.unlink()
            report.files_pruned += 1
        else:
            report.files_orphan_kept += 1
    if report.files_orphan_kept:
        logger.warning(
            "%d staged files kept: their capture group yielded no raw files "
            "(unsynced or renamed group?). Not pruning them.",
            report.files_orphan_kept,
        )

    logger.info(
        "raw files: %d total, %d already staged, %d to parse%s",
        len(raw), report.files_skipped, len(pending),
        f", {report.files_pruned} orphans pruned" if report.files_pruned else "",
    )

    for i, (f, staged) in enumerate(pending, start=1):
        fixture_id, ticks = parser.parse_file(f, index)
        if fixture_id is None:
            report.files_unreadable += 1
            _write_ticks([], staged)
            continue
        rows = []
        for t in ticks:
            if t.odds is None:
                continue
            role_of(t.book)     # refuse unclassified books loudly
            rows.append({
                "fixture_id": fixture_id,
                "book": t.book,
                "market": t.stage_name,
                "points": t.points,
                "side": t.side,
                "odds": float(t.odds),
                "fetched_at": t.ts,
                "active": t.active,
                "limit": t.limit,
                "exchange_meta": t.exchange_meta,
            })
        _write_ticks(rows, staged)
        report.files_parsed += 1
        if i % 1000 == 0:
            logger.info("parsed %d/%d", i, len(pending))

    if pending or not marker.exists():
        marker.write_text(TICKS_SCHEMA_HASH, encoding="utf-8")

    _reduce(xw, report)
    logger.info(report.summary())
    return report


def _fixture_of(path: Path) -> str:
    """Fixture id from a staged filename (raw files are historical_<fixtureId>)."""
    return path.stem.removeprefix("historical_")


SERIES = ["match_uid", "market", "points", "side"]

# How long a book's untouched price still counts as being on the board. Books also
# just stop sending ticks without going inactive, and the measured carry tail runs
# to 44.8 h; 4 h leaves ~97.5% of real carries (p95 = 80 min) untouched while
# keeping a stale price out of the closing bucket.
MAX_CARRY_MIN = 240


def _snapshots_from_ticks(ticks: pl.DataFrame) -> pl.DataFrame:
    """Ticks -> per-bucket snapshots, forward-filling each book's standing price.

    compute_open_close_odds reads "books quoting in this 15-minute bucket" as the
    number of books on the board, which is true of the scrapers because they poll
    every 15 minutes. OddsPapi captures CHANGES: measured on real ticks, a book is
    present in 10.3 of the 45.2 buckets it spans (53%).

    Two of the three prices are wrong without this:

      close   the last bucket with any quote holds only the books that MOVED then,
              usually one, so the close is that book's price instead of the best
              across the books still standing at the off.
      formed  needs two books in one bucket; a book that quoted at 09:00 and not
              again until 11:00 was on the board at 10:00 but invisible there, so
              formed arrives late or never.

    The open is unaffected: the earliest bucket with any quote is by construction
    the first bucket anything was posted in, so no book can have been live before
    it, and the fill only ever adds rows at later buckets.

    A quote stands until it changes or is withdrawn, so each book's last price is
    carried forward to the last bucket any book quoted in that series, stopping at
    an `active=False` tick. The one case this cannot see is capture downtime, which
    is indistinguishable from "nothing changed".

    MAX_CARRY_MIN bounds it, because a book can also just stop sending ticks without
    ever going inactive. Measured carry age from a book's last tick to the series'
    last prematch bucket: p50 0 min, p75 3, p90 36, p95 80, p99 862, max 2,688
    (44.8 h). Uncapped, a two-day-old price lands in the closing bucket and inflates
    both best_closing_odds and n_books on thin markets — and those close prices feed
    CLV. `compute_threshold_odds` guards the same way with `max_lag_min`.
    """
    if ticks.is_empty():
        return ticks

    obs = (
        ticks.with_columns(pl.col("fetched_at").dt.truncate("15m").alias("_rnd"))
        .sort("fetched_at")
        .group_by([*SERIES, "book", "_rnd"], maintain_order=True)
        .agg(pl.col("odds").last(), pl.col("active").last(),
             pl.col("event_status").last())
    )
    # Grid: the buckets in which SOME book quoted this series, crossed with the
    # books that quoted it at all. Buckets nobody quoted carry no information.
    grid = obs.select([*SERIES, "_rnd"]).unique()
    books = obs.select([*SERIES, "book"]).unique()
    filled = (
        grid.join(books, on=SERIES, how="inner", nulls_equal=True)
        .join(obs, on=[*SERIES, "book", "_rnd"], how="left", nulls_equal=True)
        .sort([*SERIES, "book", "_rnd"])
        .with_columns(
            # _src_rnd tracks the bucket each carried price came FROM, so the carry
            # age is measurable rather than assumed.
            pl.when(pl.col("odds").is_not_null()).then(pl.col("_rnd")).alias("_src_rnd"),
        )
        .with_columns(
            pl.col("odds").forward_fill().over([*SERIES, "book"]),
            pl.col("active").forward_fill().over([*SERIES, "book"]),
            pl.col("event_status").forward_fill().over([*SERIES, "book"]),
            pl.col("_src_rnd").forward_fill().over([*SERIES, "book"]),
        )
        # Rows before a book's first quote stay null; a withdrawn quote is gone; and
        # a price nobody has touched for MAX_CARRY_MIN is no longer evidence the book
        # is on the board.
        .filter(
            pl.col("odds").is_not_null()
            & pl.col("active").fill_null(True)
            & ((pl.col("_rnd") - pl.col("_src_rnd")).dt.total_minutes()
               <= MAX_CARRY_MIN)
        )
        .drop("_src_rnd")
    )
    return filled.rename({"_rnd": "fetched_at"})


def _snapshot_parts_dir() -> Path:
    return stage_root() / ".snapshots.parts"


def _write_snapshot_parts(
    snaps: pl.DataFrame, ids: pl.DataFrame, batch: int,
) -> None:
    """One part per (book, batch); consolidated into per-market files at the end.

    Parts rather than one accumulating frame for the reason the reduction is batched
    at all: the snapshot expansion runs several times the quote count, and holding
    the corpus is exactly what the batching exists to avoid.
    """
    if snaps.is_empty():
        return
    frame = snaps.join(ids, on="match_uid", how="left")
    for book in frame["book"].unique().sort().to_list():
        d = _snapshot_parts_dir() / book
        d.mkdir(parents=True, exist_ok=True)
        (
            frame.filter(pl.col("book") == book)
            .select(list(SNAPSHOT_SCHEMA))
            .write_parquet(d / f"part-{batch:05d}.parquet")
        )


def _consolidate_snapshots(report: TransformReport) -> None:
    """Parts -> `snapshots/<book>/<market>.parquet`, then swap the layer in.

    One book at a time: a book's parts are bounded by its share of the corpus, so
    this holds no more than the batching already permits.

    The swap renames the live layer aside before renaming the new one in, so the
    window in which no layer exists is one rename wide rather than a whole rewrite.
    B:/ is shared with the live pipeline; a consumer reading mid-write is a real
    event here, not a hypothetical.
    """
    parts = _snapshot_parts_dir()
    if not parts.exists():
        return
    new_root = stage_root() / ".snapshots.new"
    shutil.rmtree(new_root, ignore_errors=True)
    for book_dir in sorted(p for p in parts.iterdir() if p.is_dir()):
        files = sorted(book_dir.glob("part-*.parquet"))
        if not files:
            continue
        frame = pl.read_parquet(files)
        for market in frame["market"].unique().sort().to_list():
            path = new_root / book_dir.name / f"{market}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.filter(pl.col("market") == market).sort(SNAPSHOT_SORT).write_parquet(
                path
            )
        report.snapshots_by_book[book_dir.name] = len(frame)
        report.snapshots += len(frame)

    old = stage_root() / ".snapshots.old"
    shutil.rmtree(old, ignore_errors=True)
    try:
        if snapshots_dir().exists():
            snapshots_dir().rename(old)
        if new_root.exists():
            new_root.rename(snapshots_dir())
    finally:
        shutil.rmtree(old, ignore_errors=True)
        shutil.rmtree(parts, ignore_errors=True)
        shutil.rmtree(new_root, ignore_errors=True)


def _reduce(xw: pl.DataFrame, report: TransformReport) -> None:
    """Staged ticks -> the quotes aggregate, batched by fixture.

    Batching by fixture is exact, not an approximation: a price series is
    (book, market, points, side) inside one fixture, and a fixture's books live in
    at most two staged files, so no series is ever split across batches. That is
    what makes this bounded — an earlier version read the whole tree into one frame
    (141 M rows, ~10.6 GB per copy, three copies live) and a version before that
    re-scanned every file once per market.
    """
    by_fixture: dict[str, list[Path]] = {}
    for p in _staged_index():
        by_fixture.setdefault(_fixture_of(p), []).append(p)
    if not by_fixture:
        logger.warning("no staged ticks to reduce")
        return
    report.fixtures_staged = len(by_fixture)

    # Batch by MATCH, not fixture. The quote key is (match_uid, market, points,
    # side, role), and fixture -> match_uid is not injective: 38 match_uids carry
    # more than one captured fixture (superseded events, one with three). Batching
    # by fixture would put those in different batches and emit the same key twice,
    # each with a partial open/close and no uniqueness guard on the output.
    fixture_to_match = dict(zip(xw["fixture_id"].to_list(), xw["match_uid"].to_list()))
    by_match: dict[str, list[Path]] = {}
    unmatched = 0
    for fixture, files in by_fixture.items():
        match_uid = fixture_to_match.get(fixture)
        if match_uid is None:
            unmatched += 1
            continue
        by_match.setdefault(match_uid, []).extend(files)
    report.fixtures_unmatched = unmatched
    report.matches_multi_fixture = sum(
        1 for m, fs in by_match.items()
        if len({_fixture_of(p) for p in fs}) > 1
    )

    frac = unmatched / max(len(by_fixture), 1)
    if frac > MAX_UNMATCHED_FRACTION:
        raise RuntimeError(
            f"{unmatched} of {len(by_fixture)} staged fixtures did not resolve to a "
            f"match_uid ({frac:.1%} > {MAX_UNMATCHED_FRACTION:.0%}). Refusing to "
            "write a silently thin aggregate — check the matcher (stale fixtures? "
            "bad aliases?)."
        )

    xw_slim = xw.select(XW_COLS)
    boundary = pl.coalesce([pl.col("true_start_time"), pl.col("start_time")])
    out: list[pl.DataFrame] = []
    # A killed run leaves parts behind, and they would be consolidated into the next
    # run's layer as duplicates of matches it also wrote.
    shutil.rmtree(_snapshot_parts_dir(), ignore_errors=True)
    todo = sorted(by_match)
    logger.info(
        "reducing %d matches from %d fixtures (%d unmatched, %d matches with "
        "multiple fixtures) in batches of %d",
        len(todo), report.fixtures_staged, unmatched,
        report.matches_multi_fixture, REDUCE_BATCH,
    )

    for start in range(0, len(todo), REDUCE_BATCH):
        batch = todo[start:start + REDUCE_BATCH]
        files = [p for m in batch for p in by_match[m]]
        ticks = pl.read_parquet(files)
        report.ticks_parsed += len(ticks)
        # A fixture whose staged files are all zero-row is invisible downstream, and
        # parse-time counters only show it on the run that parsed it. An unreadable
        # capture stays hidden until someone runs --refresh, so surface it every run.
        report.fixtures_empty += len(
            {_fixture_of(p) for p in files} - set(ticks["fixture_id"].unique().to_list())
        )
        ticks = ticks.join(xw_slim, on="fixture_id", how="inner").with_columns(
            pl.when(boundary.is_not_null() & (pl.col("fetched_at") <= boundary))
            .then(pl.lit(PREMATCH)).otherwise(pl.lit(IN_PLAY)).alias("event_status")
        )
        # One row per match. `unique()` alone would keep two rows if two fixtures of
        # the same match list the participants in opposite order, and the left join
        # below would then duplicate every quote with contradictory p1/p2.
        ids = (
            ticks.select("match_uid", "p1_id", "p2_id")
            .unique()
            .sort(["match_uid", "p1_id", "p2_id"])
            .group_by("match_uid", maintain_order=True)
            .agg(pl.col("p1_id").first(), pl.col("p2_id").first())
        )
        # Prematch only, and expanded to snapshots before the shared aggregator
        # sees it — see _snapshots_from_ticks for why the raw ticks would misprice.
        snaps = _snapshots_from_ticks(
            ticks.filter((pl.col("event_status") == PREMATCH)
                         & pl.col("odds").is_not_null())
        )
        _write_snapshot_parts(snaps, ids, start // REDUCE_BATCH)
        for role, min_books in MIN_BOOKS_FORMED.items():
            books = REFERENCE_BOOKS if role == "reference" else None
            side = (
                snaps.filter(pl.col("book").is_in(list(books)))
                if books is not None
                else snaps.filter(~pl.col("book").is_in(list(REFERENCE_BOOKS)))
            )
            if side.is_empty():
                continue
            counts = side.group_by(SERIES).agg(
                pl.col("book").n_unique().alias("n_books")
            )
            reduced = compute_open_close_odds(
                side, min_books_formed=min_books, extra_keys=["market", "points"],
            )
            if reduced.is_empty():
                continue
            out.append(
                reduced.rename({"player_id": "side"})
                .with_columns(pl.lit(role).alias("role"))
                .join(counts, on=["match_uid", "market", "points", "side"],
                      how="left", nulls_equal=True)
                .join(ids, on="match_uid", how="left")
                # n_unique() gives UInt32; QUOTE_SCHEMA declares Int64, and an
                # empty run writes the Int64 frame, so without the cast the two
                # cases produce files that will not concatenate.
                .with_columns(pl.col("n_books").cast(pl.Int64))
                .select(list(QUOTE_SCHEMA))
            )
        if (start // REDUCE_BATCH) % 10 == 0:
            logger.info(
                "reduced %d/%d matches (%d ticks -> %d snapshot rows this batch)",
                min(start + REDUCE_BATCH, len(todo)), len(todo), len(ticks), len(snaps),
            )

    _consolidate_snapshots(report)
    if report.snapshots:
        logger.info(
            "wrote %d snapshots across %d books -> %s",
            report.snapshots, len(report.snapshots_by_book), snapshots_dir(),
        )

    quotes = (
        pl.concat(out, how="vertical").sort(QUOTE_SORT)
        if out else pl.DataFrame(schema=QUOTE_SCHEMA)
    )
    # Atomic, for the same reason the staged ticks are: this sits on the shared B:/
    # drive and a kill mid-write would leave consumers reading a truncated aggregate.
    quotes_path().parent.mkdir(parents=True, exist_ok=True)
    tmp = quotes_path().with_suffix(".parquet.tmp")
    try:
        quotes.write_parquet(tmp, metadata={SCHEMA_META_KEY: QUOTES_SCHEMA_HASH})
        tmp.replace(quotes_path())
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    report.quotes = len(quotes)
    for r in quotes.group_by("market").agg(pl.len().alias("n")).iter_rows(named=True):
        report.quotes_by_market[r["market"]] = r["n"]
    for r in quotes.group_by("role").agg(pl.len().alias("n")).iter_rows(named=True):
        report.quotes_by_role[r["role"]] = r["n"]

    logger.info(
        "wrote %d quotes from %d ticks -> %s",
        len(quotes), report.ticks_parsed, quotes_path(),
    )
