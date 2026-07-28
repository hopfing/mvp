"""raw/oddspapi -> stage/oddspapi/ticks -> stage/oddspapi/<book>/<market>.parquet.

Reshapes captured ticks to the stage contract:

    price -> odds        createdAt -> fetched_at        handicap -> points

Two layers, each with a different lifetime:

  PARSE     one raw JSON -> one parquet under stage/oddspapi/ticks/<group>/,
            keyed by fixture_id, no match resolution. Expensive (~10 min over
            35 GB) and rare, skipped per file on the rule atptour stages on
            (`atptour/pipeline.py:131-148`): staged file missing, raw newer than
            staged, or the parser's schema hash changed.
  ORGANISE  staged ticks -> stage/oddspapi/<book>/<market>.parquet, one row per
            captured tick, with `match_uid` joined and `event_status` derived. A
            transpose: input partitioned by capture file with books mixed inside,
            output partitioned by book and market with captures mixed inside.
            Nothing is aggregated or filtered, so every output row exists in the
            input. Cheap and rebuildable — 47s over the full tree — because the
            matcher keeps improving and this is a re-join, not a reparse.
There was a third, `aggregate/oddspapi/quotes.parquet`: best-across-books at open,
formed and close per (match_uid, market, points, side, role). It has been removed.
It spent book identity on the aggregation and the time axis on three moments, and
both are the things a per-book consumer needs — cross-book comparison belongs at
read time, over these files, not frozen into a fourth artifact.

Everything captured is kept, at both layers. No prematch cut, no `active` cut —
those are columns, and filtering here would make the dropped universe unrecoverable
without a full reparse. In-play is not a rounding error: it is 98% of the corpus
(betrivers/total_games is 10.8M in-play against 226k prematch), and the chain is a
score-state model that prices from any score, so live pricing is the market it has
the strongest claim on.

`event_status` is derived, because the ticks carry no match state: a historical
payload is only {fixtureId, bookmakers} and a tick only
{createdAt, price, limit, active, exchangeMeta}. The boundary is the fixture's
`trueStartTime` where known, falling back to the scheduled `startTime` — 48% of
fixtures begin more than 5 minutes late, so cutting on the schedule would file
genuinely prematch quotes as in-play.

Memory is bounded by the streaming engine, not by the corpus and not by a
hand-picked batch size: the organise step scans lazily and lets polars partition
the write.
"""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from polars.io.partition import PartitionBy
import pyarrow as pa
import pyarrow.parquet as pq

from mvp.oddspapi import markets, matcher, parser
from mvp.oddspapi.paths import (
    book_dir,
    ensure_dirs,
    historical_dirs,
    role_of,
    stage_root,
    ticks_dir,
    ticks_path,
    ticks_schema_marker,
)
from mvp.oddspapi.schemas.ticks import SCHEMA_HASH as TICKS_SCHEMA_HASH

logger = logging.getLogger(__name__)

# Fail rather than silently shrink coverage: the original transform printed the
# unmatched count and carried on, so a broken matcher looked like a thin season.
MAX_UNMATCHED_FRACTION = 0.25

PREMATCH = "NOT_STARTED"
IN_PLAY = "IN_PLAY"

SCHEMA_META_KEY = "pydantic_schema_hash"

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

# One row per captured tick. Close to a scraper's stage file — `match_uid` stands
# where a scraper carries its own event id, since the crosswalk resolved identity —
# but it carries `active` and `limit`, which the scrapers do not, because nothing
# here filters on them. `exchange_meta` is dropped: 0% populated across 16M sampled
# ticks, and the ticks tree keeps it regardless.
MARKET_SCHEMA: dict[str, pl.DataType] = {
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
    "active": pl.Boolean,
    "limit": pl.Float64,
}

XW_COLS = ["fixture_id", "match_uid", "p1_id", "p2_id", "start_time", "true_start_time"]


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
    rows: int = 0
    rows_by_book: dict[str, int] = field(default_factory=dict)

    def summary(self) -> str:
        return (
            f"{self.rows:,} rows across {len(self.rows_by_book)} books "
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
    """Parse new raw files, then rebuild the per-book stage files.

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

    _organise(xw, report)
    logger.info(report.summary())
    return report


def _fixture_of(path: Path) -> str:
    """Fixture id from a staged filename (raw files are historical_<fixtureId>)."""
    return path.stem.removeprefix("historical_")


def _swap_books_in(new_root: Path, report: TransformReport) -> None:
    """Move each freshly written book directory into the source root.

    ONE BOOK AT A TIME. The books sit directly under stage_root(), alongside
    `ticks/` (16.5k staged files, 35 GB of raw behind them) and
    `_crosswalk.parquet`, so renaming "the layer" aside would take those with it and
    the cleanup below would delete them. Never rename stage_root().

    Renaming the live directory aside before renaming the new one in keeps the
    window where a book has no directory one rename wide rather than a whole
    rewrite. B:/ is shared with the live pipeline; a reader mid-write is a real
    event here. Consumers read one book at a time, so seeing book A refreshed and
    book B not is the same as reading them a second apart.
    """
    for staged in sorted(p for p in new_root.iterdir() if p.is_dir()):
        book = staged.name
        live = book_dir(book)
        old = stage_root() / f".{book}.old"
        shutil.rmtree(old, ignore_errors=True)
        try:
            if live.exists():
                live.rename(old)
            staged.rename(live)
        finally:
            shutil.rmtree(old, ignore_errors=True)
        report.rows_by_book[book] = sum(
            pq.read_metadata(f).num_rows for f in live.glob("*.parquet")
        )
        report.rows += report.rows_by_book[book]


def _organise(xw: pl.DataFrame, report: TransformReport) -> None:
    """Staged ticks -> stage/oddspapi/<book>/<market>.parquet, in one streaming pass.

    A transpose, not a reduction. The input is partitioned by capture file with up
    to three books mixed inside; the output is partitioned by book and market with
    every capture mixed inside. Nothing is aggregated, filtered or interpolated —
    every output row exists in the input, and the only additions are `match_uid`,
    `p1_id`, `p2_id` from the crosswalk and a derived `event_status`.

    An earlier version reduced here: 15-minute buckets, a forward-filled grid of
    each book's standing price, a carry cap, and prematch-only. All of it existed to
    hand `compute_open_close_odds` a grid it already understood, for an aggregate
    that has since been deleted. It cost 21x the rows on total_games while 96% of
    price changes fell inside a bucket, and on thin books it invented rows — bet365
    carried 44,774 filled rows built from 33,828 actual ticks.

    polars partitions the write, so memory is bounded by the streaming engine rather
    than by a batch size picked by hand. Measured over the full tree: 16,535 files
    to 117 output files in 47s, 141M rows, 671 MB.
    """
    by_fixture: dict[str, list[Path]] = {}
    for p in _staged_index():
        by_fixture.setdefault(_fixture_of(p), []).append(p)
    if not by_fixture:
        logger.warning("no staged ticks to organise")
        return
    report.fixtures_staged = len(by_fixture)

    resolved = set(xw["fixture_id"].to_list())
    unmatched = sum(1 for f in by_fixture if f not in resolved)
    report.fixtures_unmatched = unmatched
    frac = unmatched / max(len(by_fixture), 1)
    if frac > MAX_UNMATCHED_FRACTION:
        raise RuntimeError(
            f"{unmatched} of {len(by_fixture)} staged fixtures did not resolve to a "
            f"match_uid ({frac:.1%} > {MAX_UNMATCHED_FRACTION:.0%}). Refusing to "
            "write a silently thin layer — check the matcher (stale fixtures? "
            "bad aliases?)."
        )

    # fixture -> match_uid is not injective: 38 match_uids carry more than one
    # captured fixture (superseded events, one with three), and two fixtures of the
    # same match can list the participants in opposite order. Taking p1/p2 from the
    # fixture row would then give one match contradictory ids, so they are
    # canonicalised per match first and joined on match_uid.
    per_match = (
        xw.select("match_uid", "p1_id", "p2_id")
        .unique()
        .sort(["match_uid", "p1_id", "p2_id"])
        .group_by("match_uid", maintain_order=True)
        .agg(pl.col("p1_id").first(), pl.col("p2_id").first())
    )
    report.matches_multi_fixture = (
        xw.group_by("match_uid").len().filter(pl.col("len") > 1).height
    )
    fixtures = xw.select("fixture_id", "match_uid", "start_time", "true_start_time")

    files = [str(p) for p in sorted(_staged_index())]
    scan = pl.scan_parquet(files)
    # One cheap single-column pass for the counters. A fixture whose staged files
    # are all zero-row is invisible downstream, and parse-time counters only show it
    # on the run that parsed it, so it is surfaced every run.
    stats = scan.select(
        pl.len().alias("ticks"), pl.col("fixture_id").n_unique().alias("fixtures")
    ).collect(engine="streaming")
    report.ticks_parsed = int(stats["ticks"][0])
    report.fixtures_empty = report.fixtures_staged - int(stats["fixtures"][0])

    boundary = pl.coalesce([pl.col("true_start_time"), pl.col("start_time")])
    lf = (
        # LEFT, not inner. An inner join drops every tick of a fixture the crosswalk
        # cannot resolve — 290 fixtures and ~3.9M ticks on the current corpus — and
        # makes the dropped universe invisible without going back to the ticks tree.
        # Same reason nothing here cuts on prematch or `active`: an unresolved
        # fixture is a null match_uid to filter, not an absence.
        scan.join(fixtures.lazy(), on="fixture_id", how="left")
        .join(per_match.lazy(), on="match_uid", how="left")
        .with_columns(
            # Three-way, not two. With no crosswalk row there is no boundary, and
            # `otherwise` would label those IN_PLAY — asserting a match state we do
            # not know. Unknown is null.
            pl.when(boundary.is_null())
            .then(pl.lit(None, dtype=pl.Utf8))
            .when(pl.col("fetched_at") <= boundary)
            .then(pl.lit(PREMATCH))
            .otherwise(pl.lit(IN_PLAY))
            .alias("event_status")
        )
        .select(list(MARKET_SCHEMA))
    )

    logger.info(
        "organising %d ticks from %d fixtures (%d unmatched, %d matches with "
        "multiple fixtures)",
        report.ticks_parsed, report.fixtures_staged, unmatched,
        report.matches_multi_fixture,
    )

    # Written aside and renamed in, so a reader on the shared B:/ never sees a
    # half-written file. A killed run leaves this behind; it is cleared on the next.
    new_root = stage_root() / ".new"
    shutil.rmtree(new_root, ignore_errors=True)
    new_root.mkdir(parents=True, exist_ok=True)

    def _dest(args) -> Path:
        key = args.partition_keys.row(0, named=True)
        return new_root / key["book"] / f"{key['market']}.parquet"

    try:
        lf.sink_parquet(
            # PartitionBy is unstable API in polars 1.39. It is confined to this
            # call: if it changes, only this function does. The size-based defaults
            # are switched off — a book x market must be exactly one file, not
            # however many the writer felt like.
            PartitionBy(
                base_path=new_root, key=["book", "market"], include_key=True,
                file_path_provider=_dest,
                max_rows_per_file=None, approximate_bytes_per_file=None,
            ),
            engine="streaming",
        )
        _swap_books_in(new_root, report)
    finally:
        shutil.rmtree(new_root, ignore_errors=True)

    if report.rows:
        logger.info(
            "wrote %d rows across %d books -> %s",
            report.rows, len(report.rows_by_book), stage_root(),
        )
