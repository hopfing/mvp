"""Entry anchors: resolving "the open", "formed", "the close" to one moment per match.

Anchors must be **exogenous** -- fixed by the clock or by what books did, never by
model output. An edge-triggered entry ("first moment we see 5% edge") is adverse
selection: edge appears because the price is drifting out, so it fires early in a move
against the pick with better prices still to come. It also makes cross-model comparison
incoherent, since each model would enter at a different instant and sample a different
part of the price path.

The output of every anchor is a `(match_uid, t)` frame, which is exactly what
`board.board_at` consumes. So "what was on offer at the open" is
`board_at(anchor_times(..., "open"), market)` -- no new machinery.

**Why this is a sweep and not an order statistic.** A book that becomes two-sided can
stop being two-sided: it pulls a side. So "the second book to open" is NOT the moment
two books are simultaneously available -- the first may have gone one-sided by then.
Readiness is tracked as +1/-1 transitions and cumulatively summed, so `formed(n)` is
the first moment `n` books are genuinely live together.

Prices persist between ticks (the feed emits on change), so a book's state at any T is
its last transition at or before T. Books never tick in the same millisecond; that is a
fact about rows, not about availability.
"""

from __future__ import annotations

import polars as pl

from mvp.oddspapi import board, paths

TOTALS_SIDES = board.TOTALS_SIDES


def _rung_live_transitions(
    df: pl.DataFrame, over_side: str, under_side: str
) -> pl.DataFrame:
    """(match_uid, book, t, delta) as each rung becomes / stops being two-sided live.

    A side is live from a tick with active=True until a later tick says otherwise, so
    both sides' states are carried forward along the rung's own timeline; the rung is
    live where both are.
    """
    per_rung = (
        df.sort("match_uid", "book", "points", "fetched_at")
        .with_columns(
            pl.when(pl.col("side") == over_side).then(pl.col("active")).alias("_o"),
            pl.when(pl.col("side") == under_side).then(pl.col("active")).alias("_u"),
        )
        .with_columns(
            pl.col("_o").forward_fill().over("match_uid", "book", "points"),
            pl.col("_u").forward_fill().over("match_uid", "book", "points"),
        )
        .with_columns(
            (pl.col("_o").fill_null(False) & pl.col("_u").fill_null(False))
            .alias("live")
        )
        # collapse ticks that do not change the rung's live state
        .with_columns(
            pl.col("live").shift().over("match_uid", "book", "points").alias("_prev")
        )
        .filter(pl.col("live") != pl.col("_prev").fill_null(False))
    )
    return per_rung.select(
        "match_uid",
        "book",
        pl.col("fetched_at").alias("t"),
        pl.when(pl.col("live")).then(1).otherwise(-1).alias("delta"),
    )


def _book_ready_transitions(rung_tx: pl.DataFrame) -> pl.DataFrame:
    """(match_uid, book, t, delta) as each book gains / loses its last live rung.

    Deltas at an identical timestamp are summed before the running total, so a book
    swapping one rung for another in the same instant does not read as a gap.
    """
    counted = (
        rung_tx.group_by("match_uid", "book", "t")
        .agg(pl.col("delta").sum())
        .sort("match_uid", "book", "t")
        .with_columns(
            pl.col("delta").cum_sum().over("match_uid", "book").alias("n_live_rungs")
        )
        .with_columns((pl.col("n_live_rungs") > 0).alias("ready"))
    )
    return (
        counted.with_columns(
            pl.col("ready").shift().over("match_uid", "book").alias("_prev")
        )
        .filter(pl.col("ready") != pl.col("_prev").fill_null(False))
        .select(
            "match_uid",
            "book",
            "t",
            pl.when(pl.col("ready")).then(1).otherwise(-1).alias("delta"),
        )
    )


def _ready_book_count(book_tx: pl.DataFrame) -> pl.DataFrame:
    """(match_uid, t, n_books) -- how many books are live together, over time."""
    return (
        book_tx.group_by("match_uid", "t")
        .agg(pl.col("delta").sum())
        .sort("match_uid", "t")
        .with_columns(pl.col("delta").cum_sum().over("match_uid").alias("n_books"))
        .select("match_uid", "t", "n_books")
    )


def _empty_times() -> pl.DataFrame:
    """The `(match_uid, t)` contract, typed to match the non-empty path.

    `fetched_at` is tz-aware UTC on disk, so an empty frame built with a naive dtype
    cannot be concatenated with a real one.
    """
    return pl.DataFrame(
        schema={"match_uid": pl.String, "t": pl.Datetime("us", "UTC")}
    )


def _start_times(match_uids: pl.Series | None = None) -> pl.DataFrame:
    """(match_uid, t) match start, deduplicated deterministically.

    11 fixtures carry two distinct start times after the coalesce -- reschedules. The
    later one is kept, which is a choice rather than a known-correct answer, but an
    arbitrary one would make the anchor non-reproducible, which is the single property
    it exists to provide.
    """
    fm = pl.read_parquet(paths.stage_root() / "_fixture_map.parquet")
    col = "true_start_time" if "true_start_time" in fm.columns else "start_time"
    out = (
        fm.filter(pl.col("match_uid").is_not_null())
        .select("match_uid", pl.coalesce(col, "start_time").alias("t"))
        .drop_nulls("t")
        .group_by("match_uid")
        .agg(pl.col("t").max())
    )
    if match_uids is not None:
        out = out.filter(pl.col("match_uid").is_in(match_uids.implode()))
    return out.sort("match_uid")


def _load(
    market: str, books: list[str], uids: pl.Series | None, prematch_only: bool
) -> pl.DataFrame:
    """Ticks for the requested books.

    Note what the exclusions mean downstream: the sweep forward-fills, so a row dropped
    here for `odds <= 1.0` or a non-prematch status reads as "state unchanged", not as
    a gap. That is inert on the current corpus (only bet365 writes unpriced rows, and
    treating them as explicit not-live changes `formed` for zero matches), but it would
    matter if a book adopted a zero-odds convention for pulling a line.
    """
    frames = []
    for bk in books:
        p = board.market_path(bk, market)
        if not p.exists():
            continue
        lf = pl.scan_parquet(p).filter(
            pl.col("match_uid").is_not_null() & (pl.col("odds") > 1.0)
        )
        if prematch_only:
            lf = lf.filter(pl.col("event_status") == "NOT_STARTED")
        if uids is not None:
            lf = lf.filter(pl.col("match_uid").is_in(uids.implode()))
        frames.append(
            lf.select("match_uid", "points", "side", "fetched_at", "active")
            .collect()
            .with_columns(pl.lit(bk).alias("book"))
        )
    return pl.concat(frames) if frames else pl.DataFrame()


def formed(
    market: str,
    n_books: int = 2,
    *,
    books: list[str] | None = None,
    match_uids: pl.Series | None = None,
    prematch_only: bool = True,
    over_side: str = TOTALS_SIDES[0],
    under_side: str = TOTALS_SIDES[1],
) -> pl.DataFrame:
    """First moment `n_books` entry books are simultaneously two-sided.

    Returns `(match_uid, t)`.

    `n_books` is the point: formation is a threshold on a count, not a fixed rule, so
    1, 2, 3, 4 are all askable. `formed(market, 1)` is the open.

    Matches that never reach `n_books` are absent from the result rather than carrying a
    null -- an anchor that did not occur is not a time.
    """
    if n_books < 1:
        raise ValueError("n_books must be >= 1")
    bks = books if books is not None else board.entry_books(market)
    df = _load(market, bks, match_uids, prematch_only)
    if df.is_empty():
        return _empty_times()
    counts = _ready_book_count(
        _book_ready_transitions(_rung_live_transitions(df, over_side, under_side))
    )
    return (
        counts.filter(pl.col("n_books") >= n_books)
        .group_by("match_uid")
        .agg(pl.col("t").min())
        .sort("match_uid")
    )


def open_(market: str, **kw) -> pl.DataFrame:
    """First moment any entry book is two-sided -- the first bettable price."""
    return formed(market, 1, **kw)


# Lead time for `close`. Books tear the board down at the off, and sampling at the
# start catches them mid-teardown: `live=False` runs 36.8% of two-sided rows at the
# start against 16.0% thirty minutes earlier. The knee is between 5 and 10 minutes --
# 34.4% at 5m, 20.4% at 10m, 17.0% at 15m -- and past 15m it is a plateau, 15.6% at
# 45m and 13.8% at four hours. 15 minutes also sits just outside the late-repricing
# flurry: median quote age jumps from 8.2 min at 5m out to 106.3 min at 10m, so books
# do nearly all their final repricing inside the last ten minutes.
CLOSE_LEAD_MINUTES = 15


def close(
    market: str | None = None,
    *,
    lead_minutes: int = CLOSE_LEAD_MINUTES,
    match_uids: pl.Series | None = None,
) -> pl.DataFrame:
    """The last moment a bet could realistically have been placed. `(match_uid, t)`.

    A fixed lead before the scheduled start, not the last instant a price existed.
    Nobody transacts at the moment the market shuts, and sampling there measures
    whatever happens to still be up at the last second -- a degenerate state rather
    than a last opportunity.

    This makes `close` exogenous, like `offset`: it depends on the clock and the
    fixture, never on what books did. An earlier definition resolved to the last
    two-sided interval and fell back to the match start, which landed exactly on the
    start for 96.8% of matches and read the board mid-teardown.

    Books that pulled before this instant simply are not live at it, which
    `board_at`'s `live` flag already expresses -- they are absent from the board
    rather than absent from the anchor.
    """
    del market  # kept for call-site symmetry with the other anchors
    return offset(lead_minutes / 60.0, match_uids=match_uids)


def offset(
    hours_before_start: float, *, match_uids: pl.Series | None = None
) -> pl.DataFrame:
    """`true_start_time` minus a duration, per match. `(match_uid, t)`.

    The only anchor that does not depend on what books did, which is what makes it
    comparable across matches whose book behaviour differs. Matches with no start time
    in the fixture map are dropped rather than defaulted.
    """
    return (
        _start_times(match_uids)
        .with_columns(
            (pl.col("t") - pl.duration(seconds=int(hours_before_start * 3600)))
            .alias("t")
        )
        .sort("match_uid")
    )
