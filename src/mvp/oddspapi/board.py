"""Read-time reconstruction of the board from the oddspapi stage tree.

Nothing here is written to disk. The per-book stage files are the datasets; these are
the functions that turn a tick stream into "what was on offer at time T".

**The query is keyed on time, not on book.** Ticks exist only where a price moved, so
asking about an arbitrary moment means taking each rung's most recent quote at or
before it. `board_at` does that across every book at once. Main line is still computed
within a book -- its own ladder, its own prices -- but that is a column on the result
rather than the unit of the query. Anchors (open, formed, close, fixed offsets) are
just ways of choosing T.

Carrying a last known price forward is safe at these horizons: measured against
pinnacle's near-off fair on the same line, |dev| p50 runs 0.014-0.023 and does not grow
with quote age out past 24h. Emit-on-change means an old timestamp is a price that has
not moved, not a decayed one. `quote_age_s` is on every row so a caller can gate anyway.

**Named-outcome markets only.** Sides here are `Over`/`Under`. Participant-sided markets
(`game_spread`, `moneyline`) side positionally -- `"1"` means whoever the feed listed
first -- and must be oriented through `side_player_id`, never through the sign of
`points`: on this feed -2.5 and +2.5 are distinct rungs rather than mirrored views of
one. That orientation is not implemented, so those markets raise rather than silently
produce a board keyed to the wrong player.
"""

from __future__ import annotations

import polars as pl

from mvp.oddspapi import paths

TOTALS_SIDES = ("Over", "Under")

BOARD_SCHEMA: dict[str, pl.DataType] = {
    "match_uid": pl.String,
    "book": pl.String,
    "points": pl.Float64,
    "over_odds": pl.Float64,
    "under_odds": pl.Float64,
    "p_over": pl.Float64,
    "overround": pl.Float64,
    "imbalance": pl.Float64,
    "live": pl.Boolean,
    "live_over": pl.Boolean,
    "live_under": pl.Boolean,
    "quote_age_s": pl.Int64,
    "last_change_s": pl.Int64,
    "n_two_sided": pl.UInt32,
    "is_main_line": pl.Boolean,
    "separation": pl.Float64,
}


def market_path(book: str, market: str):
    return paths.stage_root() / book / f"{market}.parquet"


def available_books(market: str) -> list[str]:
    """Books carrying this market.

    betmgm has no `total_games` -- only a first-set total.
    """
    return [b for b in paths.ALL_BOOKS if market_path(b, market).exists()]


def entry_books(market: str) -> list[str]:
    """Books whose prices are takeable. Roles live in `paths`, not here -- a book is
    classified in one place so reclassifying it cannot leave this module behind."""
    return [b for b in available_books(market) if b in paths.ENTRY_BOOKS]


def _scan(book: str, market: str, *, prematch_only: bool) -> pl.LazyFrame:
    lf = pl.scan_parquet(market_path(book, market)).filter(
        pl.col("match_uid").is_not_null() & (pl.col("odds") > 1.0)
    )
    if prematch_only:
        lf = lf.filter(pl.col("event_status") == "NOT_STARTED")
    return lf


def excluded_rows(book: str, market: str) -> dict[str, int]:
    """Rows `_scan` drops, by reason, so a caller can log rather than lose them.

    `event_status` is null exactly where the matcher could not resolve the fixture, so
    an unresolved fixture and a missing status are the same population.
    """
    lf = pl.scan_parquet(market_path(book, market))
    return {
        "unresolved_match_uid": lf.filter(pl.col("match_uid").is_null())
        .select(pl.len()).collect().item(),
        "odds_not_above_one": lf.filter(pl.col("odds") <= 1.0)
        .select(pl.len()).collect().item(),
    }


def _latest_per_side(
    book: str, market: str, times: pl.DataFrame, *, prematch_only: bool
) -> pl.DataFrame:
    """Each (match, rung, side)'s most recent quote at or before its match's T."""
    uids = times["match_uid"]
    return (
        _scan(book, market, prematch_only=prematch_only)
        .filter(pl.col("match_uid").is_in(uids.implode()))
        .select("match_uid", "points", "side", "odds", "fetched_at", "active")
        .collect()
        .join(times, on="match_uid", how="inner")
        .filter(pl.col("fetched_at") <= pl.col("t"))
        .group_by("match_uid", "points", "side")
        .agg(
            pl.col("odds").sort_by("fetched_at").last(),
            pl.col("active").sort_by("fetched_at").last(),
            pl.col("fetched_at").max().alias("quoted_at"),
            pl.col("t").first(),
        )
    )


def _pivot_two_sided(
    per_side: pl.DataFrame, over_side: str, under_side: str
) -> pl.DataFrame:
    """One row per rung: both sides, de-vigged, imbalance.

    One-sided rungs are KEPT with null `p_over`/`imbalance` -- a book showing one side
    is a market event, not noise -- but cannot be main line, since balance is a property
    of the pair.

    Two ages, because they answer different questions. `quote_age_s` is taken from the
    STALEST leg: a de-vigged pair is only as current as its older side, so that is the
    number a staleness gate wants. `last_change_s` is the freshest leg -- when anything
    on this rung last moved.
    """
    seen = set(per_side["side"].unique().to_list())
    if not seen <= {over_side, under_side}:
        raise ValueError(
            f"sides {sorted(seen - {over_side, under_side})} are not "
            f"{over_side}/{under_side}. Participant-sided markets need orientation "
            "through side_player_id, which this module does not implement."
        )

    wide = per_side.pivot(
        on="side",
        index=["match_uid", "points", "t"],
        values=["odds", "quoted_at", "active"],
        aggregate_function="first",
    )
    # A rung quoted on one side only produces no columns for the missing side, and a
    # frame where no rung has a side produces none at all. Both are normal (one-sided
    # quotes run 4-17% of rows by book), so materialise them rather than letting the
    # pivot's shape decide whether this works.
    for side in (over_side, under_side):
        for prefix, dtype in (
            ("odds", pl.Float64),
            ("quoted_at", wide.schema["t"]),
            ("active", pl.Boolean),
        ):
            col = f"{prefix}_{side}"
            if col not in wide.columns:
                wide = wide.with_columns(pl.lit(None, dtype=dtype).alias(col))

    o, u = f"odds_{over_side}", f"odds_{under_side}"
    inv_o, inv_u = 1 / pl.col(o), 1 / pl.col(u)
    both = pl.col(o).is_not_null() & pl.col(u).is_not_null()
    # Liveness is PER SIDE. A single AND over both legs makes a one-sided rung
    # `live=False`, which a downstream bet filter then removes -- so the one-sided
    # handling in `pricing.bets` became unreachable from real board output (measured:
    # 111 one-sided rows reached settle, 0 survived). `live` stays the AND because
    # main-line selection needs a live PAIR to compare balance on; the per-side flags
    # are what a bet filter should read.
    live_over = pl.col(f"active_{over_side}").fill_null(False)
    live_under = pl.col(f"active_{under_side}").fill_null(False)
    live = live_over & live_under
    qo, qu = f"quoted_at_{over_side}", f"quoted_at_{under_side}"

    return (
        wide.with_columns(
            pl.when(both).then(inv_o / (inv_o + inv_u)).alias("p_over"),
            pl.when(both).then(inv_o + inv_u).alias("overround"),
            live.alias("live"),
            live_over.alias("live_over"),
            live_under.alias("live_under"),
            (pl.col("t") - pl.min_horizontal(qo, qu))
            .dt.total_seconds().alias("quote_age_s"),
            (pl.col("t") - pl.max_horizontal(qo, qu))
            .dt.total_seconds().alias("last_change_s"),
        )
        .with_columns((pl.col("p_over") - 0.5).abs().alias("imbalance"))
        .rename({o: "over_odds", u: "under_odds"})
        .select(
            "match_uid", "points", "over_odds", "under_odds", "p_over", "overround",
            "imbalance", "live", "live_over", "live_under",
            "quote_age_s", "last_change_s",
        )
    )


def _flag_main_line(df: pl.DataFrame) -> pl.DataFrame:
    """Flag each book's main line: the live two-sided rung priced closest to balanced.

    De-vigged, so it reads the book's opinion rather than its margin structure. That
    changes ~0.4% of picks versus raw-odds balance: correct, not material.

    Ties are broken by the caller's row order, so the input MUST already be sorted --
    `board_at` sorts by (match_uid, book, points), making the tie-break "lowest rung"
    and the result reproducible. Without that, group order out of `_latest_per_side` is
    genuinely unstable and identical calls disagree.

    `n_two_sided` is what distinguishes a main line that was SELECTED from one that
    merely names the only rung priced -- at bet365 and fanduel most prematch moments
    carry a single two-sided rung, so the flag is a tautology unless this is read too.
    `separation` is the runner-up's imbalance minus the pick's, null when there was
    nothing to beat.
    """
    grp = ["match_uid", "book"]
    eligible = pl.col("imbalance").is_not_null() & pl.col("live")
    ranked = df.with_columns(
        pl.when(eligible).then(pl.col("imbalance"))
        .rank("ordinal").over(grp).alias("_rk"),
        eligible.sum().over(grp).alias("n_two_sided"),
    )
    runner_up = ranked.filter(pl.col("_rk") == 2).select(
        *grp, pl.col("imbalance").alias("_next")
    )
    return (
        ranked.join(runner_up, on=grp, how="left")
        .with_columns((pl.col("_rk") == 1).fill_null(False).alias("is_main_line"))
        .with_columns(
            pl.when(pl.col("is_main_line"))
            .then(pl.col("_next") - pl.col("imbalance"))
            .alias("separation")
        )
        .drop("_rk", "_next")
    )


def board_at(
    times: pl.DataFrame,
    market: str,
    *,
    books: list[str] | None = None,
    prematch_only: bool = True,
    over_side: str = TOTALS_SIDES[0],
    under_side: str = TOTALS_SIDES[1],
) -> pl.DataFrame:
    """What every book had on the board, per match, at that match's time T.

    `times` is a frame of (match_uid, t) -- one T per match, so an absolute instant and
    a per-match anchor (T-1h, the close) use the same call. One row per match is
    enforced: a duplicated `match_uid` would fan out the join and then collapse both
    anchors into one board, silently losing the earlier one.

    Returns one row per (match_uid, book, points), `BOARD_SCHEMA`-shaped even
    when empty.
    """
    missing = {"match_uid", "t"} - set(times.columns)
    if missing:
        raise ValueError(f"times is missing {sorted(missing)}")
    if times["match_uid"].is_duplicated().any():
        raise ValueError(
            "times has duplicate match_uid; board_at takes one T per match"
        )

    frames = []
    for book in books or available_books(market):
        per_side = _latest_per_side(book, market, times, prematch_only=prematch_only)
        if per_side.is_empty():
            continue
        frames.append(
            _pivot_two_sided(per_side, over_side, under_side).with_columns(
                pl.lit(book).alias("book")
            )
        )
    if not frames:
        return pl.DataFrame(schema=BOARD_SCHEMA)
    # Sort BEFORE ranking: _flag_main_line breaks imbalance ties on row order.
    ordered = pl.concat(frames).sort("match_uid", "book", "points")
    return _flag_main_line(ordered).select(BOARD_SCHEMA.keys())


def coverage_and_agreement(board: pl.DataFrame) -> pl.DataFrame:
    """How many books posted a main line, and how those lines distribute.

    **Grain is `(match_uid, points)`, not per match.** Joining it back to a board on
    `match_uid` alone fans the board out by the number of distinct main lines in the
    match (measured: 30,636 rows to 43,740). Join on `(match_uid, points)`, which is
    the point -- support attaches to the bet row, not to the match.

    Kept as separate columns and never divided. Five books on five different lines is
    the market disagreeing; no book posting is the absence of information. A ratio
    cannot tell those apart, and it flattens 3/3 against 5/5.
    """
    schema = {
        "match_uid": pl.String, "points": pl.Float64, "n_books_on_line": pl.UInt32,
        "books": pl.List(pl.String), "n_books_live": pl.UInt32,
        "line_spread": pl.Float64,
    }
    if board.is_empty() or "is_main_line" not in board.columns:
        return pl.DataFrame(schema=schema)
    mains = board.filter(pl.col("is_main_line"))
    if mains.is_empty():
        return pl.DataFrame(schema=schema)
    support = mains.group_by("match_uid", "points").agg(
        pl.col("book").n_unique().alias("n_books_on_line"),
        pl.col("book").sort().alias("books"),
    )
    coverage = mains.group_by("match_uid").agg(
        pl.col("book").n_unique().alias("n_books_live"),
        (pl.col("points").max() - pl.col("points").min()).alias("line_spread"),
    )
    return support.join(coverage, on="match_uid", how="left").sort(
        "match_uid", "n_books_on_line", descending=[False, True]
    )
