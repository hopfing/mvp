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

**Two side vocabularies, and the conversion between them is not optional.**
Named-outcome markets (`total_games`) carry `Over`/`Under` and pass through untouched.
Participant-sided markets (`game_spread`, `moneyline`, and 11 more the reference
classifies) side POSITIONALLY: `"1"` is whoever the feed listed first, which agrees
with the projection's ordering on only about half of fixtures. Those are re-sided here
to `a`/`b` -- a is the lower `player_id`, read off `match_uid` -- and emitted with
`player_id_a` / `player_id_b` naming which player each slot holds.

`points` is converted with them, and the two corrections are independent. On the feed
it is a HEAD START added to participant 1, so `"1"` at `points=h` covers iff that
player's margin exceeds `-h`. The board emits it as a THRESHOLD on `games_a - games_b`,
the frame the projection is in. Relabelling sides without re-signing attributes each
price to the right player at a backwards line, and no downstream invariant catches it:
`p_a + p_push + p_b == 1` holds either way. `-2.5` and `+2.5` remain distinct rungs
rather than mirrored views of one, in both frames.
"""

from __future__ import annotations

import functools
import logging

import polars as pl

from mvp.oddspapi import paths

logger = logging.getLogger(__name__)

TOTALS_SIDES = ("Over", "Under")

# The feed's positional outcomes. `"1"` is whoever the feed listed first -- NOT a
# player we have identified, and NOT the a/b ordinal a caller may derive from
# `side_player_id`. Anything reading the STAGE FILES sees these; anything reading
# an oriented board does not.
PARTICIPANT_SIDES = ("1", "2")

# Offline floor for `_participant_markets`. The reference lives on the data drive
# and classifies all 14 participant-sided markets; these two are the ones this
# repo actually prices, so a missing reference degrades rather than breaks.
PARTICIPANT_MARKETS_FLOOR = frozenset({"game_spread", "moneyline"})


@functools.lru_cache(maxsize=1)
def _participant_markets() -> frozenset[str]:
    """Stage names whose sides are the fixture's participants.

    DERIVED, not hardcoded. `markets.participant_sided_markets` reads the feed's
    own reference and finds 14 -- `spreads_result`, `moneyline_p1..p5`,
    `spreads_games_p1..p5`, `moneyline_aces_result` beyond the two priced here --
    and the distinction is not inferable from the side value: `exactsets_result`
    carries outcomes "2".."5" for the number of sets, so a bare `"2"` there is a
    set count rather than participant two.

    A hardcoded pair would hand every unlisted participant market `Over`/`Under`,
    which matches no rung, so `open` and `formed` return zero times and the ledger
    is silently empty -- the failure `_anchor_times` was fixed to remove.
    """
    try:
        from mvp.oddspapi.markets import participant_sided_markets

        return frozenset(participant_sided_markets())
    except OSError as exc:  # FileNotFoundError is a subclass
        logger.warning(
            "markets reference unavailable (%s); falling back to %s. Markets "
            "outside that set will be treated as named-outcome.",
            exc, sorted(PARTICIPANT_MARKETS_FLOOR),
        )
        return PARTICIPANT_MARKETS_FLOOR


def feed_sides(market: str) -> tuple[str, str]:
    """The side literals this market's stage files actually carry.

    `markets.STAGE_NAMES` splits three markets two ways: `total_games` is
    named-outcome (`Over`/`Under`), while `game_spread` and `moneyline` are
    participant-sided and carry the feed's positional `"1"`/`"2"`.

    **This is the FEED's vocabulary, not an oriented one.** Callers that work off
    the stage files -- `anchors`, which decides two-sidedness by matching the
    `side` column -- need exactly this. A caller that has oriented a board to
    player identity has its own side pair and must not use this one; passing an
    oriented pair here silently matches nothing, because `anchors._load` reads
    `side` straight off the parquet and never sees a derived ordinal.
    """
    return PARTICIPANT_SIDES if market in _participant_markets() else TOTALS_SIDES

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
    # Which player each positional slot holds. Named `_a`/`_b` rather than
    # `over_`/`under_` like the price columns: those are legacy slot names that
    # never escape (`unpivot_sides` collapses them), whereas these appear on the
    # board itself, and "the over player" means nothing on a spread.
    # Null for named-outcome
    # markets. Required, not optional: `board_at` ends with
    # `.select(BOARD_SCHEMA.keys())`, so anything absent here is dropped on the
    # way out however carefully it was carried through the pivot -- and without
    # it a spread ledger cannot say who a bet is on.
    "player_id_a": pl.String,
    "player_id_b": pl.String,
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
    """Each (match, rung, side)'s most recent quote at or before its match's T.

    `side_player_id` rides along for participant-sided markets, where it is the
    only thing that says which player a positional side belongs to. It is
    constant per (match, side) on this feed, so taking the latest is a formality
    rather than a choice.
    """
    uids = times["match_uid"]
    cols = ["match_uid", "points", "side", "odds", "fetched_at", "active"]
    has_player = "side_player_id" in pl.scan_parquet(
        market_path(book, market)
    ).collect_schema().names()
    if has_player:
        cols.append("side_player_id")
    aggs = [
        pl.col("odds").sort_by("fetched_at").last(),
        pl.col("active").sort_by("fetched_at").last(),
        pl.col("fetched_at").max().alias("quoted_at"),
        pl.col("t").first(),
    ]
    if has_player:
        aggs.append(pl.col("side_player_id").sort_by("fetched_at").last())
    return (
        _scan(book, market, prematch_only=prematch_only)
        .filter(pl.col("match_uid").is_in(uids.implode()))
        .select(*cols)
        .collect()
        .join(times, on="match_uid", how="inner")
        .filter(pl.col("fetched_at") <= pl.col("t"))
        .group_by("match_uid", "points", "side")
        .agg(*aggs)
    )


ORIENTED_SIDES = ("a", "b")


def _orient_participant_sides(
    per_side: pl.DataFrame, over_side: str, under_side: str
) -> pl.DataFrame:
    """Rewrite feed-positional rows into the projection's a/b frame.

    Two independent corrections, both required, and each one alone leaves a
    plausible board:

    **Identity.** The feed's `"1"` is whoever it listed first, which is unrelated
    to the projection's ordering. `a` is the lower `player_id`, and `match_uid`
    encodes that pair in sorted order -- its fifth underscore-separated component
    IS `min(player_id, opp_id)` on every singles match -- so the ordinal is
    derivable here with no roster join. Measured, the feed's first participant is
    the a side on only about 51% of fixtures, so this relabels roughly half the
    board.

    **Sign.** `points` is a HEAD START added to the feed's first participant, not
    a threshold on their margin: side `"1"` at `points=h` covers iff
    `margin(that player) > -h`. The ledger needs a threshold on `games_a -
    games_b`, which is the frame the pmf is in. With `h_row` the handicap given to
    the row's OWN player, the threshold is `-h_row` on the a row and `+h_row` on
    the b row -- identical on both sides of a rung by construction, which is what
    makes "both outcome rows carry the same points" structural.

    Reading `points` as a threshold instead inverts the line on every rung, and no
    ledger invariant catches it: `p_a + p_push + p_b == 1` holds either way.
    """
    a_id = pl.col("match_uid").str.split("_").list.get(4)
    is_a = pl.col("side_player_id") == a_id
    # the handicap this row's own player receives
    h_row = (
        pl.when(pl.col("side") == over_side)
        .then(pl.col("points"))
        .otherwise(-pl.col("points"))
    )
    return per_side.with_columns(
        pl.when(is_a)
        .then(pl.lit(ORIENTED_SIDES[0]))
        .otherwise(pl.lit(ORIENTED_SIDES[1]))
        .alias("side"),
        pl.when(is_a).then(-h_row).otherwise(h_row).alias("points"),
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
            f"{over_side}/{under_side}. Participant-sided markets are re-sided to "
            f"{ORIENTED_SIDES} by `_orient_participant_sides` before reaching here, "
            "so this fires only for a named-outcome market whose stage rows carry "
            "an unexpected side value."
        )

    # `side_player_id` must be in `values`: the pivot DROPS any column that is
    # neither pivoted on nor in `index`, so adding it to BOARD_SCHEMA alone would
    # not carry it -- it would already be gone by then. Two columns out, not one,
    # because the board's grain is the RUNG and a rung has two players.
    values = ["odds", "quoted_at", "active"]
    carries_player = "side_player_id" in per_side.columns
    if carries_player:
        values.append("side_player_id")
    wide = per_side.pivot(
        on="side",
        index=["match_uid", "points", "t"],
        values=values,
        aggregate_function="first",
    )
    # A rung quoted on one side only produces no columns for the missing side, and a
    # frame where no rung has a side produces none at all. Both are normal (one-sided
    # quotes run 4-17% of rows by book), so materialise them rather than letting the
    # pivot's shape decide whether this works.
    materialise = [
        ("odds", pl.Float64),
        ("quoted_at", wide.schema["t"]),
        ("active", pl.Boolean),
    ]
    if carries_player:
        materialise.append(("side_player_id", pl.String))
    for side in (over_side, under_side):
        for prefix, dtype in materialise:
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
        .with_columns(
            # Positional slot naming, matching over_odds/under_odds: `over_*` is
            # slot A. Null for named-outcome markets, which have no player.
            (
                pl.col(f"side_player_id_{over_side}")
                if carries_player
                else pl.lit(None, dtype=pl.String)
            ).alias("player_id_a"),
            (
                pl.col(f"side_player_id_{under_side}")
                if carries_player
                else pl.lit(None, dtype=pl.String)
            ).alias("player_id_b"),
        )
        .select(
            "match_uid", "points", "over_odds", "under_odds", "p_over", "overround",
            "imbalance", "live", "live_over", "live_under",
            "quote_age_s", "last_change_s",
            "player_id_a", "player_id_b",
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
    over_side: str | None = None,
    under_side: str | None = None,
) -> pl.DataFrame:
    """What every book had on the board, per match, at that match's time T.

    `times` is a frame of (match_uid, t) -- one T per match, so an absolute instant and
    a per-match anchor (T-1h, the close) use the same call. One row per match is
    enforced: a duplicated `match_uid` would fan out the join and then collapse both
    anchors into one board, silently losing the earlier one.

    Returns one row per (match_uid, book, points), `BOARD_SCHEMA`-shaped even
    when empty.

    **Sides default to the market's own vocabulary, not to totals.** They used to
    default to `Over`/`Under` for every market, which made
    `board_at(times, "game_spread", over_side="1", under_side="2")` a working call
    that returned a board keyed to the feed's positional participant -- the
    wrong-player board this module exists to prevent, produced without an error.
    Passing them explicitly is an override for NAMED-OUTCOME markets only. A
    participant-sided market REFUSES anything but its feed pair: those labels are
    consumed as the feed's vocabulary when re-siding, so an oriented pair would
    match nothing and split every rung rather than erroring. The oriented pair is
    this function's output, never its input.
    """
    missing = {"match_uid", "t"} - set(times.columns)
    if missing:
        raise ValueError(f"times is missing {sorted(missing)}")
    if times["match_uid"].is_duplicated().any():
        raise ValueError(
            "times has duplicate match_uid; board_at takes one T per match"
        )

    feed_over, feed_under = feed_sides(market)
    over_side = over_side if over_side is not None else feed_over
    under_side = under_side if under_side is not None else feed_under
    orient = market in _participant_markets()
    if orient and (over_side, under_side) != (feed_over, feed_under):
        # These are consumed as the FEED's vocabulary by
        # `_orient_participant_sides`, which reads them off the stage rows. An
        # oriented pair matches nothing there, so every row takes the `.otherwise`
        # branch: the two sides of a rung get different `points` and each rung
        # splits into two one-sided ones. Measured on 60 close anchors that turns
        # 120 rows / 120 two-sided into 231 rows / 9 two-sided, with no error and
        # no log. Refuse instead -- the oriented pair is this function's OUTPUT,
        # never its input.
        raise ValueError(
            f"{market} is participant-sided: over_side/under_side are the FEED's "
            f"labels {(feed_over, feed_under)}, not the oriented pair. Got "
            f"{(over_side, under_side)}. The board emits {ORIENTED_SIDES}; it "
            "does not accept them."
        )

    frames = []
    for book in books or available_books(market):
        per_side = _latest_per_side(book, market, times, prematch_only=prematch_only)
        if per_side.is_empty():
            continue
        if orient:
            per_side = _orient_participant_sides(per_side, over_side, under_side)
            pivot_over, pivot_under = ORIENTED_SIDES
        else:
            pivot_over, pivot_under = over_side, under_side
        frames.append(
            _pivot_two_sided(per_side, pivot_over, pivot_under).with_columns(
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
