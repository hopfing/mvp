"""Price a board against a projection's total-games pmf, and settle it.

This is the join between two things that are deliberately kept apart: `mvp.oddspapi`
knows what books offered and nothing about any model; a projection knows a
distribution and nothing about prices. Neither imports the other. This module is the
only place they meet, which is what lets a board be built and inspected before any
projector exists, and lets any projector be priced without touching the odds layer.

**Any pmf, not a particular one.** `price` takes the pmf frame as an argument. There is
no default model and no fingerprint baked in -- pricing N candidate configs against the
same board is the point of the exercise, not an extension of it.

**Pushes are real.** Books quote whole-number totals (pinnacle sits on 23.0 routinely),
and a match landing exactly on the line returns the stake. Treating every line as a
half-integer would silently score pushes as losses on the under and wins on the over.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from mvp.oddspapi import paths

logger = logging.getLogger(__name__)

PMF_COL = "total_games_pmf"
SPREAD_PMF_COL = "spread_pmf"

# Per-market pmf schema. `offset` is what separates the two index conventions:
# totals is 0-based (element i is P(total == i)), spread is SIGNED and stored with
# an offset (element i is P(margin == i - offset)). Reading a spread pmf 0-based
# does not raise -- it lands every lookup `offset` places out, and the resulting
# ledger is plausible and wrong.
_MARKET_PMF: dict[str, dict[str, str | bool | None]] = {
    "total_games": {
        "pmf": PMF_COL,
        "outcome": "actual_total",
        "expected": "expected_total_games",
        "offset": None,
        # Named-outcome: `Over` is not a player, so there is no orientation to
        # check and no `a_is_uid_min` to demand.
        "oriented": False,
    },
    "game_spread": {
        "pmf": SPREAD_PMF_COL,
        "outcome": "actual_spread",
        "expected": "expected_spread",
        "offset": "spread_offset",
        # Participant-sided: the board's `a` and the pmf's `a` are defined
        # independently and MUST be checked against each other.
        "oriented": True,
    },
}


def market_pmf_spec(market: str) -> dict[str, str | bool | None]:
    """Column names for one market's pmf frame.

    Raises on an unknown market rather than defaulting to totals: a market that
    reaches pricing without a registered schema would otherwise be priced against
    the wrong distribution under the right-looking column names.
    """
    try:
        return _MARKET_PMF[market]
    except KeyError:
        raise ValueError(
            f"no pmf schema for market {market!r}; known: {sorted(_MARKET_PMF)}"
        ) from None

# Bumped when the ledger's contract changes. Carried as a column so a reader can
# tell contracts apart without depending on `projection_evaluations` happening to
# hold exactly one directory.
#
# 2: `p_over` removed, `book_p` and `side_pos` added. `p_over` was rung-level and
#    therefore the wrong side's probability on half of every ledger's rows;
#    `book_p` is its per-side form. `side_pos` is the positional key the two-way
#    complements branch on, which the `side` label cannot serve once a market
#    labels its sides by something other than position.
LEDGER_SCHEMA_VERSION = 2

# Anchors written into every ledger. `open` and `close` answer the entry-timing
# question; `formed(2)` is the first moment a second entry book agrees there is a
# market. Fixed offsets serve the decay curve and are deliberately not here — the
# ledger is long, so adding one later is a new `anchor` value, not a new column.
DEFAULT_ANCHORS: tuple[str, ...] = ("open", "formed2", "close")


class MarketNotCarried(RuntimeError):
    """No entry book carries this market's stage file.

    A fact about the stage tree, not about the config being evaluated, so a
    caller pricing several markets can skip this one and keep the others.

    A dedicated class rather than catching `RuntimeError`: `NotImplementedError`
    and `RecursionError` are both subclasses, so a bare catch would swallow
    either from anywhere under `build_ledger` and record it as "this market is
    not carried" -- retrying forever with the same result. Subclassing
    `RuntimeError` keeps existing `except RuntimeError` callers working.
    """



def cumulative(
    pmf: pl.DataFrame, *, market: str = "total_games"
) -> pl.DataFrame:
    """(match_uid, games, p, p_at_or_below) -- the pmf as a long, cumulative table.

    `games` is the market's outcome value: a total for `total_games`, a SIGNED
    margin for `game_spread`. Exploding rather than slicing keeps the arithmetic
    legible and lets the line join be an ordinary equality rather than a per-row
    offset.

    **The index is shifted per market, not just renamed.** Totals is 0-based
    (element i is P(total == i), verified against `expected_total_games`). Spread
    stores a signed support with an offset, so element i is P(margin == i - offset)
    and the offset must be subtracted here. Renaming the column without shifting
    does not raise: board `points` for spread run roughly -10..+10 while an
    unshifted index runs 0..130, so `price`'s inner join would match spuriously
    across the 0..10 overlap with the wrong probability mass attached, and
    everything outside that band would vanish down the already-logged no-support
    path.

    The offset is read from the frame rather than assumed, and asserted constant:
    it is a storage constant of the chain, so a frame carrying two of them means
    two incompatible pmfs have been concatenated.
    """
    spec = market_pmf_spec(market)
    col = spec["pmf"]
    missing = {"match_uid", col} - set(pmf.columns)
    if missing:
        raise ValueError(f"pmf frame is missing {sorted(missing)}")

    offset = 0
    offset_col = spec["offset"]
    if offset_col is not None:
        if offset_col not in pmf.columns:
            raise ValueError(
                f"{market} pmf frame is missing {offset_col!r}; the signed index "
                "cannot be recovered without it"
            )
        distinct = pmf[offset_col].unique().to_list()
        if any(v is None for v in distinct):
            raise ValueError(
                f"{offset_col} is null; the signed index cannot be recovered"
            )
        if len(distinct) != 1:
            raise ValueError(
                f"{offset_col} is not constant across the frame ({distinct}); "
                "two pmfs with different supports have been combined"
            )
        offset = int(distinct[0])

    return (
        pmf.select("match_uid", col)
        .with_row_index("_r")
        .explode(col)
        .with_columns(
            (pl.int_range(pl.len()).over("_r") - offset).alias("games")
        )
        .rename({col: "p"})
        .with_columns(pl.col("p").cum_sum().over("_r").alias("p_at_or_below"))
        .drop("_r")
    )


def price(
    board: pl.DataFrame, pmf: pl.DataFrame, *, market: str = "total_games"
) -> pl.DataFrame:
    """Attach model probabilities and edges to a board.

    `model_p_over` is P(total > line) and `p_push` is P(total == line), zero for the
    half-integer lines that dominate and non-zero for whole-number ones. `model_p_under`
    is the remainder.

    The board's `p_over` -- the BOOK's de-vigged probability -- is left untouched. These
    are two different quantities and naming them both `p_over` silently destroys one:
    `edge` is the model against the PRICE, `edge_novig` is the model against the book's
    OPINION, and the second has no input if the book's probability is overwritten.

    Edge is RAW (`p - 1/odds`), matching the live pipeline and the classification
    backtest. De-vigged edge is a different question and belongs at read time, not
    baked in here.

    Rows whose match has no pmf are dropped -- a bet cannot be priced by a model that
    did not project the match, and carrying a null edge invites it into a mean.
    """
    if board.is_empty():
        return board
    cum = cumulative(pmf, market=market)
    # floor(line) is the largest game count that is NOT over the line
    at_or_below = cum.select(
        "match_uid",
        pl.col("games").cast(pl.Float64).alias("_floor"),
        pl.col("p_at_or_below"),
    )
    at_line = cum.select(
        "match_uid",
        pl.col("games").cast(pl.Float64).alias("_exact"),
        pl.col("p").alias("p_push"),
    )
    priced = (
        board.with_columns(pl.col("points").floor().alias("_floor"))
        .join(at_or_below, on=["match_uid", "_floor"], how="inner")
        .join(
            at_line.rename({"_exact": "points"}),
            on=["match_uid", "points"],
            how="left",
        )
        .with_columns(pl.col("p_push").fill_null(0.0))
        .with_columns(
            (1.0 - pl.col("p_at_or_below")).alias("model_p_over"),
        )
        .with_columns(
            (pl.col("p_at_or_below") - pl.col("p_push")).alias("model_p_under"),
        )
        .with_columns(
            # RAW edge -- what the bet actually returns at this price.
            (pl.col("model_p_over") - 1.0 / pl.col("over_odds")).alias("edge_over"),
            (pl.col("model_p_under") - 1.0 / pl.col("under_odds")).alias("edge_under"),
            # DISAGREEMENT with the book's own opinion, vig removed from both sides.
            # `p_over` is the BOOK's de-vigged probability, carried in from the board --
            # it must not be overwritten by the model's, which is what makes these two
            # different questions rather than the same one twice.
            (pl.col("model_p_over") - pl.col("p_over")).alias("edge_novig_over"),
            (pl.col("model_p_under") - (1.0 - pl.col("p_over")))
            .alias("edge_novig_under"),
        )
        .drop("_floor", "p_at_or_below")
    )
    _assert_orientation_agrees(priced, pmf, market=market)
    return priced


def _assert_orientation_agrees(
    priced: pl.DataFrame, pmf: pl.DataFrame, *, market: str
) -> None:
    """The pmf's `a` and the board's `a` must be the same player.

    They are defined independently and can disagree. The board derives its
    ordinal from `match_uid`'s fifth component, which is `min(player_id, opp_id)`
    by construction. The pmf's `a` is whichever perspective row survived
    filtering -- `_collapse_to_match_rows` keeps the lowest SURVIVING
    `player_id`, so a match arriving with one perspective row keeps that row
    whatever its id. Measured on the 2026 test set, 11.5% of matches are in that
    state.

    They agree on every currently priceable match, which is exactly why this
    needs asserting rather than assuming: the day a book quotes one of those
    matches, every rung on it prices the wrong player's margin against the line
    and settles with the sign flipped, silently. §7's `corr < 0` criterion cannot
    see it either, because relabelling and re-signing together leave a
    correlation unchanged.

    Scoped to the rows that survived the join, because that is the only point
    where both definitions are in scope: asserting earlier -- in the projection,
    where `player_id` is in hand -- cannot be limited to priced matches, and
    would fire on the 11.5% immediately.
    """
    if priced.is_empty():
        return
    if "a_is_uid_min" not in pmf.columns:
        # Absent is fine for a market with no orientation to check. For an
        # oriented one it is NOT: the column is mandatory, and skipping the check
        # because the operand is missing is the same fail-open the check exists to
        # prevent -- a pmf frame written by a partially-updated version, or hand
        # built, would ship mirror mode silently. Keyed off the market spec rather
        # than a literal, so a third participant market cannot silently reopen it.
        if market_pmf_spec(market)["oriented"]:
            raise ValueError(
                f"{market} pmf frame is missing `a_is_uid_min`; the board's "
                "and the projection's `a` cannot be checked against each other, "
                "and a mismatch is invisible downstream"
            )
        return
    kept = priced["match_uid"].unique()
    # Null reads as DISAGREEMENT, not agreement: an unknown orientation is
    # exactly the state this refuses to price through.
    bad = pmf.filter(
        pl.col("match_uid").is_in(kept.implode())
        & ~pl.col("a_is_uid_min").fill_null(False)
    )
    if bad.height:
        raise ValueError(
            f"orientation mismatch on {bad.height} priced match(es): the pmf's "
            f"`a` is not match_uid's lower player_id, so the board and the "
            f"projection disagree about which player each price belongs to. "
            f"First: {bad['match_uid'][0]}. Fix belongs in "
            f"_collapse_to_match_rows, not here."
        )


def settle(
    priced: pl.DataFrame, outcomes: pl.DataFrame, *, market: str = "total_games"
) -> pl.DataFrame:
    """Settle both sides of every rung against the realised outcome.

    `outcomes` is (match_uid, <the market's outcome column>) -- `actual_total` for
    totals, `actual_spread` for spread. Matches absent from it are dropped rather
    than scored: a retirement, walkover or default produces fewer games than the
    match would have, so scoring one against a total records an "under" no book
    would have settled that way. It produces a MARGIN no book would have settled
    either, and margin is what spreads price, so the same exclusion matters at
    least as much here. It belongs upstream, in whatever built `outcomes`.

    The comparison is identical in shape for both markets because both are already
    in the a-frame: `actual_spread` is `games_a - games_b` and `points` is a
    threshold on that same quantity (`board._orient_participant_sides`). So
    "over_won" reads as "the A side covered" and needs no per-market branch.

    A push returns the stake -- pnl 0, not a loss -- and is counted separately so it
    cannot be mistaken for a break-even bet in a hit rate. A whole-number spread
    pushes exactly as a whole-number total does.
    """
    if priced.is_empty():
        return priced
    outcome_col = market_pmf_spec(market)["outcome"]
    if outcome_col not in outcomes.columns:
        raise ValueError(f"outcomes needs (match_uid, {outcome_col})")
    out = priced.join(
        outcomes.select("match_uid", outcome_col), on="match_uid", how="inner"
    )
    # A null outcome makes both `push` and `over_won` null, and a null predicate
    # routes to `.otherwise`, landing the row as a loss on BOTH sides. Drop rather
    # than score: an unknown result is not a losing bet.
    unknown = out.filter(pl.col(outcome_col).is_null()).height
    if unknown:
        logger.warning("settle: dropping %d rows with a null %s", unknown, outcome_col)
        out = out.drop_nulls(outcome_col)
    over_won = pl.col(outcome_col) > pl.col("points")
    push = pl.col(outcome_col) == pl.col("points")
    return out.with_columns(
        push.alias("is_push"),
        pl.when(push).then(None).otherwise(over_won).alias("over_won"),
    ).with_columns(
        pl.when(pl.col("is_push"))
        .then(0.0)
        .when(pl.col("over_won"))
        .then(pl.col("over_odds") - 1.0)
        .otherwise(-1.0)
        .alias("pnl_over"),
        pl.when(pl.col("is_push"))
        .then(0.0)
        .when(pl.col("over_won").not_())
        .then(pl.col("under_odds") - 1.0)
        .otherwise(-1.0)
        .alias("pnl_under"),
    )


def bets(settled: pl.DataFrame, *, min_edge: float = 0.0) -> pl.DataFrame:
    """Collapse two-sided rows to the side the model likes, above an edge floor.

    One row per (match, book, rung) that clears the floor on either side. `min_edge` is
    a parameter and not a constant: the edge threshold is a read-time selection to be
    swept, never a rule compiled into the settlement.

    **Reference books are excluded here, not upstream.** `board_at` deliberately keeps
    pinnacle -- its price is the CLV reference and the market's sharpest opinion, and
    the board is where you want it. But a bet set is what you could have taken, and
    pinnacle is not reachable. Without this filter it was 39% of the bets at the open
    anchor, every one of them unplaceable, and the ROI computed off them meaningless.

    **One-sided rungs are picked by availability, not by comparison.** A rung quoted on
    one side only has a null edge on the other, and `a >= null` is null, which polars
    routes to `otherwise` — so a naive comparison emits an over-only rung as an UNDER
    bet, with null odds and (because `max_horizontal` skips nulls) the OVER's edge on
    the row. It then clears the edge floor and lands preferentially in the high-edge
    tail. One-sided quotes run 4-17% of rows by book, so this is a real population.
    """
    if settled.is_empty():
        return settled
    if "book" in settled.columns:
        settled = settled.filter(~pl.col("book").is_in(list(paths.REFERENCE_BOOKS)))
        if settled.is_empty():
            return settled
    # A rung whose most recent tick says the book took it down is not on offer, no
    # matter that a price is carried forward for it. Measured at the close anchor,
    # 36.8% of two-sided rows are in that state; without this they are scored as bets
    # at prices nobody could take.
    if "live_over" in settled.columns and "live_under" in settled.columns:
        # Per side: a rung the book pulled one leg of is still bettable on the other.
        # Filtering on the pair-AND `live` would drop it entirely.
        settled = settled.filter(pl.col("live_over") | pl.col("live_under"))
    elif "live" in settled.columns:
        settled = settled.filter(pl.col("live"))
    # A side is takeable only if it is quoted AND still live. Odds are carried forward
    # from the last tick, so a pulled leg keeps a price -- without the liveness term
    # the model could be handed the better edge on a side no longer on the board.
    live_o = pl.col("live_over") if "live_over" in settled.columns else pl.lit(True)
    live_u = pl.col("live_under") if "live_under" in settled.columns else pl.lit(True)
    has_over = pl.col("edge_over").is_not_null() & live_o
    has_under = pl.col("edge_under").is_not_null() & live_u
    take_over = (
        pl.when(has_over & has_under)
        .then(pl.col("edge_over") >= pl.col("edge_under"))
        .when(has_over)
        .then(True)
        .otherwise(False)
    )
    return (
        settled.with_columns(
            # `side_pos`, not the literals `over`/`under`. This function has no
            # `market` parameter and `settle()`'s output has no `market` column
            # (`build_ledger` adds it after the reshape), so there is nothing to
            # branch on -- and nothing to raise on either: `over_odds`,
            # `model_p_over` and `edge_novig_over` all exist on a spread frame
            # because the board is normalised positionally. It would run happily
            # and mislabel every row. Emitting the positional key removes the
            # hazard rather than guarding it.
            pl.when(take_over).then(pl.lit("a")).otherwise(pl.lit("b"))
            .alias("side_pos"),
            pl.max_horizontal("edge_over", "edge_under").alias("edge"),
            pl.when(take_over).then(pl.col("over_odds"))
            .otherwise(pl.col("under_odds")).alias("odds"),
            pl.when(take_over).then(pl.col("model_p_over"))
            .otherwise(pl.col("model_p_under")).alias("model_p"),
            pl.when(take_over).then(pl.col("edge_novig_over"))
            .otherwise(pl.col("edge_novig_under")).alias("edge_novig"),
            pl.when(take_over).then(pl.col("pnl_over"))
            .otherwise(pl.col("pnl_under")).alias("pnl"),
        )
        # A rung with neither side quoted is not a bet. `odds` null here means the
        # chosen side was never on the board, which no edge floor would catch.
        .filter(
            pl.col("odds").is_not_null()
            & (has_over | has_under)
            & (pl.col("edge") >= min_edge)
        )
    )


# ---------------------------------------------------------------------------
# The ledger: settle() output reshaped to one row per side
# ---------------------------------------------------------------------------

# Columns that describe the RUNG and are therefore shared by both of its sides.
# Rung-level columns copied onto BOTH side rows. Everything here is a property of
# the rung or the pair -- the overround of the two prices together, how balanced
# they are, when it last moved -- so duplicating it is correct.
#
# `p_over` is deliberately NOT here. It describes ONE side, so carrying it at rung
# level put the over's probability on the under's row too, where it had to be read
# as "one minus this". `unpivot_sides` emits it per side as `book_p` instead.
_RUNG_COLS_BASE = (
    "match_uid", "book", "points",
    "overround", "imbalance", "separation",
    "live", "quote_age_s", "last_change_s", "n_two_sided", "is_main_line",
    "line_offset",
    "p_push", "is_push",
)


def _rung_cols(market: str) -> tuple[str, ...]:
    """Rung columns for one market: the shared set plus that market's outcome.

    Per market rather than a union of both outcome columns, because each market
    writes its own ledger file (`artifacts.PMF_PARQUET_BY_MARKET`) and a reader of
    one should not have to know the other's schema.
    """
    return _RUNG_COLS_BASE + (market_pmf_spec(market)["outcome"],)


# POSITIONAL side key -> its (odds, model_p, edge, edge_novig, pnl, live) columns.
#
# Keyed `a`/`b`, not `over`/`under`, and the same map for every market. The board
# is normalised positionally -- `_pivot_two_sided` renames whichever pair it was
# given to `over_*`/`under_*` -- so slot A is the first side of whatever
# vocabulary the market uses, and the column names never vary by market.
#
# This is what the two-way complements branch on. They must NOT branch on the
# ledger's `side` label: for spread that label is `fav`/`dog`, which is a
# different question from "is this the a side" and disagrees with it on about half
# of all rungs.
_SIDE_COLS = {
    "a": ("over_odds", "model_p_over", "edge_over", "edge_novig_over",
          "pnl_over", "live_over"),
    "b": ("under_odds", "model_p_under", "edge_under", "edge_novig_under",
          "pnl_under", "live_under"),
}

# The ledger's human-facing `side` label, keyed by POSITION rather than paired by
# tuple order, so it cannot drift with `_SIDE_COLS`' dict ordering.
#
# `game_spread` is deliberately absent: its label is `fav`/`dog` taken from the
# sign of the (match, book, anchor) main line, which needs the oriented board that
# the orientation work supplies. Until then asking for it raises rather than
# falling back to `a`/`b` -- shipping the positional label would put "alphabetical
# accident" in a bettor-facing column with nothing to signal it.
_SIDE_LABELS: dict[str, dict[str, str]] = {
    "total_games": {"a": "over", "b": "under"},
}


def _favourite_label(pos: str) -> pl.Expr:
    """`fav` / `dog` / `pk` for one positional slot, from the main line's sign.

    A property of the (match, book, anchor) BLOCK, not of the rung. Assigning it
    per rung from the shorter price would flip along a ladder -- the a side is
    long-priced at a high line and short-priced at a low one -- and would label
    the same player both ways in one match. The favourite is whoever the market
    has stronger, which is one answer per block, inherited by every rung on it.

    **Sign is ledger-frame.** `points` here is a threshold on `games_a - games_b`,
    so a POSITIVE main line means a must win by more than X: a is the favourite.
    That is the opposite of the feed's display convention ("A -4.5"), which is
    where this gets read backwards.

    The grain is (match, book) inside `build_ledger`'s per-anchor loop, which is
    the full (match, book, anchor) key -- `anchor` is stamped after this runs. Two
    books may therefore disagree about who is favoured, and a match may flip
    between anchors. Both are real and worth seeing; `side` is NOT a match-level
    constant.

    Null where the block has no main line at all -- `_flag_main_line` needs a live
    two-sided rung, and a block with none has no sign to read. Harmless because
    both readers of `side` filter to main-line rows first, so such a block
    contributes nothing to either.
    """
    main = (
        pl.when(pl.col("is_main_line").fill_null(False))
        .then(pl.col("points"))
        .max()
        .over(["match_uid", "book"])
    )
    a_is_fav = main > 0
    fav, dog = ("fav", "dog") if pos == "a" else ("dog", "fav")
    return (
        pl.when(main.is_null()).then(pl.lit(None, dtype=pl.String))
        .when(main == 0).then(pl.lit("pk"))
        .when(a_is_fav).then(pl.lit(fav))
        .otherwise(pl.lit(dog))
    )


def _side_label_expr(market: str, pos: str) -> pl.Expr:
    """The `side` value for one positional slot, as an expression.

    An expression rather than a literal because spread's label is derived from
    the frame -- see `_favourite_label`. Raises for a market that has named
    neither, rather than falling back to `a`/`b`: shipping the positional key in
    a bettor-facing column puts alphabetical accident where a bet type belongs.
    """
    if market == "game_spread":
        return _favourite_label(pos)
    try:
        return pl.lit(_SIDE_LABELS[market][pos])
    except KeyError:
        raise ValueError(
            f"no `side` label for market {market!r} slot {pos!r}; known: "
            f"{sorted(_SIDE_LABELS) + ['game_spread']}"
        ) from None


def unpivot_sides(
    settled: pl.DataFrame, *, market: str = "total_games"
) -> pl.DataFrame:
    """One row per (rung, side) from `settle()`'s two-sided rows.

    `settle` returns rung-level rows carrying `over_*` / `under_*` pairs and no
    `side` column, because balance and de-vig are properties of the PAIR. The
    ledger's grain is per side, so this is a genuine reshape rather than a
    rename — and it is the only place the pair is broken apart.

    Deliberately NOT `bets()`. That collapses a rung to the single side the model
    prefers and applies an edge floor, which writes a bet set; the ledger is the
    OFFER set, so both sides of every rung are emitted including negative-edge
    ones, and every selection — side, edge floor, liveness, book role — is left
    to read time.

    `won` is per side and null on a push, so a push cannot be counted as a loss
    on either side. `live` is carried per side too: odds are forward-filled from
    the last tick, so a book that pulled one leg still has a price on it, and
    only the per-side flag distinguishes a takeable quote from a stale one.
    """
    if settled.is_empty():
        return settled

    shared = [c for c in _rung_cols(market) if c in settled.columns]
    frames: list[pl.DataFrame] = []
    for pos, cols in _SIDE_COLS.items():
        odds, model_p, edge, edge_novig, pnl, live = cols
        needed = {odds, model_p, edge, pnl}
        if not needed <= set(settled.columns):
            raise ValueError(
                f"settled frame is missing {sorted(needed - set(settled.columns))} "
                f"— unpivot_sides expects settle() output"
            )
        won = pl.col("over_won") if pos == "a" else pl.col("over_won").not_()
        exprs = [
            pl.lit(pos).alias("side_pos"),
            _side_label_expr(market, pos).alias("side"),
            pl.col(odds).alias("odds"),
            pl.col(model_p).alias("model_p"),
            pl.col(edge).alias("edge"),
            pl.col(pnl).alias("pnl"),
            won.alias("won"),
        ]
        # The book's de-vigged probability for THIS row's side, mirroring
        # `model_p`. `p_over` is the a-side value, so the b row takes its
        # complement -- carried at rung level it was the wrong side's number on
        # half of every ledger's rows, and nothing read it.
        #
        # NOTE the two do not sum alike: `book_p` is the two-way de-vig and
        # carries no push mass, so `book_p_a + book_p_b == 1` while
        # `model_p_a + model_p_b == 1 - p_push`. On a whole-number line they
        # therefore differ by `p_push` by construction, which is the same
        # identity as `edge_novig_a + edge_novig_b == -p_push`. Comparing the two
        # sums directly is a category error, not a bug.
        if "p_over" in settled.columns:
            book_p = (
                pl.col("p_over") if pos == "a" else 1.0 - pl.col("p_over")
            )
            exprs.append(book_p.alias("book_p"))
        player_col = f"player_id_{pos}"
        if player_col in settled.columns:
            exprs.append(pl.col(player_col).alias("side_player_id"))
        if edge_novig in settled.columns:
            exprs.append(pl.col(edge_novig).alias("edge_novig"))
        if live in settled.columns:
            exprs.append(pl.col(live).alias("live_side"))
        frames.append(settled.select(*shared, *exprs))

    return pl.concat(frames, how="vertical_relaxed")


def add_mean_covers(
    ledger: pl.DataFrame, pmf: pl.DataFrame, *, market: str = "total_games"
) -> pl.DataFrame:
    """Does the chain's EXPECTED outcome land on the side being bet?

    A per-side model-agreement gate, not a property of the rung: the pmf tail can
    put the model over the price while its centre disagrees, and the same rung
    therefore gets `mean_covers` true on one side and false on the other. This is
    the gate that separated bet-selection from chain error — H71 attributed ~80%
    of the favourite bleed to selection using it.

    The expected column is per market; the BRANCH is not. It tests `side_pos`,
    because the quantity compared is a-framed — `expected_spread` is E[margin_a],
    so the a side covers when it exceeds the line and the b side when it falls
    short. Branching on the ledger's `side` label instead would be correct for
    totals and wrong for spread on roughly half of all rungs, silently, since
    `fav`/`dog` is a different question from "is this the a side".
    """
    if ledger.is_empty():
        return ledger
    expected = market_pmf_spec(market)["expected"]
    if expected not in pmf.columns:
        raise ValueError(f"pmf frame needs {expected} for mean_covers")
    if "side_pos" not in ledger.columns:
        raise ValueError(
            "ledger needs side_pos for mean_covers; branching on `side` is "
            "correct only where the label happens to be positional"
        )
    out = ledger.join(
        pmf.select("match_uid", expected).unique(subset=["match_uid"]),
        on="match_uid",
        how="left",
    )
    return out.with_columns(
        pl.when(pl.col("side_pos") == "a")
        .then(pl.col(expected) > pl.col("points"))
        .otherwise(pl.col(expected) < pl.col("points"))
        .alias("mean_covers")
    )


def add_line_offset(board: pl.DataFrame) -> pl.DataFrame:
    """Distance from a rung to the main line OF THE BOOK QUOTING IT.

    Measured against a line the book actually named, not a cross-book median —
    a book's line is part of its opinion, so betrivers at 22.5 and draftkings at
    23.5 are two different bets rather than two prices on one. The pooled median
    the retired code used is what produced the 35.5-games artifact.

    Null where the book has no main line at that instant, which happens when
    every live rung is one-sided: a one-sided quote has no de-vig and therefore
    no balance to compare, so it cannot be the main line and nothing anchors the
    distance.

    Reading it requires conditioning on book. It is expected to be near-zero for
    books that rarely have more than one two-sided rung live at once (bet365 and
    fanduel carry ≥2 in 6.7% and 1.8% of bursts), so the non-zero population is
    effectively the other books — a slice on offset is also a slice on book.
    """
    if board.is_empty() or "is_main_line" not in board.columns:
        return board
    main_points = (
        pl.when(pl.col("is_main_line"))
        .then(pl.col("points"))
        .otherwise(None)
        .max()
        .over(["match_uid", "book"])
    )
    return board.with_columns(
        (pl.col("points") - main_points).alias("line_offset")
    )


def add_clv(
    ledger: pl.DataFrame, *, market: str = "total_games",
) -> pl.DataFrame:
    """Closing-line value against the reference book's de-vigged close.

        clv = p_reference_close(side) - 1 / odds_taken

    The sharp fair probability at the close, minus the raw implied probability
    of the price actually taken. Negative on average by the entry book's vig,
    which is why the number is only meaningful COMPARED — between configs,
    anchors or selections — never read as a level.

    Why it belongs in the ledger rather than a script: it is the metric that
    distinguishes a bias the market has already priced from one it has not.
    Residuals cannot tell those apart — correcting a priced-in bias improves
    residuals and produces no edge — so the whole question rides on this
    column, and it cannot rest on an unversioned scratch file reading a
    retired odds path.

    The reference is taken at the CLOSE regardless of which anchor the entry
    row sits at: closing line value means value against the close, so an `open`
    row and a `close` row are both scored against the same benchmark.

    Joined on (match_uid, points) — the SAME rung. A price on 22.5 compared
    against the reference's fair for 23.5 is not closing-line value, it is two
    different bets.
    """
    from mvp.oddspapi import anchors, board

    if ledger.is_empty():
        return ledger
    ref_books = [b for b in board.available_books(market)
                 if b in paths.REFERENCE_BOOKS]
    if not ref_books:
        logger.warning("no reference book carries %s; CLV unavailable", market)
        return ledger.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("p_ref_close"),
            pl.lit(None, dtype=pl.Float64).alias("clv"),
        )

    uids = ledger["match_uid"].unique()
    times = anchors.close(market, match_uids=uids)
    ref_board = board.board_at(times, market, books=ref_books)
    if ref_board.is_empty():
        logger.warning("reference board at close is empty; CLV unavailable")
        return ledger.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("p_ref_close"),
            pl.lit(None, dtype=pl.Float64).alias("clv"),
        )

    ref = (
        ref_board.select(
            "match_uid", "points", pl.col("p_over").alias("p_ref_close_over")
        )
        .drop_nulls("p_ref_close_over")
        .unique(subset=["match_uid", "points"], keep="first")
    )
    out = ledger.join(ref, on=["match_uid", "points"], how="left")
    # Branches on `side_pos`, not `side`: `p_ref_close_over` is the reference
    # board's A-SIDE probability, so the complement is positional. For spread the
    # `side` label is `fav`/`dog`, which disagrees with the a/b ordinal on about
    # half of all rungs -- keying here would invert CLV on both rows of those.
    p_ref = (
        pl.when(pl.col("side_pos") == "a")
        .then(pl.col("p_ref_close_over"))
        .otherwise(1.0 - pl.col("p_ref_close_over"))
    )
    covered = out["p_ref_close_over"].is_not_null().sum()
    logger.info(
        "CLV reference matched %d of %d ledger rows (%.1f%%)",
        covered, out.height, 100.0 * covered / out.height,
    )
    return out.with_columns(
        p_ref.alias("p_ref_close"),
    ).with_columns(
        (pl.col("p_ref_close") - 1.0 / pl.col("odds")).alias("clv"),
    ).drop("p_ref_close_over")


def _anchor_times(name: str, market: str, match_uids: pl.Series) -> pl.DataFrame:
    """`(match_uid, t)` for one named anchor, scoped to the projected matches.

    `open` and `formed` decide a rung is two-sided by matching the `side` column
    against a pair that defaults to `Over`/`Under` (`anchors.formed`), so a
    participant-sided market matched nothing and returned ZERO times -- silently,
    via `build_ledger`'s "produced no times; skipping". With
    `rank.py:HEADLINE_ANCHOR = "open"` that emptied the whole report for
    `game_spread` while `close` kept working, because `close` is start-time based
    and takes no side pair at all. Same for `offset`.

    `board.feed_sides` is the FEED's vocabulary deliberately: `anchors._load`
    reads `side` off the stage parquet, so an oriented pair would match nothing
    and leave the count at zero -- a fix that changes nothing.
    """
    from mvp.oddspapi import anchors, board

    over_side, under_side = board.feed_sides(market)
    if name == "open":
        return anchors.open_(
            market, match_uids=match_uids,
            over_side=over_side, under_side=under_side,
        )
    if name.startswith("formed"):
        n = int(name.removeprefix("formed") or 2)
        return anchors.formed(
            market, n, match_uids=match_uids,
            over_side=over_side, under_side=under_side,
        )
    if name == "close":
        return anchors.close(market, match_uids=match_uids)
    if name.startswith("offset"):
        return anchors.offset(float(name.removeprefix("offset")), match_uids=match_uids)
    raise ValueError(f"unknown anchor: {name!r}")


def build_ledger(
    pmf: pl.DataFrame,
    *,
    market: str = "total_games",
    anchor_names: tuple[str, ...] = DEFAULT_ANCHORS,
) -> pl.DataFrame:
    """The offer set: every rung every entry book had up, at every anchor.

    One row per (match_uid, book, market, points, side, anchor), negative-edge
    rows included. Selection — main line, edge floor, side, policy — is read
    time; see `rank.py`.

    **Books are passed explicitly.** `board_at` defaults to `available_books`
    while `anchors` defaults to `entry_books`, and `BOARD_SCHEMA` carries no role
    column — so a default-driven board mixes pinnacle rows in indistinguishably.
    Pinnacle is 39–53% of board rows per anchor and its thin vig makes it the
    most favourable half, so a headline computed over it would be roughly half
    unreachable prices. Entry books only, and a `role` column so the choice is
    visible in the artifact rather than remembered.
    """
    from mvp.oddspapi import board

    books = board.entry_books(market)
    if not books:
        raise MarketNotCarried(f"no entry books carry {market}")
    uids = pmf["match_uid"].unique()
    outcomes = pmf.select("match_uid", market_pmf_spec(market)["outcome"])

    frames: list[pl.DataFrame] = []
    for name in anchor_names:
        times = _anchor_times(name, market, uids)
        if times.is_empty():
            logger.warning("anchor %s produced no times; skipping", name)
            continue
        board_df = board.board_at(times, market, books=books)
        if board_df.is_empty():
            logger.warning("anchor %s produced an empty board; skipping", name)
            continue
        board_df = add_line_offset(board_df)
        priced = price(board_df, pmf, market=market)
        # `price` joins the pmf how="inner" on the floored line, so a rung whose
        # line sits outside the pmf's support disappears with nothing raised.
        # Inert while the support (0..65 games) covers every offered total, but
        # unguarded — and a silently shorter ledger reads as a thinner market
        # rather than a bug.
        dropped = board_df.height - priced.height
        if dropped:
            logger.warning(
                "anchor %s: %d of %d board rows had no pmf support and were "
                "dropped by pricing", name, dropped, board_df.height,
            )
        settled = settle(priced, outcomes, market=market)
        if settled.is_empty():
            logger.warning("anchor %s settled to nothing; skipping", name)
            continue
        rows = add_mean_covers(
            unpivot_sides(settled, market=market), pmf, market=market
        )
        frames.append(rows.with_columns(pl.lit(name).alias("anchor")))
        logger.info("anchor %s: %d ledger rows", name, rows.height)

    if not frames:
        return pl.DataFrame()

    ledger = pl.concat(frames, how="vertical_relaxed").with_columns(
        pl.lit(market).alias("market"),
        pl.when(pl.col("book").is_in(list(paths.REFERENCE_BOOKS)))
        .then(pl.lit("reference"))
        .otherwise(pl.lit("entry"))
        .alias("role"),
        pl.lit(LEDGER_SCHEMA_VERSION).alias("schema_version"),
    )
    return add_clv(ledger, market=market)


def ledger_path(
    config, config_path: Path | str, *, market: str = "total_games"
) -> Path:
    """Where this config's ledger lives — keyed by config CONTENT, not filename.

    Stem-keying collided: a sweep over N hyperparameter variants of one config
    wrote every variant to the same path, so each silently overwrote the last and
    the comparison was one model against itself.
    """
    from mvp.projection.iid.artifacts import backtest_name, fp_dir_for

    return fp_dir_for(config, Path(config_path)) / backtest_name(market)


def run_backtest(
    config_path: Path | str,
    *,
    retrain: bool = False,
    source: str | None = None,
    run_id: str | None = None,
    markets: tuple[str, ...] = ("total_games", "game_spread"),
) -> dict[str, Path]:
    """Project, price against the oddspapi board, settle, and write the ledger.

    Signature preserved from the retired `backtest.run_backtest` so `cli.py` and
    `sweep.py` call it unchanged — `retrain` in particular still trains, rather
    than becoming a parameter whose meaning was quietly removed.

    Writes parquet, not CSV: `rank.py` reads a handful of columns per fingerprint
    dir and the long grain makes the file far larger than the 45-column wide one it
    replaces, so projection pushdown is the difference between reading five columns
    and thirty.

    **One ledger per market, and the return is a mapping.** `rank.py`'s contract is
    one table per (instrument, market) and never a pooled market; the two markets'
    outcome columns differ, so a single frame could not hold both without either a
    shared outcome column or sparse per-market ones. Separate files also keep
    `print_backtest_summary` honest -- it groups by anchor alone, so two markets in
    one frame would blend into a single row per anchor.

    The signature change from `Path` to `dict[str, Path]` touches one caller
    (`cli.py`), which passes the result to `print_backtest_summary`; `sweep.py`
    discards it.
    """
    from mvp.projection.iid.artifacts import backtest_name
    from mvp.projection.iid.projection_run import run_projection

    run = run_projection(
        config_path, retrain=retrain, source=source, run_id=run_id
    )
    paths_out: dict[str, Path] = {}
    for mkt in markets:
        # Recorded before the attempt so the caller can tell a market was TRIED
        # and produced nothing from one that was never asked for. `cli.py` prints
        # "no ledger written" off exactly this path.
        out_path = run.fp_dir / backtest_name(mkt)
        paths_out[mkt] = out_path
        try:
            ledger = build_ledger(run.pmf_for(mkt), market=mkt)
        except MarketNotCarried as exc:
            # A fact about the stage tree, not about this config, so it must not
            # take the OTHER market's ledger down with it -- a missing spread
            # file would otherwise lose the totals run that already succeeded.
            # Nothing is written, so the trial stays incomplete and retries.
            # Caught narrowly: a bare `RuntimeError` would also swallow
            # NotImplementedError and RecursionError from anywhere below.
            logger.warning("%s: %s — skipping this market", mkt, exc)
            continue
        if ledger.is_empty():
            # Write the empty frame rather than skipping. `sweep._is_complete`
            # decides a trial is done by which artifacts exist, so a market that
            # legitimately produces no rows would otherwise leave the trial
            # permanently incomplete and re-running on every sweep invocation --
            # the exact failure that check's own docstring warns about. An empty
            # file says "this ran and there was nothing"; a missing one cannot.
            logger.warning("%s ledger is empty — writing 0 rows to %s", mkt, out_path)
        ledger.write_parquet(out_path)
        if ledger.is_empty():
            continue
        logger.info(
            "Wrote %d %s ledger rows -> %s", ledger.height, mkt, out_path
        )
    return paths_out


def print_backtest_summary(path: Path | str) -> None:
    """A shape-of-the-ledger view: what was offered, where, and how it settled.

    A rewrite rather than a repoint. The retired version read `open_edge_novig`,
    `close_edge`, `pnl_open`/`pnl_close`/`pnl_formed`, `clv_open`, `bet_type`,
    `mean_covers`, `best_of` and `is_main_line` off a wide CSV, and its
    aggregator referenced `clv_open` unconditionally — so it raised on the first
    view of a long ledger rather than degrading.

    Deliberately NOT a performance table. Ranking configs is `iid-rank`'s job,
    where the anchor, the selection policy and the edge band are all named; a
    second place computing ROI is a second place for those choices to differ
    silently.
    """
    path = Path(path)
    if not path.exists():
        print(f"No ledger at {path}")
        return
    df = pl.read_parquet(path)
    if df.is_empty():
        print(f"Ledger at {path} is empty")
        return

    print(f"\n{path}")
    print(f"  {df.height:,} rows | schema_version="
          f"{df['schema_version'][0] if 'schema_version' in df.columns else '?'}")
    print(f"  {df['match_uid'].n_unique():,} matches, "
          f"{df['book'].n_unique()} books")

    by_anchor = (
        df.group_by("anchor")
        .agg(
            pl.len().alias("rows"),
            pl.col("match_uid").n_unique().alias("matches"),
            pl.col("is_main_line").sum().alias("main_rows"),
            pl.col("edge").mean().alias("avg_edge"),
            pl.col("is_push").sum().alias("pushes"),
        )
    )
    by_anchor = _in_board_order(by_anchor)
    print(f"\n  {'anchor':>10} {'rows':>8} {'matches':>8} {'main':>7} "
          f"{'avg_edge':>9} {'pushes':>7}")
    for r in by_anchor.iter_rows(named=True):
        print(f"  {r['anchor']:>10} {r['rows']:>8,} {r['matches']:>8,} "
              f"{r['main_rows']:>7,} {r['avg_edge']:>9.4f} {r['pushes']:>7,}")

    neg = int((df["edge"] < 0).sum())
    print(f"\n  negative-edge rows: {neg:,} ({neg / df.height:.1%})")

    _print_edge_curve(df)
    print("  Config-vs-config comparison: iid-rank\n")


def _anchors_in_board_order(df: pl.DataFrame) -> list[str]:
    """Anchors present in `df`, in the order the board reaches them.

    NOT alphabetical, which renders close/formed2/open and reads the price path
    backwards. Anything outside `DEFAULT_ANCHORS` is appended rather than
    dropped, so an unrecognised anchor stays visible instead of vanishing.
    """
    present = set(df["anchor"].unique().to_list())
    known = [a for a in DEFAULT_ANCHORS if a in present]
    return known + sorted(present - set(known))


def _in_board_order(df: pl.DataFrame) -> pl.DataFrame:
    """Sort an anchor-keyed summary frame into board order."""
    order = {a: i for i, a in enumerate(_anchors_in_board_order(df))}
    return (
        df.with_columns(
            pl.col("anchor")
            .replace_strict(order, return_dtype=pl.Int32).alias("_ord")
        )
        .sort("_ord")
        .drop("_ord")
    )


# Edge bands for the single-model curve. This is where the curve belongs: it is
# a property of ONE config, and putting it in `iid-rank` would render a curve
# per config per policy and bury the ranking it exists to do.
_CURVE_BANDS: tuple[tuple[str, float, float], ...] = (
    ("<0", -1.0, 0.0),
    ("0-2pp", 0.0, 0.02),
    ("2-5pp", 0.02, 0.05),
    ("5-10pp", 0.05, 0.10),
    ("10pp+", 0.10, 1.0),
)


def _print_edge_curve(df: pl.DataFrame) -> None:
    """ROI by edge band, per anchor, on main-line entry rows.

    The question this answers is where the model's claimed edge starts being
    real. If ROI rose with the band the edge would be honest; a peak in the
    middle says the largest claimed edges are the least trustworthy, which is
    the selection effect an edge filter creates.

    One bet per (match, book) is NOT enforced here — this view is about the
    edge/return relationship across the offer set, not about a runnable
    strategy. Strategy selection is `iid-rank`'s job.
    """
    need = {"anchor", "is_main_line", "edge", "pnl", "won"}
    if not need <= set(df.columns):
        return
    rows = df.filter(pl.col("is_main_line").fill_null(False))
    if "role" in rows.columns:
        rows = rows.filter(pl.col("role") == "entry")
    if "live_side" in rows.columns:
        rows = rows.filter(pl.col("live_side").fill_null(True))
    rows = rows.drop_nulls(["edge", "pnl"])
    if rows.is_empty():
        return

    has_clv = "clv" in rows.columns
    print("\n  ROI and CLV by edge band (main line, entry books):")
    print(f"  {'anchor':>8} {'band':>7} {'N':>7} {'Hit%':>6} {'ROI%':>7} "
          f"{'Units':>9} {'avg edge%':>10} {'CLV%':>8} {'CLV+%':>7}")
    for anchor in _anchors_in_board_order(rows):
        sub = rows.filter(pl.col("anchor") == anchor)
        for name, lo, hi in _CURVE_BANDS:
            band = sub.filter((pl.col("edge") >= lo) & (pl.col("edge") < hi))
            if band.is_empty():
                continue
            won = band["won"].drop_nulls()
            hit = f"{won.mean()*100:.1f}" if won.len() else "--"
            clv = band["clv"].drop_nulls() if has_clv else None
            clv_s = f"{clv.mean()*100:+.2f}" if clv is not None and clv.len() else "--"
            clvp = (
                f"{(clv > 0).mean()*100:.1f}"
                if clv is not None and clv.len() else "--"
            )
            print(
                f"  {anchor:>8} {name:>7} {band.height:>7,} {hit:>6} "
                f"{band['pnl'].mean()*100:>+7.2f} {band['pnl'].sum():>+9.1f} "
                f"{band['edge'].mean()*100:>+10.2f} {clv_s:>8} {clvp:>7}"
            )

    if "side" not in rows.columns:
        return
    # A tuning change often floods one side rather than moving overall ROI, so
    # the side split is where that shows. It lives here rather than in
    # `iid-rank` because the ranking table's cell groups are the selection
    # policies; this is per-config detail.
    print("\n  by side (edge>=0, main line, entry books):")
    print(f"  {'anchor':>8} {'side':>6} {'N':>7} {'Hit%':>6} {'ROI%':>7} "
          f"{'Units':>9}")
    gated = rows.filter(pl.col("edge") >= 0.0)
    for anchor in _anchors_in_board_order(gated):
        sub = gated.filter(pl.col("anchor") == anchor)
        for side in sorted(sub["side"].unique().to_list()):
            s = sub.filter(pl.col("side") == side)
            if s.is_empty():
                continue
            won = s["won"].drop_nulls()
            hit = f"{won.mean()*100:.1f}" if won.len() else "--"
            print(
                f"  {anchor:>8} {side:>6} {s.height:>7,} {hit:>6} "
                f"{s['pnl'].mean()*100:>+7.2f} {s['pnl'].sum():>+9.1f}"
            )
