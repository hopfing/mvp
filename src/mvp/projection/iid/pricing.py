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

import polars as pl

from mvp.oddspapi import paths

logger = logging.getLogger(__name__)

PMF_COL = "total_games_pmf"


def cumulative(pmf: pl.DataFrame) -> pl.DataFrame:
    """(match_uid, games, p, p_at_or_below) -- the pmf as a long, cumulative table.

    The pmf list is indexed BY GAME COUNT: element i is P(total == i games), verified
    against `expected_total_games` (sum of i*p_i reproduces it exactly). Exploding
    rather than slicing keeps the arithmetic legible and lets the line join be an
    ordinary equality rather than a per-row offset.
    """
    missing = {"match_uid", PMF_COL} - set(pmf.columns)
    if missing:
        raise ValueError(f"pmf frame is missing {sorted(missing)}")
    return (
        pmf.select("match_uid", PMF_COL)
        .with_row_index("_r")
        .explode(PMF_COL)
        .with_columns(pl.int_range(pl.len()).over("_r").alias("games"))
        .rename({PMF_COL: "p"})
        .with_columns(pl.col("p").cum_sum().over("_r").alias("p_at_or_below"))
        .drop("_r")
    )


def price(board: pl.DataFrame, pmf: pl.DataFrame) -> pl.DataFrame:
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
    cum = cumulative(pmf)
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
    return (
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


def settle(priced: pl.DataFrame, outcomes: pl.DataFrame) -> pl.DataFrame:
    """Settle both sides of every rung against the realised total.

    `outcomes` is (match_uid, actual_total). Matches absent from it are dropped rather
    than scored: a retirement, walkover or default produces fewer games than the match
    would have, so scoring one against a total records an "under" no book would have
    settled that way. The exclusion belongs upstream, in whatever built `outcomes`.

    A push returns the stake -- pnl 0, not a loss -- and is counted separately so it
    cannot be mistaken for a break-even bet in a hit rate.
    """
    if priced.is_empty():
        return priced
    if "actual_total" not in outcomes.columns:
        raise ValueError("outcomes needs (match_uid, actual_total)")
    out = priced.join(
        outcomes.select("match_uid", "actual_total"), on="match_uid", how="inner"
    )
    # A null outcome makes both `push` and `over_won` null, and a null predicate
    # routes to `.otherwise`, landing the row as a loss on BOTH sides. Drop rather
    # than score: an unknown result is not a losing bet.
    unknown = out.filter(pl.col("actual_total").is_null()).height
    if unknown:
        logger.warning("settle: dropping %d rows with a null actual_total", unknown)
        out = out.drop_nulls("actual_total")
    over_won = pl.col("actual_total") > pl.col("points")
    push = pl.col("actual_total") == pl.col("points")
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
            pl.when(take_over).then(pl.lit("over")).otherwise(pl.lit("under"))
            .alias("side"),
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
