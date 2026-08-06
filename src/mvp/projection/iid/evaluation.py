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

# Bumped when the ledger's contract changes. Carried as a column so a reader can
# tell contracts apart without depending on `projection_evaluations` happening to
# hold exactly one directory.
LEDGER_SCHEMA_VERSION = 1

# Anchors written into every ledger. `open` and `close` answer the entry-timing
# question; `formed(2)` is the first moment a second entry book agrees there is a
# market. Fixed offsets serve the decay curve and are deliberately not here — the
# ledger is long, so adding one later is a new `anchor` value, not a new column.
DEFAULT_ANCHORS: tuple[str, ...] = ("open", "formed2", "close")


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


# ---------------------------------------------------------------------------
# The ledger: settle() output reshaped to one row per side
# ---------------------------------------------------------------------------

# Columns that describe the RUNG and are therefore shared by both of its sides.
_RUNG_COLS = (
    "match_uid", "book", "points",
    "p_over", "overround", "imbalance", "separation",
    "live", "quote_age_s", "last_change_s", "n_two_sided", "is_main_line",
    "line_offset",
    "p_push", "actual_total", "is_push",
)

# (side, odds, model_p, edge, edge_novig, pnl, live) column per side.
_SIDE_COLS = {
    "over": ("over_odds", "model_p_over", "edge_over", "edge_novig_over",
             "pnl_over", "live_over"),
    "under": ("under_odds", "model_p_under", "edge_under", "edge_novig_under",
              "pnl_under", "live_under"),
}


def unpivot_sides(settled: pl.DataFrame) -> pl.DataFrame:
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

    shared = [c for c in _RUNG_COLS if c in settled.columns]
    frames: list[pl.DataFrame] = []
    for side, (odds, model_p, edge, edge_novig, pnl, live) in _SIDE_COLS.items():
        needed = {odds, model_p, edge, pnl}
        if not needed <= set(settled.columns):
            raise ValueError(
                f"settled frame is missing {sorted(needed - set(settled.columns))} "
                f"— unpivot_sides expects settle() output"
            )
        won = (
            pl.col("over_won") if side == "over" else pl.col("over_won").not_()
        )
        exprs = [
            pl.lit(side).alias("side"),
            pl.col(odds).alias("odds"),
            pl.col(model_p).alias("model_p"),
            pl.col(edge).alias("edge"),
            pl.col(pnl).alias("pnl"),
            won.alias("won"),
        ]
        if edge_novig in settled.columns:
            exprs.append(pl.col(edge_novig).alias("edge_novig"))
        if live in settled.columns:
            exprs.append(pl.col(live).alias("live_side"))
        frames.append(settled.select(*shared, *exprs))

    return pl.concat(frames, how="vertical_relaxed")


def add_mean_covers(ledger: pl.DataFrame, pmf: pl.DataFrame) -> pl.DataFrame:
    """Does the chain's EXPECTED total land on the side being bet?

    A per-side model-agreement gate, not a property of the rung: the pmf tail can
    put the model over the price while its centre disagrees, and the same rung
    therefore gets `mean_covers` true on one side and false on the other. This is
    the gate that separated bet-selection from chain error — H71 attributed ~80%
    of the favourite bleed to selection using it.
    """
    if ledger.is_empty():
        return ledger
    if "expected_total_games" not in pmf.columns:
        raise ValueError("pmf frame needs expected_total_games for mean_covers")
    out = ledger.join(
        pmf.select("match_uid", "expected_total_games").unique(subset=["match_uid"]),
        on="match_uid",
        how="left",
    )
    return out.with_columns(
        pl.when(pl.col("side") == "over")
        .then(pl.col("expected_total_games") > pl.col("points"))
        .otherwise(pl.col("expected_total_games") < pl.col("points"))
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
    p_ref = (
        pl.when(pl.col("side") == "over")
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
    """`(match_uid, t)` for one named anchor, scoped to the projected matches."""
    from mvp.oddspapi import anchors

    if name == "open":
        return anchors.open_(market, match_uids=match_uids)
    if name.startswith("formed"):
        n = int(name.removeprefix("formed") or 2)
        return anchors.formed(market, n, match_uids=match_uids)
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
        raise RuntimeError(f"no entry books carry {market}")
    uids = pmf["match_uid"].unique()
    outcomes = pmf.select("match_uid", "actual_total")

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
        priced = price(board_df, pmf)
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
        settled = settle(priced, outcomes)
        if settled.is_empty():
            logger.warning("anchor %s settled to nothing; skipping", name)
            continue
        rows = add_mean_covers(unpivot_sides(settled), pmf)
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


def ledger_path(config, config_path: Path | str) -> Path:
    """Where this config's ledger lives — keyed by config CONTENT, not filename.

    Stem-keying collided: a sweep over N hyperparameter variants of one config
    wrote every variant to the same path, so each silently overwrote the last and
    the comparison was one model against itself.
    """
    from mvp.projection.iid.artifacts import BACKTEST_PARQUET, fp_dir_for

    return fp_dir_for(config, Path(config_path)) / BACKTEST_PARQUET


def run_backtest(
    config_path: Path | str,
    *,
    retrain: bool = False,
    source: str | None = None,
    run_id: str | None = None,
) -> Path:
    """Project, price against the oddspapi board, settle, and write the ledger.

    Signature preserved from the retired `backtest.run_backtest` so `cli.py` and
    `sweep.py` call it unchanged — `retrain` in particular still trains, rather
    than becoming a parameter whose meaning was quietly removed.

    Writes `backtest.parquet`, not CSV: `rank.py` reads a handful of columns per
    fingerprint dir and the long grain makes the file far larger than the 45-column
    wide one it replaces, so projection pushdown is the difference between reading
    five columns and thirty.
    """
    from mvp.projection.iid.artifacts import BACKTEST_PARQUET
    from mvp.projection.iid.projection_run import run_projection

    run = run_projection(
        config_path, retrain=retrain, source=source, run_id=run_id
    )
    ledger = build_ledger(run.pmf)
    out_path = run.fp_dir / BACKTEST_PARQUET
    if ledger.is_empty():
        logger.warning("ledger is empty — writing nothing to %s", out_path)
        return out_path
    ledger.write_parquet(out_path)
    logger.info("Wrote %d ledger rows -> %s", ledger.height, out_path)
    return out_path


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
          f"{df['book'].n_unique()} books, "
          f"{df['market'].n_unique()} market(s)")

    by_anchor = (
        df.group_by("anchor")
        .agg(
            pl.len().alias("rows"),
            pl.col("match_uid").n_unique().alias("matches"),
            pl.col("is_main_line").sum().alias("main_rows"),
            pl.col("edge").mean().alias("avg_edge"),
            pl.col("is_push").sum().alias("pushes"),
        )
        .sort("anchor")
    )
    print(f"\n  {'anchor':>10} {'rows':>8} {'matches':>8} {'main':>7} "
          f"{'avg_edge':>9} {'pushes':>7}")
    for r in by_anchor.iter_rows(named=True):
        print(f"  {r['anchor']:>10} {r['rows']:>8,} {r['matches']:>8,} "
              f"{r['main_rows']:>7,} {r['avg_edge']:>9.4f} {r['pushes']:>7,}")

    neg = int((df["edge"] < 0).sum())
    print(f"\n  negative-edge rows: {neg:,} ({neg / df.height:.1%}) — the ledger "
          "is the OFFER set; selection happens at read time.")

    _print_edge_curve(df)
    print("  Config-vs-config comparison: iid-rank\n")


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
    for anchor in sorted(rows["anchor"].unique().to_list()):
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
    for anchor in sorted(gated["anchor"].unique().to_list()):
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
