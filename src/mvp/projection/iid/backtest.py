"""Backtest the IID projector against captured 2026 totals/spread book lines.

Single entry point: train (lazy) → project on 2026 settled matches with
event_map coverage → join to per-book closing snapshots → settle vs actuals →
emit a bet-level CSV (one row per match × market × line × side × book where
the model has positive no-vig edge over the book).

Artifact: B:/projections/iid/{config_stem}.joblib
Output:   B:/projections/iid/backtests/{config_stem}.csv
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import polars as pl

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.projection.iid.artifacts import (
    BACKTEST_CSV,
    SERVE_MODEL_JOBLIB,
    fp_dir_for,
    record_run,
    write_pmf_parquet,
)
from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.engine import make_fs_engine
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.projection.iid.config import IIDProjectionConfig
from mvp.projection.iid.projector import ProjectionOutput, TennisProjector
from mvp.projection.iid.serve_model import (
    ScoreStateChainServeModel,
    build_serve_model,
)

logger = logging.getLogger(__name__)

# (book_code_in_event_map, stage_dir_name, event_id_col_in_stage)
_BOOKS: list[tuple[str, str, str]] = [
    ("czr", "caesars", "czr_event_id"),
    ("mgm", "betmgm", "mgm_event_id"),
    ("dk", "draftkings", "dk_event_id"),
    ("br", "betrivers", "br_event_id"),
]

def artifact_path(config: IIDProjectionConfig, config_path: Path) -> Path:
    """Trained-projector cache, keyed by config CONTENT rather than filename stem.

    Stem-keying collided: a sweep over N hyperparameter variants of one config
    wrote every variant to the same `<stem>.joblib` / `<stem>.csv`, so each run
    silently overwrote the last and the comparison was between one model and
    itself.
    """
    return fp_dir_for(config, config_path) / SERVE_MODEL_JOBLIB


def output_path(config: IIDProjectionConfig, config_path: Path) -> Path:
    return fp_dir_for(config, config_path) / BACKTEST_CSV


_RUNNER_COLUMNS = [
    "match_uid", "player_id", "won", "reason", "best_of",
    "circuit", "surface", "round", "effective_match_date",
    "player_set1_games", "player_set2_games",
    "player_set3_games", "player_set4_games", "player_set5_games",
    "opp_set1_games", "opp_set2_games",
    "opp_set3_games", "opp_set4_games", "opp_set5_games",
    "pts_service_pts_won", "pts_service_pts_played",
    "opp_pts_service_pts_won", "opp_pts_service_pts_played",
    "svc_games_played", "svc_bp_saved", "svc_bp_faced",
    "opp_svc_games_played", "opp_svc_bp_saved", "opp_svc_bp_faced",
    "player_set1_tiebreak", "player_set2_tiebreak",
    "player_set3_tiebreak", "player_set4_tiebreak", "player_set5_tiebreak",
    "opp_set1_tiebreak", "opp_set2_tiebreak",
    "opp_set3_tiebreak", "opp_set4_tiebreak", "opp_set5_tiebreak",
    "player_first_name", "player_last_name",
    "opp_first_name", "opp_last_name", "opp_id",
    "tournament_id", "tournament_name",
]


def _resolve_targets(df: pl.DataFrame) -> pl.DataFrame:
    df = df.filter(
        pl.col("player_set1_games").is_not_null()
        & pl.col("player_set2_games").is_not_null()
    )
    if "reason" in df.columns:
        df = df.filter(
            pl.col("reason").fill_null("").is_in(["W/O", "RET", "DEF", "UNP"]).not_()
        )
    df = df.with_columns(
        total_games_won().cast(pl.Float64).alias("_target_games_a"),
        total_games_lost().cast(pl.Float64).alias("_target_games_b"),
    )
    return df.filter(
        pl.col("_target_games_a").is_not_null()
        & pl.col("_target_games_b").is_not_null()
    )


def _collapse_to_match_rows(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort(["match_uid", "player_id"]).unique(
        subset=["match_uid"], keep="first", maintain_order=True,
    )


def _compute_features(
    config: IIDProjectionConfig,
) -> tuple[pl.DataFrame, Any]:
    matches_path = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
    cache_dir = get_local_data_root() / "features" / "cache"
    engine = make_fs_engine(matches_path=matches_path, cache_dir=cache_dir)

    feature_specs = config.features.include
    compute_only = config.features.compute_only or []
    filter_specs = get_filter_feature_specs(config.data.filters)
    extra = compute_only + filter_specs
    all_specs = feature_specs + [s for s in extra if s not in feature_specs]

    runner_columns = list(_RUNNER_COLUMNS)
    if config.data.filters:
        for col in config.data.filters:
            if col not in runner_columns:
                runner_columns.append(col)

    return engine.compute(all_specs, extra_columns=runner_columns), engine


def _train_projector(
    config: IIDProjectionConfig, df: pl.DataFrame, engine: Any = None,
) -> TennisProjector:
    """Fit the projector on the config's training window."""
    train_df = df
    if config.data.filters:
        train_df = apply_filters(train_df, config.data.filters)
    train_df = _resolve_targets(train_df)
    train_df = train_df.filter(
        (pl.col("effective_match_date") >= config.data.date_range.start)
        & (pl.col("effective_match_date") <= config.data.date_range.end)
    )
    train_df = train_df.filter(pl.col("best_of").is_in([3, 5]))
    train_df = _collapse_to_match_rows(train_df)
    if len(train_df) == 0:
        raise ValueError("No training matches after filters")
    logger.info("Training projector on %d matches", len(train_df))
    serve_model = build_serve_model(config.serve_model, engine=engine)
    projector = TennisProjector(serve_model)
    projector.fit(train_df)
    return projector


def _save_artifact(
    projector: TennisProjector, config: IIDProjectionConfig, config_path: Path,
    n_train: int,
) -> None:
    path = artifact_path(config, config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "serve_model": projector.serve_model,
        "config_path": str(config_path),
        "config_yaml": Path(config_path).read_text(encoding="utf-8"),
        "n_train": n_train,
        "trained_at": datetime.now(UTC).isoformat(),
    }
    joblib.dump(artifact, path)
    logger.info("Saved IID artifact to %s", path)


def _load_artifact(
    config: IIDProjectionConfig, config_path: Path,
) -> TennisProjector | None:
    """Load the cached projector, or None if it doesn't match the current config.

    The fingerprint already keys the dir by content, so a mismatch here should be
    rare — but the artifact also stores the config text it was trained from, and
    fields outside the fingerprint (or a hand-edited file) can still diverge.
    Without the check, editing a config and re-running without `--retrain`
    silently scores the OLD model.
    """
    path = artifact_path(config, config_path)
    if not path.exists():
        return None
    artifact = joblib.load(path)
    current = Path(config_path).read_text(encoding="utf-8")
    if artifact.get("config_yaml") != current:
        logger.info(
            "IID artifact at %s was trained from different config text — retraining",
            path,
        )
        return None
    return TennisProjector(serve_model=artifact["serve_model"])


def _train_or_load(
    config: IIDProjectionConfig, config_path: Path, df: pl.DataFrame,
    *, retrain: bool, engine: Any = None,
) -> TennisProjector:
    if not retrain:
        cached = _load_artifact(config, config_path)
        if cached is not None:
            logger.info("Loading existing IID artifact for %s", config_path.stem)
            return cached
    projector = _train_projector(config, df, engine=engine)
    _save_artifact(projector, config, config_path, n_train=len(df))
    return projector


def _build_test_set(
    config: IIDProjectionConfig,
    df: pl.DataFrame,
    *,
    require_odds_coverage: bool = True,
) -> pl.DataFrame:
    """Filter to settled 2026 matches; collapse to one row per match.

    `require_odds_coverage=False` skips the staged-book event_map join, giving
    every settleable 2026 match — the universe the pmf dump wants, since oddspapi
    prices matches the staged books don't carry.
    """
    test = df
    if config.data.filters:
        test = apply_filters(test, config.data.filters)
    test = _resolve_targets(test)
    test = test.filter(pl.col("effective_match_date") >= pl.date(2026, 1, 1))
    test = test.filter(pl.col("best_of").is_in([3, 5]))
    test = _collapse_to_match_rows(test)

    if not require_odds_coverage:
        return test
    em = pl.read_parquet(get_data_root() / "odds" / "event_map.parquet")
    covered = em.select("match_uid").unique()
    test = test.join(covered, on="match_uid", how="inner")
    return test


def _pmf_frame(test_df: pl.DataFrame, out: ProjectionOutput) -> pl.DataFrame:
    """Per-match total-games pmf — the probability source for CLV scoring.

    Column set matches scripts/oddspapi/iid_project_dump.py so the oddspapi
    scorer reads it unchanged; the difference is that this one is written per
    fingerprint instead of to a single fixed path that every run overwrote.
    """
    pmf = out.distribution.total_games_pmf
    return pl.DataFrame({
        "match_uid": test_df["match_uid"],
        "circuit": test_df["circuit"],
        "surface": test_df["surface"],
        "round": test_df["round"],
        "best_of": test_df["best_of"].cast(pl.Int64),
        "actual_total": (
            test_df["_target_games_a"] + test_df["_target_games_b"]
        ).cast(pl.Float64),
        "p_match_win_a": out.distribution.p_match_win_a,
        "expected_total_games": out.distribution.expected_total_games,
        "total_games_pmf": [row.tolist() for row in pmf],
    })


def _project(projector: TennisProjector, test_df: pl.DataFrame) -> ProjectionOutput:
    return projector.project(test_df)


# ---------------- odds-side join helpers ----------------

def _closing_snapshots(
    market_df: pl.DataFrame, event_id_col: str,
) -> pl.DataFrame:
    """One row per (event_id, market, points, side): the latest pre-event snapshot."""
    pre = market_df.filter(pl.col("event_status") == "NOT_STARTED")
    if len(pre) == 0:
        return pre
    return (
        pre.sort("fetched_at")
        .group_by([event_id_col, "market", "points", "side", "player_name"])
        .agg(pl.col("odds").last(), pl.col("fetched_at").last())
    )


def _prematch_snapshots(
    market_df: pl.DataFrame, event_id_col: str,
) -> pl.DataFrame:
    """Every pre-event snapshot, not just the last one.

    The backtest used to read closing prices only, which is the wrong end of the
    market for a strategy that enters at open. Keeping the full prematch series
    lets `compute_open_close_odds` place open / formed / close at time-aligned
    instants across books.
    """
    return market_df.filter(
        (pl.col("event_status") == "NOT_STARTED") & pl.col("odds").is_not_null()
    )


def _price_points(
    snapshots: pl.DataFrame, side_col: str, line_col: str = "points",
) -> pl.DataFrame:
    """Time-aligned open/formed/close per (match_uid, side, line).

    Delegates to the odds aggregator so line markets use the same instants and
    the same best-across-books rule as the moneyline pipeline: per-book first and
    last are hours apart, so maxing each book's own first price blends moments
    that never coexisted on the board.
    """
    from mvp.odds.aggregator import compute_open_close_odds

    if len(snapshots) == 0:
        return pl.DataFrame()
    frame = snapshots.rename({side_col: "side"}) if side_col != "side" else snapshots
    out = compute_open_close_odds(frame, extra_keys=[line_col])
    # The aggregator returns its id column as `player_id` regardless of input.
    return out.rename({"player_id": "side"})


def _book_totals_snapshots(
    book_code: str, stage_dir: str, event_id_col: str, em: pl.DataFrame,
) -> pl.DataFrame:
    """Prematch total_games snapshots for one book, resolved to match_uid.

    Returns the full time series rather than a closing pair — pricing happens
    once across all books in `_totals_prices`, so open/formed/close land on
    instants that actually existed on the board.
    """
    path = get_data_root() / "stage" / stage_dir / "total_games.parquet"
    if not path.exists():
        return pl.DataFrame()
    pre = _prematch_snapshots(pl.read_parquet(path), event_id_col)
    if len(pre) == 0:
        return pl.DataFrame()

    em_book = em.filter(pl.col("book") == book_code).select(
        pl.col("event_id"), "match_uid", "p1_id", "p2_id",
    )
    joined = pre.join(em_book, left_on=event_id_col, right_on="event_id", how="inner")
    if len(joined) == 0:
        return pl.DataFrame()
    return joined.select(
        "match_uid", "p1_id", "p2_id", "points", "side", "odds",
        "fetched_at", "event_status",
        pl.lit(book_code).alias("book"),
    )


def _totals_prices(snapshots: pl.DataFrame) -> pl.DataFrame:
    """Per (match_uid, points): over/under prices at open, formed and close.

    One row per line, best-across-books at each instant — not one row per book.
    Books are shopped, so the price that matters is the best one available at
    the moment of entry; a row per book would count the same bet repeatedly at
    prices you would not have taken.
    """
    if len(snapshots) == 0:
        return pl.DataFrame()
    priced = _price_points(snapshots, side_col="side")
    if len(priced) == 0:
        return pl.DataFrame()

    depth = snapshots.group_by(["match_uid", "points"]).agg(
        pl.col("book").n_unique().alias("n_books")
    )
    ids = snapshots.group_by(["match_uid", "points"]).agg(
        pl.col("p1_id").first(), pl.col("p2_id").first()
    )

    wide = None
    for side, prefix in (("over", "over"), ("under", "under")):
        part = priced.filter(pl.col("side") == side).select(
            "match_uid", "points",
            pl.col("best_opening_odds").alias(f"{prefix}_open"),
            pl.col("formed_odds").alias(f"{prefix}_formed"),
            pl.col("best_closing_odds").alias(f"{prefix}_close"),
        )
        wide = part if wide is None else wide.join(
            part, on=["match_uid", "points"], how="inner"
        )
    if wide is None or len(wide) == 0:
        return pl.DataFrame()
    return wide.join(depth, on=["match_uid", "points"], how="left").join(
        ids, on=["match_uid", "points"], how="left"
    )


def _book_spread_snapshots(
    book_code: str, stage_dir: str, event_id_col: str, em: pl.DataFrame,
) -> pl.DataFrame:
    """Prematch game_spread snapshots for one book, sides resolved to p1/p2."""
    path = get_data_root() / "stage" / stage_dir / "game_spread.parquet"
    if not path.exists():
        return pl.DataFrame()
    closing = _prematch_snapshots(pl.read_parquet(path), event_id_col)
    if len(closing) == 0:
        return pl.DataFrame()

    em_book = em.filter(pl.col("book") == book_code).select(
        pl.col("event_id"), "match_uid", "p1_id", "p2_id",
        "p1_book_name", "p2_book_name",
    )
    joined = closing.join(em_book, left_on=event_id_col, right_on="event_id", how="inner")
    if len(joined) == 0:
        return pl.DataFrame()

    # Resolve each row's bet-side player_id from player_name.
    joined = joined.with_columns(
        pl.when(pl.col("player_name") == pl.col("p1_book_name"))
        .then(pl.col("p1_id"))
        .when(pl.col("player_name") == pl.col("p2_book_name"))
        .then(pl.col("p2_id"))
        .otherwise(pl.lit(None))
        .alias("side_player_id")
    ).filter(pl.col("side_player_id").is_not_null())

    if len(joined) == 0:
        return pl.DataFrame()

    # Pair the two sides by (match_uid, abs_points). The two sides have
    # opposite-sign points; absolute value defines the line magnitude.
    joined = joined.with_columns(pl.col("points").abs().alias("abs_points"))

    # Side label ("p1"/"p2") + line magnitude are the pricing key; the signed
    # points per side are carried alongside so the settle keeps the handicap sign.
    return joined.select(
        "match_uid", "p1_id", "p2_id", "points",
        pl.col("abs_points"),
        pl.when(pl.col("side_player_id") == pl.col("p1_id"))
        .then(pl.lit("p1")).otherwise(pl.lit("p2")).alias("side"),
        "odds", "fetched_at", "event_status",
        pl.lit(book_code).alias("book"),
    )


def _spread_prices(snapshots: pl.DataFrame) -> pl.DataFrame:
    """Per (match_uid, abs_points): p1/p2 prices at open, formed and close."""
    if len(snapshots) == 0:
        return pl.DataFrame()
    priced = _price_points(snapshots, side_col="side", line_col="abs_points")
    if len(priced) == 0:
        return pl.DataFrame()

    depth = snapshots.group_by(["match_uid", "abs_points"]).agg(
        pl.col("book").n_unique().alias("n_books"),
        pl.col("p1_id").first(), pl.col("p2_id").first(),
    )
    signed = (
        snapshots.group_by(["match_uid", "abs_points", "side"])
        .agg(pl.col("points").first())
        .pivot(values="points", index=["match_uid", "abs_points"], on="side")
    )
    if "p1" not in signed.columns or "p2" not in signed.columns:
        return pl.DataFrame()
    signed = signed.rename({"p1": "p1_points", "p2": "p2_points"})

    wide = None
    for side in ("p1", "p2"):
        part = priced.filter(pl.col("side") == side).select(
            "match_uid", "abs_points",
            pl.col("best_opening_odds").alias(f"{side}_open"),
            pl.col("formed_odds").alias(f"{side}_formed"),
            pl.col("best_closing_odds").alias(f"{side}_close"),
        )
        wide = part if wide is None else wide.join(
            part, on=["match_uid", "abs_points"], how="inner"
        )
    if wide is None or len(wide) == 0:
        return pl.DataFrame()
    return (
        wide.join(signed, on=["match_uid", "abs_points"], how="inner")
        .join(depth, on=["match_uid", "abs_points"], how="left")
    )


# ---------------- distribution lookups ----------------

def _p_over_at(pmf: np.ndarray, line: float) -> np.ndarray:
    """P(total > line) per match, given (N, K) total_games_pmf."""
    threshold = int(np.floor(line)) + 1
    if threshold < 0:
        threshold = 0
    if threshold >= pmf.shape[1]:
        return np.zeros(pmf.shape[0], dtype=np.float64)
    return pmf[:, threshold:].sum(axis=1)


def _p_a_cover_at(
    pmf: np.ndarray, spread_offset: int, line: float,
) -> np.ndarray:
    """P((games_a - games_b) > line) per match, given (N, 2K+1) spread_pmf."""
    threshold = int(np.floor(line)) + 1 + spread_offset
    if threshold < 0:
        threshold = 0
    if threshold >= pmf.shape[1]:
        return np.zeros(pmf.shape[0], dtype=np.float64)
    return pmf[:, threshold:].sum(axis=1)


# ---------------- main backtest ----------------

def _build_predictions_frame(
    test_df: pl.DataFrame, out: ProjectionOutput,
) -> pl.DataFrame:
    """One row per match with model summary + actuals + per-match pmf indices."""
    return pl.DataFrame({
        "match_uid": test_df["match_uid"],
        "effective_match_date": test_df["effective_match_date"],
        "a_player_id": test_df["player_id"],
        "b_player_id": test_df["opp_id"],
        "a_name": (
            test_df["player_first_name"].fill_null("") + " "
            + test_df["player_last_name"].fill_null("")
        ),
        "b_name": (
            test_df["opp_first_name"].fill_null("") + " "
            + test_df["opp_last_name"].fill_null("")
        ),
        "tournament_id": test_df["tournament_id"],
        "tournament_name": test_df["tournament_name"],
        "circuit": test_df["circuit"],
        "surface": test_df["surface"],
        "round": test_df["round"],
        "best_of": test_df["best_of"].cast(pl.Int64),
        "p_match_win_a": out.distribution.p_match_win_a,
        "_row_idx": np.arange(len(out.distribution.p_match_win_a), dtype=np.int64),
        "actual_total": (
            test_df["_target_games_a"] + test_df["_target_games_b"]
        ).cast(pl.Float64),
        "actual_a_margin": (
            test_df["_target_games_a"] - test_df["_target_games_b"]
        ).cast(pl.Float64),
    })


def _offered_context(joined: pl.DataFrame, line_col: str) -> dict:
    """Per-match offered-line context: (median, n_distinct_lines, main_line).

    Computed over EVERY offered line, before any edge gate, so the emitted rows
    self-describe: `is_main_line` is a column you can filter in a spreadsheet
    rather than a rule every reader has to re-derive. Deriving it downstream only
    works when the rows are the complete offer set, which is exactly what the old
    positive-edge-only emission broke.

    Main line = offered line closest to the per-match median, ties broken by the
    line carried at the most books. Magnitudes throughout, since +/- spreads are
    paired sides of one line.
    """
    axis = pl.col(line_col).abs()
    books = (
        pl.col("n_books").max() if "n_books" in joined.columns
        else pl.col("book").n_unique()
    )
    per_line = joined.group_by(["match_uid", axis.alias("_axis")]).agg(
        books.alias("_n_books")
    )
    medians = joined.group_by("match_uid").agg(
        axis.median().alias("_median"),
        axis.n_unique().alias("_n_lines"),
    )
    picked = (
        per_line.join(medians, on="match_uid")
        .with_columns((pl.col("_axis") - pl.col("_median")).abs().alias("_dist"))
        .sort(
            ["match_uid", "_dist", "_n_books", "_axis"],
            descending=[False, False, True, False],
        )
        .group_by("match_uid")
        .agg(
            pl.col("_axis").first().alias("_main"),
            pl.col("_median").first(),
            pl.col("_n_lines").first(),
        )
    )
    return {
        r["match_uid"]: (r["_median"], r["_n_lines"], r["_main"])
        for r in picked.iter_rows(named=True)
    }


_PRICE_POINTS = ("open", "formed", "close")


def _identity_cols(
    r: dict, med: float | None, n_lines: int | None, main: float | None,
    line: float, market: str,
) -> dict:
    """Match / market identity, in the p1/p2 convention used everywhere else.

    p1 and p2 come from `event_map` (the book's ordering). They are NOT the
    projector's a/b, which is just "lower player_id first" from
    `_collapse_to_match_rows` — so this maps names across rather than renaming.
    """
    a_id = r.get("a_player_id")
    p1_is_a = r.get("p1_id") == a_id
    axis = abs(line) if market == "game_spread" else line
    return {
        "match_uid": r["match_uid"],
        "date": r["effective_match_date"],
        "p1_id": r.get("p1_id"),
        "p2_id": r.get("p2_id"),
        "p1_name": r["a_name"] if p1_is_a else r["b_name"],
        "p2_name": r["b_name"] if p1_is_a else r["a_name"],
        "tournament_id": r.get("tournament_id"),
        "tournament": r["tournament_name"],
        "circuit": r["circuit"],
        "surface": r["surface"],
        "round": r["round"],
        "best_of": int(r["best_of"]),
        "market": market,
        "line": line,
        "is_main_line": int(main is not None and axis == main),
        "median_offered_line": med,
        "n_lines_offered": n_lines,
        "line_offset": None if med is None else axis - med,
        "n_books": r.get("n_books"),
    }


def _price_cols(model_p: float, own: dict, other: dict, won: int) -> dict:
    """Odds / implied / no-vig / edge / pnl at each price point, plus CLV.

    Mirrors the classification backtest's open|formed|close triples so the two
    CSVs read the same way. CLV is `closing_implied - entry_implied`: how much
    the close beat the price you actually took. Reading closing prices alone —
    what this backtest did before — measures a strategy nobody runs.
    """
    out: dict = {}
    close_implied = None
    for point in _PRICE_POINTS:
        odds = own.get(point)
        opp_odds = other.get(point)
        implied = (1.0 / odds) if odds else None
        novig = None
        if odds and opp_odds:
            overround = (1.0 / odds) + (1.0 / opp_odds)
            novig = (1.0 / odds) / overround
        if point == "close":
            close_implied = implied
        out[f"{point}_odds"] = odds
        out[f"{point}_implied"] = implied
        out[f"{point}_p_novig"] = novig
        out[f"{point}_edge"] = None if implied is None else model_p - implied
        out[f"{point}_edge_novig"] = None if novig is None else model_p - novig
        out[f"pnl_{point}"] = (
            None if odds is None else ((odds - 1.0) if won else -1.0)
        )
    for point in ("open", "formed"):
        entry = out[f"{point}_implied"]
        out[f"clv_{point}"] = (
            None if (close_implied is None or entry is None)
            else close_implied - entry
        )
    return out


def _settle_totals(preds: pl.DataFrame, totals: pl.DataFrame, dist) -> pl.DataFrame:
    """Bet-level rows for totals across every book × line × side.

    Emits EVERY offered line, including negative-edge rows. The CSV is the
    market record, not one selection rule's output: gating at emission made the
    offered-line universe unrecoverable downstream, so "main line" could only be
    computed over the lines the model happened to like, and every summary view
    silently inherited a raw-edge gate regardless of the edge column it named.
    Filter at read time instead.
    """
    if len(totals) == 0:
        return pl.DataFrame()

    joined = totals.join(preds, on="match_uid", how="inner")
    if len(joined) == 0:
        return pl.DataFrame()

    offered = _offered_context(joined, "points")
    pmf = dist.total_games_pmf
    total_support = np.arange(pmf.shape[1], dtype=np.float64)
    rows = []
    for r in joined.iter_rows(named=True):
        line = float(r["points"])
        idx = int(r["_row_idx"])
        e_total = float((total_support * pmf[idx]).sum())  # chain's expected total
        p_over_model = float(_p_over_at(pmf[idx:idx+1], line)[0])
        p_under_model = 1.0 - p_over_model

        actual = r["actual_total"]
        med, n_lines, main = offered.get(r["match_uid"], (None, None, None))
        ident = _identity_cols(r, med, n_lines, main, line, "total_games")

        for side, model_p, won in [
            ("over", p_over_model, int(actual > line)),
            ("under", p_under_model, int(actual < line)),
        ]:
            prices = _price_cols(
                model_p,
                own={k: r[f"{side}_{k}"] for k in ("open", "formed", "close")},
                other={
                    k: r[f"{'under' if side == 'over' else 'over'}_{k}"]
                    for k in ("open", "formed", "close")
                },
                won=won,
            )
            rows.append({
                **ident,
                "side": side,
                "bet_type": side,  # "over" / "under"
                "model_p": model_p,
                # Mean gate: does the chain's expected total land on the bet side?
                "mean_covers": int(e_total > line) if side == "over"
                else int(e_total < line),
                "expected_total": e_total,
                **prices,
                "actual": actual,
                "won": won,
            })
    if not rows:
        return pl.DataFrame()
    # infer_schema_length=None scans every row. The price columns are sparse —
    # `formed_*` is null on ~69% of lines (it needs two books quoting in one
    # 15-min bucket) — so the default 100-row inference can type a column as
    # Null from an all-null head and then fail on the first real price.
    return pl.DataFrame(rows, infer_schema_length=None)


def _settle_spreads(preds: pl.DataFrame, spreads: pl.DataFrame, dist) -> pl.DataFrame:
    if len(spreads) == 0:
        return pl.DataFrame()
    joined = spreads.join(preds, on="match_uid", how="inner")
    if len(joined) == 0:
        return pl.DataFrame()

    offered = _offered_context(joined, "p1_points")
    pmf = dist.spread_pmf
    offset = dist.spread_offset
    margin_support = np.arange(pmf.shape[1], dtype=np.float64) - offset
    rows = []
    for r in joined.iter_rows(named=True):
        idx = int(r["_row_idx"])
        a_id = r["a_player_id"]
        e_a_margin = float((margin_support * pmf[idx]).sum())  # chain's expected (a-b) margin
        e_p1_margin = e_a_margin if r["p1_id"] == a_id else -e_a_margin
        e_p2_margin = -e_p1_margin
        # p1's points (book row) — the spread on p1's side.
        p1_points = float(r["p1_points"])
        p2_points = float(r["p2_points"])
        # Side wins ⟺ side margin > -points.
        # If p1 == projector A: A's margin > -p1_points → P_a_cover(line=-p1_points)
        # If p1 != A (i.e., p1 == B): B's margin > -p1_points → P(games_a - games_b < p1_points)
        if r["p1_id"] == a_id:
            p_p1_model = float(_p_a_cover_at(pmf[idx:idx+1], offset, -p1_points)[0])
        else:
            # p1 is B; bet wins iff B's margin > -p1_points, i.e. a_margin < p1_points.
            # For half-integer p1_points: P(a_margin < p1_points) = 1 - P(a_margin > p1_points).
            p_p1_model = 1.0 - float(_p_a_cover_at(pmf[idx:idx+1], offset, p1_points)[0])
        p_p2_model = 1.0 - p_p1_model

        a_margin = r["actual_a_margin"]
        # p1_won ⟺ p1's margin > -p1_points; p1's margin is a_margin if p1==A else -a_margin
        if r["p1_id"] == a_id:
            p1_won = int(a_margin > -p1_points)
            p2_won = int(-a_margin > -p2_points)
        else:
            p1_won = int(-a_margin > -p1_points)
            p2_won = int(a_margin > -p2_points)

        med, n_lines, main = offered.get(r["match_uid"], (None, None, None))
        for side, model_p, won, points, e_side_margin in [
            ("p1", p_p1_model, p1_won, p1_points, e_p1_margin),
            ("p2", p_p2_model, p2_won, p2_points, e_p2_margin),
        ]:
            other_side = "p2" if side == "p1" else "p1"
            prices = _price_cols(
                model_p,
                own={k: r[f"{side}_{k}"] for k in _PRICE_POINTS},
                other={k: r[f"{other_side}_{k}"] for k in _PRICE_POINTS},
                won=won,
            )
            rows.append({
                **_identity_cols(r, med, n_lines, main, points, "game_spread"),
                "side": side,  # "p1" / "p2" in the event_map ordering
                "bet_type": "favorite" if points < 0 else ("underdog" if points > 0 else "pickem"),
                "model_p": model_p,
                # Mean gate: does the chain's expected margin cover this line?
                "mean_covers": int(e_side_margin > -points),
                "expected_total": e_side_margin,
                **prices,
                "actual": a_margin if r["p1_id"] == a_id else -a_margin,
                "won": won,
            })
    if not rows:
        return pl.DataFrame()
    # infer_schema_length=None scans every row. The price columns are sparse —
    # `formed_*` is null on ~69% of lines (it needs two books quoting in one
    # 15-min bucket) — so the default 100-row inference can type a column as
    # Null from an all-null head and then fail on the first real price.
    return pl.DataFrame(rows, infer_schema_length=None)


def run_backtest(
    config_path: Path | str,
    *,
    retrain: bool = False,
    source: str | None = None,
    run_id: str | None = None,
) -> Path:
    """End-to-end backtest. Returns the output CSV path.

    `source` / `run_id` group this run under a parent config in the fingerprint
    dir's source.txt — a sweep passes its parent stem so `iid-rank` can show the
    variants together.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    config = IIDProjectionConfig.from_file(str(config_path))
    record_run(config, config_path, source=source, run_id=run_id)
    df, engine = _compute_features(config)

    projector = _train_or_load(
        config, config_path, df, retrain=retrain, engine=engine,
    )

    # Project the FULL settleable 2026 set once, not just the staged-book-covered
    # subset. Settlement inner-joins preds to book rows, so the wider projection
    # yields an identical bet set — and it gives the pmf dump the universe it
    # needs (oddspapi prices matches the staged books don't carry) for one chain
    # pass instead of two.
    test_df = _build_test_set(config, df, require_odds_coverage=False)
    if len(test_df) == 0:
        raise RuntimeError("No settled 2026 matches to backtest")
    logger.info("Projecting %d 2026 matches", len(test_df))
    out = _project(projector, test_df)

    fp_dir = fp_dir_for(config, config_path)
    pmf_path = write_pmf_parquet(fp_dir, _pmf_frame(test_df, out))
    logger.info("Wrote per-match total-games pmf -> %s", pmf_path)

    preds = _build_predictions_frame(test_df, out)
    em = pl.read_parquet(get_data_root() / "odds" / "event_map.parquet")

    # Collect every book's prematch series first, then price once across books —
    # open/formed/close have to be read at instants that existed on the board,
    # which per-book aggregation can't do.
    totals_snaps = []
    spread_snaps = []
    for book_code, stage_dir, event_id_col in _BOOKS:
        t = _book_totals_snapshots(book_code, stage_dir, event_id_col, em)
        s = _book_spread_snapshots(book_code, stage_dir, event_id_col, em)
        if len(t) > 0:
            totals_snaps.append(t)
        if len(s) > 0:
            spread_snaps.append(s)
    totals = _totals_prices(
        pl.concat(totals_snaps, how="diagonal_relaxed")
        if totals_snaps else pl.DataFrame()
    )
    spreads = _spread_prices(
        pl.concat(spread_snaps, how="diagonal_relaxed")
        if spread_snaps else pl.DataFrame()
    )

    bets_totals = _settle_totals(preds, totals, out.distribution)
    bets_spread = _settle_spreads(preds, spreads, out.distribution)

    parts = [b for b in (bets_totals, bets_spread) if len(b) > 0]
    if not parts:
        raise RuntimeError("Backtest produced no priced rows (no book coverage)")
    bets = pl.concat(parts, how="diagonal_relaxed").sort(
        # No `book` key: a row is a line, priced best-across-books at each
        # instant, not one row per book.
        ["date", "match_uid", "market", "line", "side"]
    )

    out_path = output_path(config, config_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    bets.write_csv(out_path)

    # Headline numbers are over the positive-no-vig-edge subset, not the whole
    # ledger — the ledger now includes every offered line, most of which the
    # model would never bet.
    gated = bets.filter(pl.col("open_edge_novig") > 0)
    if len(gated):
        logger.info(
            "Wrote %d priced rows -> %s (%d with open no-vig edge>0: "
            "hit_rate=%.3f, ROI=%.3f, avg CLV=%+.4f)",
            len(bets), out_path, len(gated),
            float(gated["won"].mean()), float(gated["pnl_open"].mean()),
            float(gated["clv_open"].drop_nulls().mean() or 0.0),
        )
    else:
        logger.info(
            "Wrote %d priced rows -> %s (none with no-vig edge>0)",
            len(bets), out_path,
        )
    return out_path


_BAND_EDGES = [0.0, 0.02, 0.04, 0.06, 0.08, 0.10]
_BAND_LABELS = ["neg", "0-2%", "2-4%", "4-6%", "6-8%", "8-10%", "10%+"]


def _band_exprs(edge_col: str) -> tuple[pl.Expr, pl.Expr]:
    """Return (label, sort_order) expressions for an edge column.

    A 'neg' band is emitted when edge < 0, which can happen for edge_novig
    (vig > raw edge). Raw 'edge' is gated > 0 upstream, so 'neg' will be empty.
    """
    n = len(_BAND_LABELS)
    label = pl.when(pl.col(edge_col) < _BAND_EDGES[0]).then(pl.lit(_BAND_LABELS[0]))
    order = pl.when(pl.col(edge_col) < _BAND_EDGES[0]).then(n - 1)
    for i, cut in enumerate(_BAND_EDGES[1:], start=1):
        label = label.when(pl.col(edge_col) < cut).then(pl.lit(_BAND_LABELS[i]))
        order = order.when(pl.col(edge_col) < cut).then(n - 1 - i)
    label = label.otherwise(pl.lit(_BAND_LABELS[-1])).alias("edge_band")
    order = order.otherwise(0).alias("_band_order")
    return label, order


def _dedupe_best_price(raw: pl.DataFrame) -> pl.DataFrame:
    """Passthrough — rows are already one per (match × market × line × side).

    Best-across-books now happens at pricing time, at a single instant per price
    point, rather than by maxing per-book rows after the fact. Kept so older
    callers and CSVs still work.
    """
    if "odds" not in raw.columns:
        return raw
    return (
        raw.sort("odds", descending=True)
        .group_by(["match_uid", "market", "line", "side"])
        .agg(pl.all().first())
    )


def _select_main_line(raw: pl.DataFrame) -> pl.DataFrame:
    """Filter rows to the main line per (match × market).

    Reads the `is_main_line` flag stamped at settle time over EVERY offered line.
    Falls back to the median of the lines present, which is only correct when
    those rows are the complete offer set — on a filtered frame it silently takes
    the median of whatever survived. That fallback exists for CSVs written before
    the flag; regenerate them and it stops being used.

    Spreads use line magnitude (|line|) since +/- are paired sides of one line.
    """
    if len(raw) == 0:
        return raw
    if "is_main_line" in raw.columns:
        return raw.filter(pl.col("is_main_line") == 1)
    line_axis = (
        pl.when(pl.col("market") == "game_spread")
        .then(pl.col("line").abs())
        .otherwise(pl.col("line"))
    )
    raw = raw.with_columns(line_axis.alias("_line_axis"))
    medians = (
        raw.group_by(["match_uid", "market"])
        .agg(pl.col("_line_axis").median().alias("_median_line"))
    )
    if "n_books" in raw.columns:
        book_depth = pl.col("n_books").max()
    elif "book" in raw.columns:
        book_depth = pl.col("book").n_unique()
    else:
        book_depth = pl.lit(1)
    coverage = (
        raw.group_by(["match_uid", "market", "_line_axis"])
        .agg(book_depth.alias("_n_books"))
    )
    pick = (
        coverage.join(medians, on=["match_uid", "market"])
        .with_columns((pl.col("_line_axis") - pl.col("_median_line")).abs().alias("_dist"))
        .sort(
            ["match_uid", "market", "_dist", "_n_books"],
            descending=[False, False, False, True],
        )
        .group_by(["match_uid", "market"])
        .agg(pl.col("_line_axis").first().alias("_main_line_axis"))
    )
    return (
        raw.join(pick, on=["match_uid", "market"])
        .filter(pl.col("_line_axis") == pl.col("_main_line_axis"))
        .drop("_line_axis", "_main_line_axis")
    )


def _pnl_col(edge_col: str) -> str:
    """The pnl column for the price point an edge column belongs to."""
    for point in _PRICE_POINTS:
        if edge_col.startswith(f"{point}_"):
            return f"pnl_{point}"
    return "pnl_open"


def _agg(df: pl.DataFrame, by: list[str], edge_col: str) -> pl.DataFrame:
    pnl = _pnl_col(edge_col)
    return df.group_by(by).agg(
        pl.len().alias("n_bets"),
        pl.col(edge_col).mean().alias("avg_edge"),
        pl.col("won").mean().alias("hit_rate"),
        pl.col(pnl).mean().alias("ROI"),
        pl.col(pnl).sum().round(2).alias("pl_units"),
        pl.col("clv_open").mean().alias("avg_clv"),
        (pl.col("clv_open") > 0).mean().alias("clv_pos_rate"),
    ).sort(by)


def _agg_band(df: pl.DataFrame, by: list[str], edge_col: str) -> pl.DataFrame:
    label, order = _band_exprs(edge_col)
    pnl = _pnl_col(edge_col)
    return (
        df.with_columns(label, order)
        .group_by(by + ["edge_band", "_band_order"]).agg(
            pl.len().alias("n_bets"),
            pl.col(edge_col).mean().alias("avg_edge"),
            pl.col("won").mean().alias("hit_rate"),
            pl.col(pnl).mean().alias("ROI"),
            pl.col(pnl).sum().round(2).alias("pl_units"),
            pl.col("clv_open").mean().alias("avg_clv"),
        )
        .sort(by + ["_band_order"])
        .drop("_band_order")
    )


def _print_view(label: str, df: pl.DataFrame, edge_col: str) -> None:
    """Print one view. Gates on `edge_col` > 0 — the column the label names.

    The CSV now carries every offered line including negative-edge rows, so the
    gate is explicit here. Previously emission applied a RAW-edge gate and every
    view inherited it, which made the NO-VIG views a raw-edge bet set relabelled.
    """
    considered = len(df)
    df = df.filter(pl.col(edge_col) > 0)
    pnl = _pnl_col(edge_col)
    print(f"\n=== {label} (edge = {edge_col}, settled at {pnl}) ===")
    if len(df) == 0:
        print(f"Bets: 0 of {considered} rows considered")
        return
    clv = df["clv_open"].drop_nulls() if "clv_open" in df.columns else None
    clv_str = (
        f"  |  CLV: {clv.mean():+.4f} ({(clv > 0).mean():.1%} positive)"
        if clv is not None and clv.len() else ""
    )
    print(f"Bets: {len(df)} of {considered} considered  |  "
          f"Hit rate: {df['won'].mean():.3f}  "
          f"|  ROI: {df[pnl].mean():.4f}  |  P&L: {df[pnl].sum():+.2f}u{clv_str}")
    with pl.Config(tbl_rows=-1, tbl_cols=-1):
        print("\nBy market:")
        print(_agg(df, ["market"], edge_col))
        print("\nBy market × bet_type:")
        print(_agg(df, ["market", "bet_type"], edge_col))
        print("\nBy market × edge_band:")
        print(_agg_band(df, ["market"], edge_col))
        print("\nBy market × bet_type × edge_band:")
        print(_agg_band(df, ["market", "bet_type"], edge_col))


def print_backtest_summary(csv_path: Path) -> None:
    raw = pl.read_csv(csv_path)

    print(f"\nBacktest output: {csv_path}")
    print(f"Priced rows (every book x line x side, pre-gate): {len(raw)}")

    # Three sections: combined plus bo3 / bo5 broken out separately. Main lines
    # are recomputed per-filter so the median-line selection reflects only the
    # matches in that section (bo5 main lines come from bo5 matches alone).
    sections = [
        ("ALL", raw),
        ("BO3", raw.filter(pl.col("best_of") == 3)),
        ("BO5", raw.filter(pl.col("best_of") == 5)),
    ]
    for tag, sub_raw in sections:
        if len(sub_raw) == 0:
            print(f"\n{'#' * 70}\n### {tag}: no bets\n{'#' * 70}")
            continue
        sub_all = _dedupe_best_price(sub_raw)
        sub_main = _dedupe_best_price(_select_main_line(sub_raw))
        print(f"\n{'#' * 70}")
        print(f"### {tag}")
        print(
            f"### Raw rows: {len(sub_raw)} | "
            f"Best-price all-line: {len(sub_all)} | "
            f"Main-line: {len(sub_main)}"
        )
        print(f"{'#' * 70}")
        # Open first: that's where entry actually happens. The close views are
        # kept for contrast — a strategy that only looks good at the close is
        # measuring a price it never got.
        _print_view(f"{tag} — MAIN LINE — OPEN NO-VIG", sub_main, "open_edge_novig")
        _print_view(f"{tag} — MAIN LINE — OPEN RAW", sub_main, "open_edge")
        _print_view(f"{tag} — MAIN LINE — CLOSE NO-VIG", sub_main, "close_edge_novig")
        _print_view(f"{tag} — ALL LINES — OPEN NO-VIG", sub_all, "open_edge_novig")
        _print_view(f"{tag} — ALL LINES — OPEN RAW", sub_all, "open_edge")

        # Mean-gated: only bets whose expected total/margin lands on the bet side
        # (the chain's point estimate agrees with the side, not just a pmf tail).
        if "mean_covers" in sub_raw.columns:
            mean_raw = sub_raw.filter(pl.col("mean_covers") == 1)
            if len(mean_raw):
                mean_all = _dedupe_best_price(mean_raw)
                mean_main = _dedupe_best_price(_select_main_line(mean_raw))
                _print_view(f"{tag} — MEAN-GATED MAIN LINE — OPEN NO-VIG", mean_main, "open_edge_novig")
                _print_view(f"{tag} — MEAN-GATED ALL LINES — OPEN NO-VIG", mean_all, "open_edge_novig")
