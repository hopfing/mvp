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
from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.engine import FeatureEngine
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.projection.iid.config import IIDProjectionConfig, ServeModelConfig
from mvp.projection.iid.projector import ProjectionOutput, TennisProjector
from mvp.projection.iid.serve_model import (
    IdentityServeModel,
    MatchupServeModel,
    ScoreStateChainServeModel,
    ServeWinProbEstimator,
)

logger = logging.getLogger(__name__)

# (book_code_in_event_map, stage_dir_name, event_id_col_in_stage)
_BOOKS: list[tuple[str, str, str]] = [
    ("czr", "caesars", "czr_event_id"),
    ("mgm", "betmgm", "mgm_event_id"),
    ("dk", "draftkings", "dk_event_id"),
    ("br", "betrivers", "br_event_id"),
]

ARTIFACT_ROOT = Path("B:/projections/iid")
BACKTEST_ROOT = ARTIFACT_ROOT / "backtests"


def artifact_path(config_path: Path) -> Path:
    return ARTIFACT_ROOT / f"{config_path.stem}.joblib"


def output_path(config_path: Path) -> Path:
    return BACKTEST_ROOT / f"{config_path.stem}.csv"


def _build_serve_model(cfg: ServeModelConfig) -> ServeWinProbEstimator:
    if cfg.type == "identity":
        return IdentityServeModel(
            window=cfg.window, clip_min=cfg.clip_min, clip_max=cfg.clip_max,
        )
    if cfg.type == "matchup":
        if not cfg.feature_columns:
            raise ValueError("serve_model.feature_columns required for type=matchup")
        return MatchupServeModel(
            feature_columns=cfg.feature_columns,
            match_level_columns=cfg.match_level_columns,
            regressor_type=cfg.regressor.type,
            regressor_params=dict(cfg.regressor.params),
            clip_min=cfg.clip_min,
            clip_max=cfg.clip_max,
        )
    if cfg.type == "score_state":
        if not cfg.match_level_features and not cfg.point_level_features:
            raise ValueError(
                "serve_model.match_level_features and/or point_level_features "
                "required for type=score_state"
            )
        return ScoreStateChainServeModel(
            model_type=cfg.model_type,
            match_level_features=cfg.match_level_features,
            point_level_features=cfg.point_level_features,
            params=dict(cfg.params),
        )
    raise ValueError(f"Unknown serve model type: {cfg.type}")


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


def _compute_features(config: IIDProjectionConfig) -> pl.DataFrame:
    matches_path = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
    cache_dir = get_local_data_root() / "features" / "cache"
    engine = FeatureEngine(matches_path=matches_path, cache_dir=cache_dir)

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

    return engine.compute(all_specs, extra_columns=runner_columns)


def _train_projector(config: IIDProjectionConfig, df: pl.DataFrame) -> TennisProjector:
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
    serve_model = _build_serve_model(config.serve_model)
    projector = TennisProjector(serve_model)
    projector.fit(train_df)
    return projector


def _save_artifact(
    projector: TennisProjector, config_path: Path, n_train: int,
) -> None:
    path = artifact_path(config_path)
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


def _load_artifact(config_path: Path) -> TennisProjector:
    path = artifact_path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"No IID artifact at {path}")
    artifact = joblib.load(path)
    return TennisProjector(serve_model=artifact["serve_model"])


def _train_or_load(
    config: IIDProjectionConfig, config_path: Path, df: pl.DataFrame,
    *, retrain: bool,
) -> TennisProjector:
    if retrain or not artifact_path(config_path).exists():
        projector = _train_projector(config, df)
        _save_artifact(projector, config_path, n_train=0)
        return projector
    logger.info("Loading existing IID artifact for %s", config_path.stem)
    return _load_artifact(config_path)


def _build_test_set(config: IIDProjectionConfig, df: pl.DataFrame) -> pl.DataFrame:
    """Filter to settled 2026 matches with event_map coverage; collapse."""
    test = df
    if config.data.filters:
        test = apply_filters(test, config.data.filters)
    test = _resolve_targets(test)
    test = test.filter(pl.col("effective_match_date") >= pl.date(2026, 1, 1))
    test = test.filter(pl.col("best_of").is_in([3, 5]))
    test = _collapse_to_match_rows(test)

    em = pl.read_parquet(get_data_root() / "odds" / "event_map.parquet")
    covered = em.select("match_uid").unique()
    test = test.join(covered, on="match_uid", how="inner")
    return test


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


def _book_totals_pairs(
    book_code: str, stage_dir: str, event_id_col: str, em: pl.DataFrame,
) -> pl.DataFrame:
    """Per-(match_uid, points) total_games closing pair: over_odds + under_odds."""
    path = get_data_root() / "stage" / stage_dir / "total_games.parquet"
    if not path.exists():
        return pl.DataFrame()
    raw = pl.read_parquet(path)
    closing = _closing_snapshots(raw, event_id_col)
    if len(closing) == 0:
        return pl.DataFrame()

    em_book = em.filter(pl.col("book") == book_code).select(
        pl.col("event_id"), "match_uid", "p1_id", "p2_id",
    )
    joined = closing.join(em_book, left_on=event_id_col, right_on="event_id", how="inner")
    if len(joined) == 0:
        return pl.DataFrame()

    pivot = (
        joined.select("match_uid", "points", "side", "odds")
        .pivot(values="odds", index=["match_uid", "points"], on="side", aggregate_function="first")
    )
    cols = pivot.columns
    if "over" not in cols or "under" not in cols:
        return pl.DataFrame()
    pivot = pivot.rename({"over": "over_odds", "under": "under_odds"})
    return (
        pivot.filter(pl.col("over_odds").is_not_null() & pl.col("under_odds").is_not_null())
        .with_columns(pl.lit(book_code).alias("book"))
    )


def _book_spread_pairs(
    book_code: str, stage_dir: str, event_id_col: str, em: pl.DataFrame,
) -> pl.DataFrame:
    """Per (match_uid, abs_points) game_spread closing pair, with each side's
    odds and resolved player_id."""
    path = get_data_root() / "stage" / stage_dir / "game_spread.parquet"
    if not path.exists():
        return pl.DataFrame()
    raw = pl.read_parquet(path)
    closing = _closing_snapshots(raw, event_id_col)
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

    p1 = (
        joined.filter(pl.col("side_player_id") == pl.col("p1_id"))
        .select(
            "match_uid", "abs_points", "p1_id", "p2_id",
            pl.col("points").alias("p1_points"),
            pl.col("odds").alias("p1_odds"),
        )
    )
    p2 = (
        joined.filter(pl.col("side_player_id") == pl.col("p2_id"))
        .select(
            "match_uid", "abs_points",
            pl.col("points").alias("p2_points"),
            pl.col("odds").alias("p2_odds"),
        )
    )
    paired = p1.join(p2, on=["match_uid", "abs_points"], how="inner")
    return paired.with_columns(pl.lit(book_code).alias("book"))


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
        "p_match_win_a": out.distribution.p_match_win_a,
        "_row_idx": np.arange(len(out.distribution.p_match_win_a), dtype=np.int64),
        "actual_total": (
            test_df["_target_games_a"] + test_df["_target_games_b"]
        ).cast(pl.Float64),
        "actual_a_margin": (
            test_df["_target_games_a"] - test_df["_target_games_b"]
        ).cast(pl.Float64),
    })


def _settle_totals(preds: pl.DataFrame, totals: pl.DataFrame, dist) -> pl.DataFrame:
    """Build bet-level rows for totals across all books × lines, edge > 0 only."""
    if len(totals) == 0:
        return pl.DataFrame()

    joined = totals.join(preds, on="match_uid", how="inner")
    if len(joined) == 0:
        return pl.DataFrame()

    pmf = dist.total_games_pmf
    rows = []
    for r in joined.iter_rows(named=True):
        line = float(r["points"])
        idx = int(r["_row_idx"])
        p_over_model = float(_p_over_at(pmf[idx:idx+1], line)[0])
        p_under_model = 1.0 - p_over_model

        actual = r["actual_total"]
        for side, model_p, odds, won in [
            ("over", p_over_model, r["over_odds"], int(actual > line)),
            ("under", p_under_model, r["under_odds"], int(actual < line)),
        ]:
            book_p = 1.0 / odds
            edge = model_p - book_p
            if edge <= 0:
                continue
            profit = (odds - 1.0) if won else -1.0
            rows.append({
                "match_uid": r["match_uid"],
                "date": r["effective_match_date"],
                "a_name": r["a_name"],
                "b_name": r["b_name"],
                "tournament": r["tournament_name"],
                "circuit": r["circuit"],
                "surface": r["surface"],
                "round": r["round"],
                "market": "total_games",
                "line": line,
                "side": side,
                "bet_type": side,  # "over" / "under"
                "book": r["book"],
                "odds": odds,
                "book_p_implied": book_p,
                "model_p": model_p,
                "edge": edge,
                "actual": actual,
                "won": won,
                "profit": profit,
            })
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def _settle_spreads(preds: pl.DataFrame, spreads: pl.DataFrame, dist) -> pl.DataFrame:
    if len(spreads) == 0:
        return pl.DataFrame()
    joined = spreads.join(preds, on="match_uid", how="inner")
    if len(joined) == 0:
        return pl.DataFrame()

    pmf = dist.spread_pmf
    offset = dist.spread_offset
    rows = []
    for r in joined.iter_rows(named=True):
        idx = int(r["_row_idx"])
        a_id = r["a_player_id"]
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

        for side, model_p, odds, won, points in [
            ("p1", p_p1_model, r["p1_odds"], p1_won, p1_points),
            ("p2", p_p2_model, r["p2_odds"], p2_won, p2_points),
        ]:
            book_p = 1.0 / odds
            edge = model_p - book_p
            if edge <= 0:
                continue
            profit = (odds - 1.0) if won else -1.0
            rows.append({
                "match_uid": r["match_uid"],
                "date": r["effective_match_date"],
                "a_name": r["a_name"],
                "b_name": r["b_name"],
                "tournament": r["tournament_name"],
                "circuit": r["circuit"],
                "surface": r["surface"],
                "round": r["round"],
                "market": "game_spread",
                "line": points,
                "side": side,  # "p1" or "p2" relative to the event_map ordering
                "bet_type": "favorite" if points < 0 else ("underdog" if points > 0 else "pickem"),
                "book": r["book"],
                "odds": odds,
                "book_p_implied": book_p,
                "model_p": model_p,
                "edge": edge,
                "actual": a_margin if r["p1_id"] == a_id else -a_margin,
                "won": won,
                "profit": profit,
            })
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def run_backtest(config_path: Path | str, *, retrain: bool = False) -> Path:
    """End-to-end backtest. Returns the output CSV path."""
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    config = IIDProjectionConfig.from_file(str(config_path))
    df = _compute_features(config)

    projector = _train_or_load(config, config_path, df, retrain=retrain)

    test_df = _build_test_set(config, df)
    if len(test_df) == 0:
        raise RuntimeError("No 2026 settled matches with event_map coverage to backtest")
    logger.info("Projecting %d 2026 matches", len(test_df))
    out = _project(projector, test_df)

    preds = _build_predictions_frame(test_df, out)
    em = pl.read_parquet(get_data_root() / "odds" / "event_map.parquet")

    totals_frames = []
    spread_frames = []
    for book_code, stage_dir, event_id_col in _BOOKS:
        t = _book_totals_pairs(book_code, stage_dir, event_id_col, em)
        s = _book_spread_pairs(book_code, stage_dir, event_id_col, em)
        if len(t) > 0:
            totals_frames.append(t)
        if len(s) > 0:
            spread_frames.append(s)
    totals = (
        pl.concat(totals_frames, how="diagonal_relaxed")
        if totals_frames else pl.DataFrame()
    )
    spreads = (
        pl.concat(spread_frames, how="diagonal_relaxed")
        if spread_frames else pl.DataFrame()
    )

    bets_totals = _settle_totals(preds, totals, out.distribution)
    bets_spread = _settle_spreads(preds, spreads, out.distribution)

    parts = [b for b in (bets_totals, bets_spread) if len(b) > 0]
    if not parts:
        raise RuntimeError("Backtest produced no positive-edge bets")
    bets = pl.concat(parts, how="diagonal_relaxed").sort(
        ["date", "match_uid", "market", "line", "side", "book"]
    )

    out_path = output_path(config_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    bets.write_csv(out_path)

    n_won = int(bets["won"].sum())
    roi = float(bets["profit"].sum() / max(len(bets), 1))
    logger.info(
        "Wrote %d bet rows -> %s (hit_rate=%.3f, ROI=%.3f)",
        len(bets), out_path, n_won / max(len(bets), 1), roi,
    )
    return out_path


def print_backtest_summary(csv_path: Path) -> None:
    raw = pl.read_csv(csv_path)
    # Realistic view: one bet per (match × market × line × side) at best price.
    bets = (
        raw.sort("odds", descending=True)
        .group_by(["match_uid", "market", "line", "side"])
        .agg(pl.all().first())
    )
    print(f"\nBacktest output: {csv_path}")
    print(f"Raw bet rows (all books): {len(raw)}")
    print(f"Deduped to best-price per (match × market × line × side): {len(bets)}")
    print(f"Hit rate:   {bets['won'].mean():.3f}")
    print(f"ROI:        {bets['profit'].mean():.4f}")

    def _agg(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
        return df.group_by(by).agg(
            pl.len().alias("n_bets"),
            pl.col("edge").mean().alias("avg_edge"),
            pl.col("won").mean().alias("hit_rate"),
            pl.col("profit").mean().alias("ROI"),
        ).sort(by)

    # Edge bands ordered low→high; the _band_order column drives sort and is dropped before print.
    bets = bets.with_columns(
        pl.when(pl.col("edge") < 0.02).then(pl.lit("0-2%"))
        .when(pl.col("edge") < 0.04).then(pl.lit("2-4%"))
        .when(pl.col("edge") < 0.06).then(pl.lit("4-6%"))
        .when(pl.col("edge") < 0.08).then(pl.lit("6-8%"))
        .when(pl.col("edge") < 0.10).then(pl.lit("8-10%"))
        .otherwise(pl.lit("10%+"))
        .alias("edge_band"),
        pl.when(pl.col("edge") < 0.02).then(5)
        .when(pl.col("edge") < 0.04).then(4)
        .when(pl.col("edge") < 0.06).then(3)
        .when(pl.col("edge") < 0.08).then(2)
        .when(pl.col("edge") < 0.10).then(1)
        .otherwise(0)
        .alias("_band_order"),
    )

    def _agg_band(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
        return (
            df.group_by(by + ["edge_band", "_band_order"]).agg(
                pl.len().alias("n_bets"),
                pl.col("edge").mean().alias("avg_edge"),
                pl.col("won").mean().alias("hit_rate"),
                pl.col("profit").mean().alias("ROI"),
            )
            .sort(by + ["_band_order"])
            .drop("_band_order")
        )

    with pl.Config(tbl_rows=-1, tbl_cols=-1):
        print("\nBy market:")
        print(_agg(bets, ["market"]))
        print("\nBy market × bet_type:")
        print(_agg(bets, ["market", "bet_type"]))
        print("\nBy market × edge_band:")
        print(_agg_band(bets, ["market"]))
        print("\nBy market × bet_type × edge_band:")
        print(_agg_band(bets, ["market", "bet_type"]))
