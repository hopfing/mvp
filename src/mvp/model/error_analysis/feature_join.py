"""Step 1 — load prediction data and join with engine-computed features.

Tier 1 (default): reads `fold_predictions.parquet` from a fingerprint dir.
Per-fold OOF predictions across the full date_sliding window.

Tier 2: reads `backtest.csv` from the same dir. Smaller forward-test sample
but includes odds/units for loss-attribution analyses.

Both tiers use the same config.yaml (sibling of the data file) to recover
the feature spec the model saw. FeatureEngine.compute() is PIT-correct by
design (rolling windows use closed="left"; cumulative features use
.shift(1).over(player_id, order_by=effective_match_date)), so feature
vectors recovered now match the values the model trained/predicted on as
long as matches.parquet still contains the underlying rows.
"""
from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

import yaml

from mvp.common.base_job import get_data_root
from mvp.model.engine import FeatureEngine, get_feature_columns, make_fs_engine

logger = logging.getLogger(__name__)

DEFAULT_MATCHES_PATH = Path("B:/aggregate/atptour/matches.parquet")


def join_predictions_with_features(
    fingerprint_dir: Path | str,
    matches_path: Path | str | None = None,
    *,
    source: str = "auto",
    cache_dir: Path | str | None = None,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None, list[str]]:
    """Load predictions from a fingerprint dir and join with features.

    Tries both data sources:
      - fold_predictions.parquet: ~200k rows over the full training history.
        No odds.
      - backtest.csv: ~14k rows over the forward-test window. Has odds + pnl.

    Args:
        fingerprint_dir: Path to a model_evaluations/<fp>/ directory.
        matches_path: Path to matches.parquet (default: standard prod path).
        source: "auto" (default; loads whatever exists), "fold" (only fold
            predictions), or "backtest" (only backtest).
        cache_dir: Engine cache directory.

    Returns:
        (fold_df, backtest_df, feature_cols) — fold_df / backtest_df may be
        None if that source's input file doesn't exist (at least one will be
        non-None). feature_cols is the canonical list of feature column names
        the MODEL actually trained on (derived from the config spec via
        get_feature_columns), to be passed to downstream analyses so they
        ignore engine-side dependency intermediates and structural cols.

    Raises:
        FileNotFoundError: If config.yaml missing OR neither input source
            file exists.
        ValueError: If the config has no feature spec.
    """
    fp_dir = Path(fingerprint_dir)
    config_path = fp_dir / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at {config_path}")

    fold_path = fp_dir / "fold_predictions.parquet"
    backtest_path = fp_dir / "backtest.csv"

    want_fold = source in ("auto", "fold") and fold_path.exists()
    want_backtest = source in ("auto", "backtest") and backtest_path.exists()

    if not want_fold and not want_backtest:
        if source == "fold":
            raise FileNotFoundError(
                f"fold_predictions.parquet not found at {fold_path}. "
                f"This file is produced by `mvp model <config>` runs after "
                f"the Step 0 wiring landed; older fingerprint dirs need a "
                f"re-train to populate it."
            )
        if source == "backtest":
            raise FileNotFoundError(
                f"backtest.csv not found at {backtest_path}. "
                f"Run `mvp backtest <config> --retrain` to produce one."
            )
        raise FileNotFoundError(
            f"Neither fold_predictions.parquet nor backtest.csv exists in "
            f"{fp_dir}. To populate the dir, run either:\n"
            f"  - `mvp model <config>` to produce fold_predictions.parquet "
            f"(larger sample, no odds)\n"
            f"  - `mvp backtest <config> --retrain` to produce backtest.csv "
            f"(smaller sample, has odds)\n"
            f"Running both gives the fullest picture."
        )

    fold_predictions = (
        _load_fold_predictions(fold_path) if want_fold else None
    )
    backtest_predictions = (
        _load_backtest(backtest_path) if want_backtest else None
    )

    if fold_predictions is not None:
        logger.info("Loaded %d fold-prediction rows", fold_predictions.height)
    else:
        logger.info("fold_predictions.parquet: not found (skipping Tier 1)")
    if backtest_predictions is not None:
        logger.info("Loaded %d backtest rows", backtest_predictions.height)
    else:
        logger.info("backtest.csv: not found (skipping loss attribution)")

    # Read feature spec directly from YAML as a plain dict. The fingerprint dir
    # contains a canonicalized config snapshot (flattened for hashing) that
    # ExperimentConfig.from_file can't deserialize directly, but the
    # features.include list is preserved verbatim.
    with open(config_path) as f:
        cfg_raw = yaml.safe_load(f) or {}
    features_block = cfg_raw.get("features") or {}
    feature_specs = list(features_block.get("include") or [])
    if not feature_specs:
        raise ValueError(
            f"Config at {config_path} has no features.include spec; "
            f"can't recover feature vectors."
        )
    logger.info("Recovering %d feature specs from config", len(feature_specs))

    matches_path_resolved = (
        Path(matches_path) if matches_path is not None else DEFAULT_MATCHES_PATH
    )
    if not matches_path_resolved.exists():
        raise FileNotFoundError(f"matches.parquet not found at {matches_path_resolved}")

    cache_dir_resolved = (
        Path(cache_dir) if cache_dir is not None
        else get_data_root() / "features"
    )
    engine = make_fs_engine(
        matches_path=matches_path_resolved,
        cache_dir=cache_dir_resolved,
    )

    extra_columns = [
        "won", "circuit", "surface", "round",
        "match_uid", "player_id", "opp_id",
        "effective_match_date",
    ]
    feature_df = engine.compute(feature_specs, extra_columns=extra_columns)
    logger.info(
        "Engine produced %d rows x %d cols (%d feature cols)",
        feature_df.height, len(feature_df.columns), len(feature_specs),
    )

    fold_joined = (
        _join_with_features(fold_predictions, feature_df, len(feature_specs))
        if fold_predictions is not None else None
    )
    backtest_joined = (
        _join_with_features(backtest_predictions, feature_df, len(feature_specs))
        if backtest_predictions is not None else None
    )

    # Canonical feature column list — exactly what the model trained on,
    # derived from the spec via the same helper the runner uses (handles
    # mirror expansion etc.). Downstream analyses filter to this list so
    # engine-side dependency intermediates and structural cols aren't
    # treated as features.
    feature_cols = get_feature_columns(feature_specs)
    # Intersect with whatever's actually present in the joined frame.
    representative = fold_joined if fold_joined is not None else backtest_joined
    feature_cols_present = [c for c in feature_cols if c in representative.columns]
    missing = set(feature_cols) - set(feature_cols_present)
    if missing:
        logger.warning(
            "Canonical feature columns not found in joined df (%d missing): %s",
            len(missing), sorted(missing)[:10],
        )
    return fold_joined, backtest_joined, feature_cols_present


def _join_with_features(
    predictions: pl.DataFrame, feature_df: pl.DataFrame, n_features: int,
) -> pl.DataFrame:
    """Join a predictions df with the engine feature_df on (match_uid, player_id)."""
    joined = predictions.join(
        feature_df,
        on=["match_uid", "player_id"],
        how="left",
        suffix="_engine",
    )
    if joined.height != predictions.height:
        raise RuntimeError(
            f"Join row-count mismatch: predictions had {predictions.height}, "
            f"joined has {joined.height}. Possible duplicates in feature_df."
        )
    drop_cols = [c for c in joined.columns if c.endswith("_engine")]
    if drop_cols:
        joined = joined.drop(drop_cols)
    logger.info(
        "Joined %d rows with %d feature cols", joined.height, n_features,
    )
    return joined


def _load_fold_predictions(path: Path) -> pl.DataFrame:
    """Load fold_predictions.parquet. Schema documented in runner.py:1188."""
    df = pl.read_parquet(path)
    required = {"match_uid", "player_id", "y_test", "y_prob", "fold_idx"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"fold_predictions.parquet missing required columns: {missing}"
        )
    return df


def _load_backtest(path: Path) -> pl.DataFrame:
    """Load backtest.csv from a fingerprint dir.

    Filters to is_pick rows (the bet-side prediction the model produced) so
    each row corresponds to a single prediction.
    """
    df = pl.read_csv(path, infer_schema_length=10000).with_columns(
        pl.col("effective_match_date").str.to_datetime(strict=False),
    )
    # Filter to the model's pick side, since each match has 2 rows (one per
    # side) and the model only predicts one of them as the pick
    if "is_pick" in df.columns:
        df = df.filter(pl.col("is_pick"))
    # Backtest schema uses "side" not "player_id"; the data identifier is
    # already (match_uid, side). Need a player_id-aligned join — backtest
    # rows already have player_id alongside side, so use that.
    required = {"match_uid", "player_id", "model_prob", "won"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"backtest.csv missing required columns: {missing}"
        )
    return df
