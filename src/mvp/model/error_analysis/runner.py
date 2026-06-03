"""Orchestrator for the feature-error analysis pipeline.

Reads a fingerprint dir, runs whichever analyses are possible given the
available input files, writes outputs to `<fingerprint_dir>/error_analysis/`.

Inputs (whichever exist):
  - fold_predictions.parquet: full training history (no odds). If present, used
    as the primary source for calibration / SHAP / drift (larger sample).
  - backtest.csv: forward-test window (has odds). If present, used for loss
    attribution + ROI overlay; also for calibration / SHAP / drift if
    fold_predictions doesn't exist.
"""
from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from mvp.model.error_analysis.analyses import (
    naive_baseline_comparison,
    per_feature_calibration,
    per_feature_calibration_by_segment,
    temporal_drift,
)
from mvp.model.error_analysis.feature_join import join_predictions_with_features

logger = logging.getLogger(__name__)


def run_error_analysis(
    fingerprint_dir: Path | str,
    *,
    config_path: Path | str | None = None,
    matches_path: Path | str | None = None,
    skip_shap: bool = False,
    cache_dir: Path | str | None = None,
) -> Path:
    """Run the feature-error analysis pipeline.

    Both fold_predictions.parquet and backtest.csv are required. If either is
    missing this runs training and/or backtest to populate them, which
    requires `config_path` to be a YAML config.

    Args:
        fingerprint_dir: Path to `B:/model_evaluations/<fp>/`.
        config_path: Path to the source YAML config. Required when auto-
            generating missing inputs.
        matches_path: Override matches.parquet path.
        skip_shap: If True, skip the SHAP-on-errors meta-model.
        cache_dir: FeatureEngine cache dir.

    Returns:
        Path to the output dir containing CSVs.
    """
    fp_dir = Path(fingerprint_dir)
    out_dir = fp_dir / "error_analysis"
    out_dir.mkdir(exist_ok=True)

    logger.info("=== Feature-error analysis: %s ===", fp_dir.name)
    logger.info("Output: %s", out_dir)

    _ensure_inputs_present(
        fp_dir=fp_dir,
        config_path=Path(config_path) if config_path else None,
        matches_path=matches_path,
    )

    fold_joined, backtest_joined, feature_cols = join_predictions_with_features(
        fingerprint_dir=fp_dir,
        matches_path=matches_path,
        source="auto",
        cache_dir=cache_dir,
    )
    logger.info(
        "Analyzing %d canonical feature columns (from config spec, "
        "excluding engine intermediates / structural cols)",
        len(feature_cols),
    )

    # Prefer fold_predictions for upstream analyses (larger sample). Fall back
    # to backtest if fold isn't available.
    if fold_joined is not None:
        upstream_df = fold_joined
        prob_col, target_col = "y_prob", "y_test"
        upstream_source = "fold_predictions.parquet"
    elif backtest_joined is not None:
        upstream_df = backtest_joined
        prob_col, target_col = "model_prob", "won"
        upstream_source = "backtest.csv"
    else:
        raise RuntimeError("Neither data source loaded — should not reach here.")

    logger.info(
        "Upstream analyses (calibration / naive baseline / drift / SHAP) "
        "running on %s (%d rows)",
        upstream_source, upstream_df.height,
    )

    cal = per_feature_calibration(
        upstream_df, prob_col=prob_col, target_col=target_col, feature_cols=feature_cols,
    )
    cal.write_csv(out_dir / "per_feature_calibration.csv")
    logger.info("3A per_feature_calibration: %d rows -> per_feature_calibration.csv", cal.height)

    # Per-circuit and per-round breakdowns to disambiguate features whose
    # bucket effect could be a circuit or round proxy.
    if "circuit" in upstream_df.columns:
        cal_circ = per_feature_calibration_by_segment(
            upstream_df, "circuit",
            prob_col=prob_col, target_col=target_col, feature_cols=feature_cols,
        )
        cal_circ.write_csv(out_dir / "per_feature_calibration_by_circuit.csv")
        logger.info("3A per_feature_calibration_by_circuit: %d rows", cal_circ.height)
    if "round" in upstream_df.columns:
        cal_round = per_feature_calibration_by_segment(
            upstream_df, "round",
            prob_col=prob_col, target_col=target_col, feature_cols=feature_cols,
        )
        cal_round.write_csv(out_dir / "per_feature_calibration_by_round.csv")
        logger.info("3A per_feature_calibration_by_round: %d rows", cal_round.height)

    naive = naive_baseline_comparison(
        upstream_df, prob_col=prob_col, target_col=target_col, feature_cols=feature_cols,
    )
    naive.write_csv(out_dir / "naive_baseline_comparison.csv")
    logger.info("3D naive_baseline_comparison: %d rows -> naive_baseline_comparison.csv", naive.height)

    drift = temporal_drift(
        upstream_df, prob_col=prob_col, target_col=target_col, feature_cols=feature_cols,
    )
    drift.write_csv(out_dir / "temporal_drift.csv")
    logger.info("3E temporal_drift: %d rows -> temporal_drift.csv", drift.height)

    if not skip_shap:
        from mvp.model.error_analysis.shap_errors import shap_on_errors

        logger.info("3C SHAP-on-errors: fitting meta-model on signed residual...")
        shap_df = shap_on_errors(
            upstream_df, prob_col=prob_col, target_col=target_col, feature_cols=feature_cols,
        )
        shap_df.write_csv(out_dir / "shap_on_errors.csv")
        logger.info("  %d feature rankings -> shap_on_errors.csv", shap_df.height)
    else:
        logger.info("3C SHAP-on-errors: skipped (--skip-shap)")

    if backtest_joined is not None:
        from mvp.model.error_analysis.tier2 import (
            loss_attribution,
            loss_attribution_by_segment,
        )

        logger.info("3F loss attribution running on backtest.csv (%d rows)",
                    backtest_joined.height)
        loss_attr = loss_attribution(
            backtest_joined, prob_col="model_prob", target_col="won", feature_cols=feature_cols,
        )
        loss_attr.write_csv(out_dir / "loss_attribution.csv")
        logger.info("  %d rows -> loss_attribution.csv", loss_attr.height)

        if "circuit" in backtest_joined.columns:
            la_circ = loss_attribution_by_segment(
                backtest_joined, "circuit",
                prob_col="model_prob", target_col="won", feature_cols=feature_cols,
            )
            la_circ.write_csv(out_dir / "loss_attribution_by_circuit.csv")
            logger.info("  by-circuit: %d rows", la_circ.height)
        if "round" in backtest_joined.columns:
            la_round = loss_attribution_by_segment(
                backtest_joined, "round",
                prob_col="model_prob", target_col="won", feature_cols=feature_cols,
            )
            la_round.write_csv(out_dir / "loss_attribution_by_round.csv")
            logger.info("  by-round: %d rows", la_round.height)
    else:
        logger.warning(
            "3F loss attribution skipped: backtest.csv missing in %s. "
            "Run `mvp backtest <config> --retrain` to populate it.",
            fp_dir,
        )

    if fold_joined is None:
        logger.warning(
            "fold_predictions.parquet missing in %s. Upstream analyses ran on "
            "backtest.csv only (smaller sample). For the full ~4-year history "
            "view, run `mvp model <config>` to populate fold_predictions.parquet.",
            fp_dir,
        )

    rollup = _build_feature_rollup(out_dir, has_backtest=backtest_joined is not None, skip_shap=skip_shap)
    if rollup is not None:
        rollup.write_csv(out_dir / "feature_rollup.csv")
        logger.info("feature_rollup.csv: %d features", rollup.height)

    logger.info("=== Done. Outputs in %s ===", out_dir)
    return out_dir


def _ensure_inputs_present(
    *,
    fp_dir: Path,
    config_path: Path | None,
    matches_path: Path | str | None,
) -> None:
    """Ensure both fold_predictions.parquet and backtest.csv exist in fp_dir.

    Runs `mvp model` (for fold_predictions) and/or `mvp backtest --retrain`
    (for backtest) to populate whatever's missing. Requires a YAML config_path.
    """
    fold_path = fp_dir / "fold_predictions.parquet"
    backtest_path = fp_dir / "backtest.csv"

    fold_missing = not fold_path.exists()
    backtest_missing = not backtest_path.exists()

    if not fold_missing and not backtest_missing:
        return

    if config_path is None:
        raise FileNotFoundError(
            f"Missing inputs in {fp_dir} and no source config provided. "
            f"Pass a YAML config path (e.g. `mvp model-errors "
            f"models/.../config.yaml`), not just a fingerprint hash or model name."
        )
    if not config_path.exists():
        raise FileNotFoundError(
            f"Source config not found at {config_path}; can't generate "
            f"missing inputs."
        )

    if fold_missing:
        logger.info(
            "fold_predictions.parquet missing — running `mvp model %s` ...",
            config_path.name,
        )
        from mvp.model.runner import ExperimentRunner

        runner = ExperimentRunner(
            config_path=config_path,
            matches_path=Path(matches_path) if matches_path else None,
        )
        runner.run()
        if not fold_path.exists():
            raise RuntimeError(
                f"Training completed but fold_predictions.parquet still "
                f"missing at {fold_path}. Training may have failed silently."
            )

    if backtest_missing:
        logger.info(
            "backtest.csv missing — running `mvp backtest %s --retrain` ...",
            config_path.name,
        )
        from mvp.model.backtest import run_backtest

        run_backtest(config_path, retrain=True)
        if not backtest_path.exists():
            raise RuntimeError(
                f"Backtest completed but backtest.csv still missing at "
                f"{backtest_path}. Backtest may have failed silently."
            )


def _build_feature_rollup(
    out_dir: Path, *, has_backtest: bool, skip_shap: bool
) -> pl.DataFrame | None:
    """One row per feature, joined columns from each analysis output."""
    cal = pl.read_csv(out_dir / "per_feature_calibration.csv")
    cal_summary = (
        cal.with_columns(pl.col("gap").abs().alias("abs_gap"))
        .group_by("feature")
        .agg(
            pl.col("abs_gap").max().alias("max_abs_calibration_gap"),
            pl.col("bucket")
            .filter(pl.col("abs_gap") == pl.col("abs_gap").max())
            .first()
            .alias("max_gap_bucket"),
            pl.col("n").sum().alias("total_n"),
        )
    )

    rollup = cal_summary
    if not skip_shap and (out_dir / "shap_on_errors.csv").exists():
        shap_df = pl.read_csv(out_dir / "shap_on_errors.csv")
        rollup = rollup.join(
            shap_df.select(["feature", "mean_abs_shap", "mean_signed_shap", "rank"]),
            on="feature", how="left",
        )

    if has_backtest and (out_dir / "loss_attribution.csv").exists():
        loss = pl.read_csv(out_dir / "loss_attribution.csv")
        loss_summary = loss.group_by("feature").agg(
            pl.col("log_loss_sum").sum().alias("total_log_loss"),
            pl.col("pnl_open_sum").sum().alias("total_pnl_open"),
            pl.col("pnl_close_sum").sum().alias("total_pnl_close"),
        )
        rollup = rollup.join(loss_summary, on="feature", how="left")

    sort_col = "mean_abs_shap" if "mean_abs_shap" in rollup.columns else "max_abs_calibration_gap"
    return rollup.sort(sort_col, descending=True, nulls_last=True)
