"""Sweep validation.test_months on a model config to compare retraining cadences.

Runs the same model under each cadence, pools per-fold OOF predictions into one
parquet per cadence, then prints comparison tables at every granularity in the
swept list (e.g. sweeping 12/6/3/1 prints yearly, semiannual, quarterly, and
monthly breakdowns).
"""

from __future__ import annotations

import logging
import tempfile
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from mvp.common.base_job import get_data_root
from mvp.model.metrics import compute_metrics
from mvp.model.runner import ExperimentRunner

logger = logging.getLogger(__name__)

# (metric_key, column_header) pairs in display order.
METRIC_COLS: list[tuple[str, str]] = [
    ("accuracy", "Accuracy"),
    ("roc_auc", "AUC"),
    ("log_loss", "Log Loss"),
    ("brier_score", "Brier"),
    ("calibration_error", "Cal Err"),
    ("error_rate_80plus", "Err80"),
]


def _sweep_output_dir(config_name: str) -> Path:
    out = get_data_root() / "model_sweeps" / config_name
    out.mkdir(parents=True, exist_ok=True)
    return out


def _extract_oof_df(all_predictions: list[dict[str, Any]]) -> pl.DataFrame:
    parts = []
    for i, fold in enumerate(all_predictions):
        base = fold["df"].select(["match_uid", "effective_match_date"])
        part = base.with_columns(
            pl.lit(i).alias("fold_idx"),
            pl.Series("y_true", fold["y_true"]).cast(pl.Int8),
            pl.Series("y_prob", fold["y_prob"]).cast(pl.Float64),
        )
        parts.append(part)
    return pl.concat(parts, how="vertical")


def _run_one_cadence(
    base_config: dict[str, Any],
    config_path: Path,
    test_months: int,
) -> pl.DataFrame:
    cfg = deepcopy(base_config)
    cfg["validation"] = dict(cfg.get("validation", {}))
    cfg["validation"]["test_months"] = test_months

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        yaml.dump(cfg, f, default_flow_style=False)
        tmp_path = Path(f.name)
    try:
        runner = ExperimentRunner(
            config_path=tmp_path,
            run_name=f"sweep_{config_path.stem}_tm{test_months}",
            log_to_mlflow=False,
        )
        results = runner.run()
        return _extract_oof_df(results["all_predictions"])
    finally:
        tmp_path.unlink(missing_ok=True)


def _period_start_expr(granularity_months: int) -> pl.Expr:
    col = pl.col("effective_match_date")
    y = col.dt.year()
    m = col.dt.month()
    if granularity_months >= 12:
        return pl.date(y, 1, 1)
    if granularity_months == 6:
        return pl.date(y, pl.when(m <= 6).then(1).otherwise(7), 1)
    if granularity_months == 3:
        return pl.date(y, ((m - 1) // 3) * 3 + 1, 1)
    if granularity_months == 1:
        return pl.date(y, m, 1)
    raise ValueError(f"Unsupported granularity: {granularity_months}")


def _period_label(start: date, granularity_months: int) -> str:
    if granularity_months >= 12:
        return f"{start.year}"
    if granularity_months == 6:
        return f"{start.year}-H{1 if start.month <= 6 else 2}"
    if granularity_months == 3:
        return f"{start.year}-Q{(start.month - 1) // 3 + 1}"
    if granularity_months == 1:
        return f"{start.year}-{start.month:02d}"
    return start.isoformat()


def _safe_compute_metrics(y_true, y_prob) -> dict[str, float]:
    """compute_metrics that returns NaN for metrics that can't be computed."""
    try:
        return compute_metrics(y_true, y_prob)
    except Exception:
        # roc_auc fails on single-class samples; fall back metric-by-metric.
        import numpy as np
        from sklearn.metrics import (
            accuracy_score,
            brier_score_loss,
            log_loss,
            roc_auc_score,
        )

        from mvp.model.metrics import (
            compute_calibration_error,
            compute_error_rate_80plus,
            compute_signed_calibration,
        )

        y_prob_clipped = np.clip(y_prob, 1e-15, 1 - 1e-15)
        y_pred = (y_prob >= 0.5).astype(int)
        out: dict[str, float] = {}
        for key, fn in {
            "accuracy": lambda: float(accuracy_score(y_true, y_pred)),
            "log_loss": lambda: float(log_loss(y_true, y_prob_clipped)),
            "brier_score": lambda: float(brier_score_loss(y_true, y_prob)),
            "roc_auc": lambda: float(roc_auc_score(y_true, y_prob)),
            "calibration_error": lambda: compute_calibration_error(y_true, y_prob),
            "signed_calibration": lambda: compute_signed_calibration(y_true, y_prob),
            "error_rate_80plus": lambda: compute_error_rate_80plus(y_true, y_prob),
        }.items():
            try:
                out[key] = fn()
            except Exception:
                out[key] = float("nan")
        return out


def _format_value(key: str, v: float) -> str:
    if v != v:  # NaN
        return "-"
    if key == "accuracy":
        return f"{v * 100:.1f}%"
    if key == "roc_auc":
        return f"{v:.3f}"
    return f"{v:.4f}"


def _format_row(metrics: dict[str, float]) -> list[str]:
    return [_format_value(key, metrics.get(key, float("nan"))) for key, _ in METRIC_COLS]


def _print_table(
    title: str,
    labels: list[str],
    n_samples: list[int],
    rows: list[list[str]],
) -> None:
    print()
    print(title)
    header = ["", "N"] + [name for _, name in METRIC_COLS]
    n_strs = [f"{n:,}" for n in n_samples]
    widths = [len(h) for h in header]
    widths[0] = max(widths[0], max(len(lbl) for lbl in labels))
    widths[1] = max(widths[1], max(len(n) for n in n_strs))
    for row in rows:
        for i, cell in enumerate(row):
            widths[i + 2] = max(widths[i + 2], len(cell))
    fmt = "  ".join(f"{{:>{w}}}" for w in widths)
    print(fmt.format(*header))
    for label, n, row in zip(labels, n_strs, rows):
        print(fmt.format(label, n, *row))


def run_cadence_sweep(
    config_path: Path,
    test_months_values: list[int],
) -> None:
    with open(config_path) as f:
        base_config = yaml.safe_load(f)

    config_name = config_path.stem
    out_dir = _sweep_output_dir(config_name)

    oof_by_cadence: dict[int, pl.DataFrame] = {}
    for tm in test_months_values:
        logger.info("Running cadence test_months=%d", tm)
        oof = _run_one_cadence(base_config, config_path, tm)
        oof_path = out_dir / f"cadence_{tm}m.parquet"
        oof.write_parquet(oof_path)
        logger.info("Wrote %d OOF rows to %s", len(oof), oof_path)
        oof_by_cadence[tm] = oof

    cadence_labels = [f"test_months={tm}" for tm in test_months_values]

    # Pooled summary
    pooled_rows = []
    pooled_n = []
    for tm in test_months_values:
        df = oof_by_cadence[tm]
        m = _safe_compute_metrics(
            df["y_true"].to_numpy().astype(int),
            df["y_prob"].to_numpy(),
        )
        pooled_rows.append(_format_row(m))
        pooled_n.append(len(df))
    _print_table(
        f"=== Pooled summary: {config_name} ({len(test_months_values)} cadences) ===",
        cadence_labels,
        pooled_n,
        pooled_rows,
    )

    # Breakdown tables, one per (granularity, period)
    for gran in test_months_values:
        # Bucket each cadence's predictions into periods of this granularity
        bucketed: dict[int, pl.DataFrame] = {}
        periods: set[date] = set()
        for tm in test_months_values:
            df = oof_by_cadence[tm].with_columns(
                _period_start_expr(gran).alias("_period")
            )
            bucketed[tm] = df
            periods.update(df["_period"].to_list())

        print()
        print(f"=== {gran}-month windows ===")
        for period_start in sorted(periods):
            rows = []
            row_n = []
            for tm in test_months_values:
                slice_df = bucketed[tm].filter(pl.col("_period") == period_start)
                row_n.append(len(slice_df))
                if len(slice_df) == 0:
                    rows.append(["-"] * len(METRIC_COLS))
                else:
                    m = _safe_compute_metrics(
                        slice_df["y_true"].to_numpy().astype(int),
                        slice_df["y_prob"].to_numpy(),
                    )
                    rows.append(_format_row(m))
            _print_table(
                f"--- {_period_label(period_start, gran)} ---",
                cadence_labels,
                row_n,
                rows,
            )
