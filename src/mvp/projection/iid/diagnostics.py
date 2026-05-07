"""Lightweight per-segment diagnostics for IID projector outputs.

For v1, this provides per-circuit and per-surface metric breakdowns. The
heavier residual / quantile diagnostics in `mvp.projection.diagnostics` are
not duplicated here — the IID projector's main job is producing distributions
and the runner already logs distributional metrics globally.
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl

from mvp.projection.iid.chain import (
    SET_SCORE_LABELS,
    MatchDistribution,
    set_score_distribution,
)
from mvp.projection.iid.metrics import (
    compute_hold_diagnostics,
    compute_iid_metrics,
    compute_serve_diagnostics,
    compute_set_score_diagnostics,
    compute_tiebreak_diagnostics,
)
from mvp.projection.iid.projector import ProjectionOutput
from mvp.projection.iid.serve_model import SERVE_PROB_MAX, SERVE_PROB_MIN


@dataclass
class IIDProjectionDiagnosticResults:
    """Container for per-segment IID projection metrics."""

    segments: dict[str, dict[str, dict[str, float]]]

    @property
    def metrics(self) -> dict[str, float]:
        flat: dict[str, float] = {}
        for seg_type, segs in self.segments.items():
            for seg_value, m in segs.items():
                for key, val in m.items():
                    flat[f"segment_{seg_type}_{seg_value}_{key}"] = val
        return flat


class IIDProjectionDiagnostics:
    """Compute per-segment IID metrics across circuit/surface/best_of."""

    def compute_all(
        self,
        predictions: list[dict[str, Any]],
        *,
        total_lines: list[float] | None = None,
        spread_lines: list[float] | None = None,
        clip_min: float = SERVE_PROB_MIN,
        clip_max: float = SERVE_PROB_MAX,
    ) -> IIDProjectionDiagnosticResults:
        """Compute segmented metrics from accumulated fold predictions.

        Each fold prediction dict must have keys: `df`, `out`, `y_won`,
        `y_games_a`, `y_games_b`. The `df` carries the segment columns plus
        the inputs needed by the chain-layer diagnostic functions.
        """
        if not predictions:
            return IIDProjectionDiagnosticResults(segments={})

        combined_df = pl.concat([p["df"] for p in predictions])
        combined_y_won = np.concatenate([p["y_won"] for p in predictions])
        combined_y_a = np.concatenate([p["y_games_a"] for p in predictions])
        combined_y_b = np.concatenate([p["y_games_b"] for p in predictions])

        # Concatenate the per-fold projection outputs into a single combined ProjectionOutput
        combined_out = _concat_outputs([p["out"] for p in predictions])

        # Tag each match with quartile buckets for chain-predicted P(tight set)
        # and P(blowout set). Slicing on the chain's own predicted mass (rather
        # than realized outcomes) is leakage-free, so the per-bucket tight/
        # blowout biases measure calibration at each prediction level.
        combined_df = _add_pred_shape_buckets(combined_df, combined_out)

        segments: dict[str, dict[str, dict[str, float]]] = {}
        for seg_col in (
            "circuit",
            "surface",
            "best_of",
            "pred_tight_bucket",
            "pred_blowout_bucket",
        ):
            if seg_col not in combined_df.columns:
                continue
            seg_metrics: dict[str, dict[str, float]] = {}
            for value in combined_df[seg_col].drop_nulls().unique().sort().to_list():
                mask = (combined_df[seg_col] == value).fill_null(False).to_numpy()
                if not mask.any():
                    continue
                sub_out = _slice_output(combined_out, mask)
                sub_df = combined_df.filter(pl.Series(mask))
                m = compute_iid_metrics(
                    sub_out,
                    combined_y_won[mask],
                    combined_y_a[mask],
                    combined_y_b[mask],
                    total_lines=total_lines,
                    spread_lines=spread_lines,
                )
                # Chain-layer diagnostics — same set the runner computes at
                # the top level, but per-segment so the user can see whether
                # serve/hold/set-score/tiebreak biases differ across bo3 vs
                # bo5 (or per circuit/surface).
                m.update(compute_serve_diagnostics(
                    sub_out, sub_df, clip_min=clip_min, clip_max=clip_max,
                ))
                m.update(compute_hold_diagnostics(sub_out, sub_df))
                m.update(compute_set_score_diagnostics(sub_out, sub_df))
                m.update(compute_tiebreak_diagnostics(sub_out, sub_df))
                m["segment_n"] = float(int(mask.sum()))
                seg_metrics[str(value)] = m
            segments[seg_col] = seg_metrics

        return IIDProjectionDiagnosticResults(segments=segments)


def _concat_outputs(outputs: list[ProjectionOutput]) -> ProjectionOutput:
    """Concatenate a list of ProjectionOutputs into one along the row axis."""
    if not outputs:
        raise ValueError("Cannot concat empty list of ProjectionOutputs")

    first = outputs[0]
    spread_offset = first.distribution.spread_offset

    p_match_win_a = np.concatenate([o.distribution.p_match_win_a for o in outputs])
    total_pmf = np.concatenate([o.distribution.total_games_pmf for o in outputs], axis=0)
    spread_pmf = np.concatenate([o.distribution.spread_pmf for o in outputs], axis=0)
    expected_total = np.concatenate([o.distribution.expected_total_games for o in outputs])
    expected_spread = np.concatenate([o.distribution.expected_spread for o in outputs])

    # Set outcome probs: union of keys, fill missing folds with zeros
    all_keys = set()
    for o in outputs:
        all_keys.update(o.distribution.set_outcome_probs.keys())
    set_outcomes: dict[tuple[int, int], np.ndarray] = {}
    for key in all_keys:
        per_fold = []
        for o in outputs:
            n_o = len(o.distribution.p_match_win_a)
            if key in o.distribution.set_outcome_probs:
                per_fold.append(o.distribution.set_outcome_probs[key])
            else:
                per_fold.append(np.zeros(n_o, dtype=np.float64))
        set_outcomes[key] = np.concatenate(per_fold)

    combined_dist = MatchDistribution(
        p_match_win_a=p_match_win_a,
        set_outcome_probs=set_outcomes,
        total_games_pmf=total_pmf,
        spread_pmf=spread_pmf,
        spread_offset=spread_offset,
        expected_total_games=expected_total,
        expected_spread=expected_spread,
    )

    return ProjectionOutput(
        distribution=combined_dist,
        match_uid=np.concatenate([o.match_uid for o in outputs]),
        best_of=np.concatenate([o.best_of for o in outputs]),
        p_a_serve_win=np.concatenate([o.p_a_serve_win for o in outputs]),
        p_b_serve_win=np.concatenate([o.p_b_serve_win for o in outputs]),
        h_a=np.concatenate([o.h_a for o in outputs]),
        h_b=np.concatenate([o.h_b for o in outputs]),
        t_ab=np.concatenate([o.t_ab for o in outputs]),
    )


_TIGHT_LABELS = frozenset({"7-5", "5-7", "7-6", "6-7"})
_BLOWOUT_LABELS = frozenset({"6-0", "0-6", "6-1", "1-6"})


def _add_pred_shape_buckets(
    df: pl.DataFrame, out: ProjectionOutput
) -> pl.DataFrame:
    """Add Q1..Q4 buckets for chain-predicted P(tight set) and P(blowout set).

    Tight = {7-5, 5-7, 7-6, 6-7}; blowout = {6-0, 0-6, 6-1, 1-6} — same
    indices `compute_set_score_diagnostics` aggregates. Buckets are
    equal-count quartiles via ordinal rank, so labels are MLflow-safe.
    """
    pmf = set_score_distribution(out.h_a, out.h_b, out.t_ab)
    tight_idx = [
        i for i, lab in enumerate(SET_SCORE_LABELS) if lab in _TIGHT_LABELS
    ]
    blowout_idx = [
        i for i, lab in enumerate(SET_SCORE_LABELS) if lab in _BLOWOUT_LABELS
    ]
    pred_tight = pmf[:, tight_idx].sum(axis=1)
    pred_blowout = pmf[:, blowout_idx].sum(axis=1)

    df = df.with_columns(
        pl.Series("_pred_tight_p", pred_tight),
        pl.Series("_pred_blowout_p", pred_blowout),
    )
    return df.with_columns(
        _quartile_bucket("_pred_tight_p").alias("pred_tight_bucket"),
        _quartile_bucket("_pred_blowout_p").alias("pred_blowout_bucket"),
    )


def _quartile_bucket(col_name: str) -> pl.Expr:
    """Equal-count Q1..Q4 bucket via ordinal rank. Null inputs → null."""
    rank_expr = pl.col(col_name).rank(method="ordinal")
    n_valid = pl.col(col_name).is_not_null().sum()
    idx = ((rank_expr - 1) * 4 // n_valid + 1).clip(1, 4)
    return (
        pl.when(pl.col(col_name).is_null())
        .then(None)
        .when(idx == 1)
        .then(pl.lit("Q1"))
        .when(idx == 2)
        .then(pl.lit("Q2"))
        .when(idx == 3)
        .then(pl.lit("Q3"))
        .otherwise(pl.lit("Q4"))
    )


def _slice_output(out: ProjectionOutput, mask: np.ndarray) -> ProjectionOutput:
    """Boolean-mask a ProjectionOutput along the match axis."""
    sub_dist = MatchDistribution(
        p_match_win_a=out.distribution.p_match_win_a[mask],
        set_outcome_probs={
            k: v[mask] for k, v in out.distribution.set_outcome_probs.items()
        },
        total_games_pmf=out.distribution.total_games_pmf[mask],
        spread_pmf=out.distribution.spread_pmf[mask],
        spread_offset=out.distribution.spread_offset,
        expected_total_games=out.distribution.expected_total_games[mask],
        expected_spread=out.distribution.expected_spread[mask],
    )
    return ProjectionOutput(
        distribution=sub_dist,
        match_uid=out.match_uid[mask],
        best_of=out.best_of[mask],
        p_a_serve_win=out.p_a_serve_win[mask],
        p_b_serve_win=out.p_b_serve_win[mask],
        h_a=out.h_a[mask],
        h_b=out.h_b[mask],
        t_ab=out.t_ab[mask],
    )
