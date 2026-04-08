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

from mvp.projection.iid.chain import MatchDistribution
from mvp.projection.iid.metrics import compute_iid_metrics
from mvp.projection.iid.projector import ProjectionOutput


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
    ) -> IIDProjectionDiagnosticResults:
        """Compute segmented metrics from accumulated fold predictions.

        Each fold prediction dict must have keys: `df`, `out`, `y_won`,
        `y_games_a`, `y_games_b`. The `df` carries the segment columns.
        """
        if not predictions:
            return IIDProjectionDiagnosticResults(segments={})

        combined_df = pl.concat([p["df"] for p in predictions])
        combined_y_won = np.concatenate([p["y_won"] for p in predictions])
        combined_y_a = np.concatenate([p["y_games_a"] for p in predictions])
        combined_y_b = np.concatenate([p["y_games_b"] for p in predictions])

        # Concatenate the per-fold projection outputs into a single combined ProjectionOutput
        combined_out = _concat_outputs([p["out"] for p in predictions])

        segments: dict[str, dict[str, dict[str, float]]] = {}
        for seg_col in ("circuit", "surface", "best_of"):
            if seg_col not in combined_df.columns:
                continue
            seg_metrics: dict[str, dict[str, float]] = {}
            for value in combined_df[seg_col].drop_nulls().unique().sort().to_list():
                mask = (combined_df[seg_col] == value).fill_null(False).to_numpy()
                if not mask.any():
                    continue
                sub_out = _slice_output(combined_out, mask)
                seg_metrics[str(value)] = compute_iid_metrics(
                    sub_out,
                    combined_y_won[mask],
                    combined_y_a[mask],
                    combined_y_b[mask],
                    total_lines=total_lines,
                    spread_lines=spread_lines,
                )
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
