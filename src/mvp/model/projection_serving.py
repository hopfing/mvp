"""Match-win probability from a projection model for PENDING matches.

`player_prior_logit_<projection stem>` is null on every live row while present
for all but a month of training rows, so a stage declaring it is trained with a
feature production never supplies. The cause is structural: the column is built
from evaluation artifacts (`fold_match_win.parquet`, `total_games_pmf.parquet`),
and both come from a pipeline that calls `resolve_targets`, which drops matches
without completed set scores. A pending match can never appear in either, so
regenerating them cannot close the gap. `_fill_prior_logit` covers only the
OFFSET prior; any other declared prior has no live source at all.

The model scores a pending match fine — it needs features, not targets. Two
deviations from the evaluation path, and only two, are what make that possible:

1. **No cutoff engine.** `compute_features` builds its engine with
   `make_fs_engine`, whose `cutoff_date` is the first of the current month
   (`engine.py:28-38`), so every pending match is dropped at load — correct for
   evaluation, which wants a cache key stable across a month, and precisely
   wrong here, because those are the only rows serving cares about. This module
   builds no engine at all: it takes the caller's frame, and the serving
   caller's engine is already cutoff-free (`predictor.py:1670`; the one at
   `:693` is the train path, also cutoff-free). See `serving_requirements` for
   why it borrows rather than builds.
2. **No `resolve_targets`.** That step is what excludes pending matches.

Everything else is reused rather than restated — the spec set, the filters, the
collapse, the trained projector, and the calibration. A serving path that fits
its own model or builds its own frame would put a different quantity in the
column than training put there, which is the failure this module exists to fix.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)


def serving_requirements(stem: str) -> tuple[list[str], list[str]]:
    """(specs, extra columns) a caller must compute for `pending_match_win_logits`.

    Exists so the projection rides the tick's EXISTING engine pass instead of
    opening a second one. A separate `FeatureEngine` here would hold a second
    full-corpus frame alive alongside `predict()`'s -- additive peak, on a 16 GB
    serving box whose `check_memory` guard is a no-op (it is Windows-only,
    `engine.py:92-93`), so the failure mode there is the OOM killer rather than
    a raised limit. It would also ignore whatever `matches_path` / `cache_dir`
    the caller was constructed with.

    Mirrors `projection_run.compute_features`' spec set exactly.
    """
    from mvp.model.config import get_filter_feature_specs
    from mvp.model.features.prior import resolve_prior
    from mvp.projection.iid.config import IIDProjectionConfig
    from mvp.projection.iid.projection_run import _RUNNER_COLUMNS

    config = IIDProjectionConfig.from_file(str(resolve_prior(stem).config_path))
    specs = list(config.features.include)
    extra = (config.features.compute_only or []) + get_filter_feature_specs(
        config.data.filters
    )
    specs += [s for s in extra if s not in specs]

    columns = list(_RUNNER_COLUMNS)
    if config.data.filters:
        for col in config.data.filters:
            if col not in columns:
                columns.append(col)
    return specs, columns


def _pending_frame(config: Any, df: pl.DataFrame, uids: list[str]) -> pl.DataFrame:
    """The projection's frame for pending matches: its filters and collapse,
    without `resolve_targets` — deviation (2) in the module docstring."""
    from mvp.model.config import apply_filters
    from mvp.projection.iid.projection_run import _collapse_to_match_rows

    out = df.filter(pl.col("match_uid").is_in(uids))
    if config.data.filters:
        out = apply_filters(out, config.data.filters)
    out = out.filter(pl.col("best_of").is_in([3, 5]))
    return _collapse_to_match_rows(out)


@lru_cache(maxsize=8)
def _forward_calibrator(stem: str) -> Any:
    """Global Platt on every walk-forward OOF row.

    `_projection_forward_rows` (`features/prior.py`) defines how the column is
    built for rows AFTER the projection's train end — which is what a pending
    match is: one fit through train_end, calibrated by a global Platt fit on all
    walk-forward OOF rows. Serving the chain's raw output instead would put an
    uncalibrated quantity in a calibrated column.
    """
    from mvp.model.calibration import PlattCalibrator
    from mvp.model.features.prior import resolve_prior

    path = resolve_prior(stem).fold_match_win
    if not path.exists():
        raise FileNotFoundError(
            f"projection {stem}: no fold_match_win at {path}; the served prior "
            f"cannot be calibrated the way the trained column was."
        )
    oof = pl.read_parquet(path)
    cal = PlattCalibrator()
    cal.fit(
        oof["p_match_win_a"].to_numpy(), oof["won_a"].to_numpy().astype(np.int64)
    )
    return cal


def _projector_path(stem: str) -> Path:
    """The projector beside the artifacts the TRAINED column reads.

    Not `artifact_path(config, config_path)`, which recomputes the fingerprint.
    `resolve_prior` can land on a different directory via `_tagged_fallback`
    when the true fingerprint dir is absent, and the pmf and fold artifacts come
    from THAT dir. Deriving the projector from the same `eval_dir` makes this
    module's "produced TOGETHER" invariant true by construction instead of by
    assumption -- and avoids raising on the lead path, which has no degrade.
    """
    from mvp.model.features.prior import resolve_prior
    from mvp.projection.iid.artifacts import SERVE_MODEL_JOBLIB

    return resolve_prior(stem).eval_dir / SERVE_MODEL_JOBLIB


def check_served_projector(stem: str, path: Path | None = None) -> Path:
    """Verify a usable trained projector exists for `stem`. Never fits.

    `path` overrides where to look: promotion checks the EVALUATION dir's
    projector before copying it, while the default resolves through
    `resolve_prior` to the promoted copy production reads.

    Deliberately NOT `train_or_load`. That fits and overwrites
    `serve_model.joblib` when the staleness check fails, WITHOUT regenerating
    `total_games_pmf.parquet` beside it -- and the pmf is where the trained
    column's forward rows come from. Fitting only half the pair would serve a
    different fit than training conditioned on, which is the defect this whole
    module exists to remove, relocated one directory down.

    It is reachable rather than theoretical: the fingerprint is content-
    canonical and `description` is not in the canonical dict, so editing only a
    description keeps the same fp dir (same pmf) while failing the artifact's
    raw config-text check. `PriorSource.salt()` keys on the mtimes of
    `fold_match_win` and the pmf, neither of which moves, so nothing downstream
    would surface the divergence either.

    The two artifacts are produced together by the projection's own command, so
    that is what this points at when either is absent or stale.
    """
    from mvp.model.features.prior import resolve_prior
    from mvp.projection.iid.config import IIDProjectionConfig
    from mvp.projection.iid.projection_run import _load_artifact

    config_path = Path(resolve_prior(stem).config_path)
    config = IIDProjectionConfig.from_file(str(config_path))
    path = path or _projector_path(stem)
    if _load_artifact(config, config_path, path) is None:
        raise FileNotFoundError(
            f"projection {stem}: no usable projector at {path} (missing, or "
            f"trained from different config text). It and the pmf the trained "
            f"prior reads are produced together by promotion: "
            f"poetry run py -m mvp train"
        )
    return path


def pending_match_win_logits(
    stem: str, uids: list[str], df: pl.DataFrame,
) -> dict[tuple[str, str], float]:
    """(match_uid, player_id) -> calibrated match-win logit for pending matches.

    `df` is the CALLER's already-computed frame, widened by
    `serving_requirements` -- see there for why this does not build its own.

    Load-only: `_load_artifact` returns None for a missing or config-stale
    artifact and this raises, because a fit inside the 15-minute tick is not an
    option and silently returning nothing would leave the column NaN — the exact
    failure being fixed.

    Both orientations are returned. `_both_orientations` gives B `1 - p`, and
    `logit(1 - p) == -logit(p)`, so the negation below is that mirror exactly.
    """
    from mvp.model.features.prior import resolve_prior
    from mvp.projection.iid.config import IIDProjectionConfig
    from mvp.projection.iid.projection_run import _load_artifact

    # Before anything is loaded. A caller with nothing to serve must not pay a
    # joblib read per fold, and must not be able to raise on an artifact it does
    # not need -- `predict()` and `predict_voters` have no degrade path, so that
    # raise would sink a whole backtest fold. Guarded here rather than at each
    # call site, because there are two and they have already diverged once.
    if not uids:
        return {}

    config_path = Path(resolve_prior(stem).config_path)
    config = IIDProjectionConfig.from_file(str(config_path))
    path = _projector_path(stem)
    projector = _load_artifact(config, config_path, path)
    if projector is None:
        raise FileNotFoundError(
            f"projection {stem}: no trained projector at "
            f"{path} (missing, or trained from "
            f"different config text). It and the pmf the trained prior reads "
            f"are produced together by promotion: poetry run py -m mvp train"
        )

    pending = _pending_frame(config, df, list(uids))
    if len(pending) == 0:
        return {}

    out = projector.project(pending)
    raw = np.asarray(out.distribution.p_match_win_a, dtype=np.float64)
    p = np.clip(
        np.asarray(_forward_calibrator(stem).transform(raw), dtype=np.float64),
        1e-6, 1 - 1e-6,
    )
    lg = np.log(p / (1.0 - p))
    fill: dict[tuple[str, str], float] = {}
    for uid, a, b, v in zip(
        pending["match_uid"].to_list(), pending["player_id"].to_list(),
        pending["opp_id"].to_list(), lg, strict=True,
    ):
        fill[(uid, a)] = float(v)
        fill[(uid, b)] = float(-v)
    logger.info(
        "projection %s: served %d pending match(es)", stem, len(pending),
    )
    return fill
