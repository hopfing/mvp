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
import math
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

    Mirrors `projection_run.compute_features`' spec set exactly, unioned
    recursively with every projection `stem` starts its arms from (an arm
    offset or a `chain_arm` spec): those projectors run on the same pending
    frame (`fill_pending_arms`), so the one engine pass carries their inputs.
    """
    from mvp.model.config import get_filter_feature_specs
    from mvp.projection.iid.projection_run import _RUNNER_COLUMNS, arm_source_stems

    specs: list[str] = []
    columns = list(_RUNNER_COLUMNS)
    seen: set[str] = set()

    def visit(s: str) -> None:
        if s in seen:
            return
        seen.add(s)
        config = _config_of(s)
        own = list(config.features.include)
        own += (config.features.compute_only or []) + get_filter_feature_specs(
            config.data.filters
        )
        specs.extend(x for x in own if x not in specs)
        for col in config.data.filters or {}:
            if col not in columns:
                columns.append(col)
        for source in arm_source_stems(config):
            visit(source)

    visit(stem)
    return specs, columns


def _config_of(stem: str) -> Any:
    """The projection config `stem` resolves to."""
    from mvp.model.features.prior import resolve_prior
    from mvp.projection.iid.config import IIDProjectionConfig

    return IIDProjectionConfig.from_file(str(resolve_prior(stem).config_path))


def _load_source(stem: str) -> tuple[Any, Any]:
    """(config, trained projector) for `stem`, load-only: a fit inside the
    15-minute tick is not an option, so a missing or stale projector raises."""
    from mvp.model.features.prior import resolve_prior
    from mvp.projection.iid.projection_run import _load_artifact

    config_path = Path(resolve_prior(stem).config_path)
    config = _config_of(stem)
    path = _projector_path(stem)
    projector = _load_artifact(config, config_path, path)
    if projector is None:
        raise FileNotFoundError(
            f"projection {stem}: no trained projector at "
            f"{path} (missing, or trained from "
            f"different config text). It and the pmf the trained prior reads "
            f"are produced together by promotion: poetry run py -m mvp train"
        )
    return config, projector


def _pending_frame(
    config: Any, df: pl.DataFrame, uids: list[str],
    parity_columns: list[str] | None = None, stem: str = "",
) -> pl.DataFrame:
    """The projection's frame for pending matches: its filters and collapse,
    without `resolve_targets` — deviation (2) in the module docstring.

    The live/train parity check runs BEFORE the config's `not_null` filters:
    those filters remove exactly the rows an all-null input would produce,
    so a check placed after them sees a clean (or empty) frame and the
    09-02 defect class passes silently. Order here: the other filters, the
    check on `parity_columns`, then `not_null`, with a raise if `not_null`
    empties a frame large enough to mean something.
    """
    from mvp.model.config import apply_filters
    from mvp.projection.iid.column_checks import ALL_NULL_MIN_ROWS, check_required_columns
    from mvp.projection.iid.projection_run import _collapse_to_match_rows

    out = df.filter(pl.col("match_uid").is_in(uids))
    filters = dict(config.data.filters or {})
    not_null = {k: v for k, v in filters.items() if v == "not_null"}
    others = {k: v for k, v in filters.items() if v != "not_null"}
    if others:
        out = apply_filters(out, others)
    out = out.filter(pl.col("best_of").is_in([3, 5]))
    out = _collapse_to_match_rows(out)
    if parity_columns:
        check_required_columns(
            out, list(parity_columns), where=f"projection {stem} (pending)",
        )
    if not_null:
        before = out.height
        out = apply_filters(out, not_null)
        if before >= ALL_NULL_MIN_ROWS and out.height == 0:
            raise ValueError(
                f"projection {stem} (pending): the not_null filters on "
                f"{sorted(not_null)} dropped all {before} rows — the frame "
                "carries none of the input the estimator was trained on"
            )
        if out.height < before:
            logger.info(
                "projection %s (pending): not_null filters dropped %d of %d rows",
                stem, before - out.height, before,
            )
    return out


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


def _pending_projection(
    stem: str, uids: list[str], df: pl.DataFrame,
) -> tuple[pl.DataFrame, Any] | None:
    """(pending frame, ProjectionOutput) for `stem` on the pending `uids`, or
    None when there is nothing to project.

    `df` is subset to `uids` first, then the arm inputs `stem` reads from other
    projections are filled (`fill_pending_arms`), then the projection's own
    filters run on the filled rows, the classification order (fill, then
    filters): a `not_null` filter on an arm offset column must see the filled
    value. One projection serves the match-win prior and the chain-shape
    columns alike, so the fill sites call this once per stem.
    """
    # Before anything is loaded. A caller with nothing to serve must not pay a
    # joblib read per fold, and must not be able to raise on an artifact it does
    # not need -- `predict()` and `predict_voters` have no degrade path, so that
    # raise would sink a whole backtest fold.
    if not uids:
        return None
    subset = fill_pending_arms(
        stem, list(uids), df.filter(pl.col("match_uid").is_in(list(uids))),
    )
    config, projector = _load_source(stem)
    # Live/train parity on the estimator's declared inputs runs inside
    # `_pending_frame`, ahead of the config's not_null filters: a column that
    # was present in training and is null on every pending row is the 09-02
    # defect class, and it raises there (stage degrades, run report carries
    # it) rather than projecting a frame the model never saw.
    pending = _pending_frame(
        config, subset, list(uids),
        parity_columns=list(projector.serve_model.parity_columns), stem=stem,
    )
    if len(pending) == 0:
        return None
    return pending, projector.project(pending)


def match_win_logits_from(
    stem: str, projection: tuple[pl.DataFrame, Any] | None,
) -> dict[tuple[str, str], float]:
    """(match_uid, player_id) -> calibrated match-win logit, both orientations.

    `_both_orientations` gives B `1 - p`, and `logit(1 - p) == -logit(p)`, so
    the negation below is that mirror exactly.
    """
    if projection is None:
        return {}
    pending, out = projection
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


def pending_match_win_logits(
    stem: str, uids: list[str], df: pl.DataFrame,
) -> dict[tuple[str, str], float]:
    """(match_uid, player_id) -> calibrated match-win logit for pending matches.

    `df` is the CALLER's already-computed frame, widened by
    `serving_requirements` -- see there for why this does not build its own.
    Load-only (`_load_source`).
    """
    return match_win_logits_from(stem, _pending_projection(stem, uids, df))


def chain_shape_values_from(
    projection: tuple[pl.DataFrame, Any] | None,
) -> dict[str, dict[tuple[str, str], float]]:
    """Shape column (bare `player_<scalar>`) -> (match_uid, player_id) ->
    value, both orientations: the pending row's player reads the A-oriented
    scalars, the mirror row reads symmetric ones as-is and antisymmetric ones
    negated -- the `chain_shape` transform's own rule. Valid because
    `_pending_frame` collapses to the lower `player_id`, the orientation the
    stores are written in. Uncalibrated, as the trained column is."""
    from mvp.common.chain_shape import SHAPE_ANTISYMMETRIC, shape_scalars

    if projection is None:
        return {}
    pending, out = projection
    uids = pending["match_uid"].to_list()
    a_ids = pending["player_id"].to_list()
    b_ids = pending["opp_id"].to_list()
    values: dict[str, dict[tuple[str, str], float]] = {}
    for name, arr in shape_scalars(out).items():
        sign = -1.0 if name in SHAPE_ANTISYMMETRIC else 1.0
        col: dict[tuple[str, str], float] = {}
        for uid, a, b, v in zip(uids, a_ids, b_ids, arr, strict=True):
            col[(uid, a)] = float(v)
            col[(uid, b)] = sign * float(v)
        values[f"player_{name}"] = col
    return values


def pending_chain_shape_values(
    stem: str, uids: list[str], df: pl.DataFrame,
) -> dict[str, dict[tuple[str, str], float]]:
    """`chain_shape_values_from` on a fresh projection of `stem`."""
    return chain_shape_values_from(_pending_projection(stem, uids, df))


# ---------------------------------------------------------------------------
# chain_arm: a source projection's per-arm outputs for pending matches
# ---------------------------------------------------------------------------

from mvp.model.prior_naming import ARM_VALUES as _ARM_BARE  # noqa: E402


def pending_arm_values(
    stem: str, uids: list[str], df: pl.DataFrame,
) -> dict[tuple[str, str], tuple[float, float, float]]:
    """(match_uid, server_id) -> (first-in rate, win-on-first logit,
    win-on-second logit): `stem`'s neutral-state arm outputs for the pending
    `uids`, both servers, in the `chain_arm` transform's units.

    No calibration: the consuming arm calibrates the value in its own fit and
    applies the same logistic at predict, the offset's contract, so the raw
    value is what the trained column holds. `stem`'s own arm inputs are filled
    first (`_pending_projection`'s order), so a source with a source works.
    """
    from mvp.model.features.prior import _LOGIT_EPS

    if not uids:
        return {}
    subset = fill_pending_arms(
        stem, list(uids), df.filter(pl.col("match_uid").is_in(list(uids))),
    )
    config, projector = _load_source(stem)
    pending = _pending_frame(
        config, subset, list(uids),
        parity_columns=list(projector.serve_model.parity_columns), stem=stem,
    )
    if len(pending) == 0:
        return {}
    arms = projector.serve_model.predict_arms(pending)

    def logit(p: float) -> float:
        p = min(max(p, _LOGIT_EPS), 1.0 - _LOGIT_EPS)
        return math.log(p / (1.0 - p))

    return {
        (uid, server): (float(fi), logit(w1), logit(w2))
        for uid, server, fi, w1, w2 in arms.select(
            "match_uid", "server_id", "chain_fi_rate", "chain_w1_prob",
            "chain_w2_prob",
        ).iter_rows()
    }


def inject_arm_columns(
    df: pl.DataFrame, stem: str,
    values: dict[tuple[str, str], tuple[float, float, float]],
    *, injected_wins: bool,
) -> pl.DataFrame:
    """Write `stem`'s arm values into the frame's `chain_arm` columns, in two
    passes: the `player_` columns keyed (match_uid, player_id = server) and
    the `opp_` columns keyed (match_uid, opp_id = server). Only columns the
    frame carries are written.

    `injected_wins` for a source named in an arm offset (the offset's
    contract, `_fill_prior_logit`); existing-wins for a plain `chain_arm`
    feature (`_fill_prior_column`), so a settled row keeps its OOF value.
    """
    if not values:
        return df
    inj = pl.DataFrame({
        "match_uid": [k[0] for k in values],
        "_server": [k[1] for k in values],
        **{
            f"_inj_{bare}": [v[i] for v in values.values()]
            for i, bare in enumerate(_ARM_BARE)
        },
    })
    for side, key in (("player", "player_id"), ("opp", "opp_id")):
        cols = {
            bare: f"{side}_{bare}_{stem}" for bare in _ARM_BARE
            if f"{side}_{bare}_{stem}" in df.columns
        }
        if not cols:
            continue
        joined = df.join(
            inj.rename({"_server": key}).with_columns(
                pl.col(key).cast(df.schema[key])
            ),
            on=["match_uid", key], how="left",
        )
        joined = joined.with_columns([
            (
                pl.coalesce(pl.col(f"_inj_{bare}"), pl.col(col)) if injected_wins
                else pl.coalesce(pl.col(col), pl.col(f"_inj_{bare}"))
            ).alias(col)
            for bare, col in cols.items()
        ])
        df = joined.drop([f"_inj_{bare}" for bare in _ARM_BARE])
    return df


def fill_pending_arms(
    stem: str, uids: list[str], df: pl.DataFrame,
) -> pl.DataFrame:
    """Fill the arm columns of every projection `stem` starts its arms from
    (`arm_source_stems`), for the pending `uids`, and return the frame.

    The transform leaves a pending match null (both stores come from
    target-resolving paths); the source's live arm values stand in, exactly
    as the source's live match-win probability does for a prior. `df` is the
    caller's frame already subset to `uids` where it can be: filling the
    full-corpus frame would cost full-width joins per source and side.
    Recursion ends at a projection naming no arm source.
    """
    if not uids:
        return df
    from mvp.model.prior_naming import prior_kind_of
    from mvp.projection.iid.projection_run import arm_source_stems

    config = _config_of(stem)
    offset_stems = {
        found[1] for spec in config.serve_model.arm_offset.values()
        if (found := prior_kind_of(spec)) is not None
    }
    for source in arm_source_stems(config):
        df = inject_arm_columns(
            df, source, pending_arm_values(source, uids, df),
            injected_wins=source in offset_stems,
        )
    return df
