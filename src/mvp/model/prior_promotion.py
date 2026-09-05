"""Production owns the prior artifacts it depends on.

A production entry (the lead, a residual stage, a voter) can declare another
model's out-of-sample probability as a feature -- `player_prior_logit(model=X)`,
the `offset.prior` sugar for it, or `chain_shape(model=X)`. Those columns are
built from X's EVALUATION artifacts, which live in a fingerprint-keyed scratch
dir that:

- the weekly eval wipe deletes from (`wipe_stale_evaluations` removes every
  entry under model_evaluations whose mtime predates Monday);
- any ad-hoc run of X overwrites (a 3-day backtest replaced the lead's forward
  ledger with 380 rows on 2026-09-05);
- a config edit relocates (the fingerprint changes; production resolves to an
  empty dir).

And nothing in the tree could see a fourth kind of staleness: an artifact fit
by code that has since changed. The fingerprint hashes config content and the
projector check compares config text, so a serve-model fix leaves every
artifact "valid" by every check that existed (two_level_flat's projector was
fit 2026-09-01; the serve model it runs through was fixed 2026-09-05, and every
check passed).

This module is the promotion path that closes all four:

1. `mvp train` REGENERATES every prior its entries depend on, base-first and
   unconditionally: the evaluation (fold OOF) and the forward artifact
   (backtest ledger, or projector + pmf), through the same functions the
   commands a human would run call, with retrain forced so no cached fit
   survives a code change. Unconditional is the price of "never stale"
   without a code-version stamp; `mvp train` is a promotion command, and a
   promotion is exactly when the chain must agree with current code and data.
2. It then COPIES the artifacts into `promoted_dir(stem)`, under the data
   root's models/ dir, which no wipe targets and no experiment writes to.
   `resolve_prior` redirects every reader -- training, the tick's transform,
   the projection serving path -- to that copy whenever it exists.
3. The tick PREFLIGHTS the promoted store before scoring anything: every
   dependency present, complete by its readers' own schema checks, and (for a
   projection) its projector loadable against the current config text. It
   never regenerates; a live box must never start an evaluation. A failure
   raises before any prediction exists, so nothing reaches the sheet.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mvp.model.features.prior import (
    PROMOTED_MANIFEST,
    PriorSource,
    _backtest_cutoffs,
    _cached_frame,
    _cached_shape_frame,
    _forward_artifact_ready,
    _load_config,
    _source_tags,
    prior_artifacts_ready,
    promoted_dir,
    promoted_fingerprint,
    resolve_prior,
)
from mvp.model.prior_naming import prior_model_of

logger = logging.getLogger(__name__)

_CHAIN_SHAPE_RE = re.compile(r"^chain_shape\(model=([^)]+)\)$")

# What a promoted copy carries, by kind. `_REQUIRED` must exist and pass the
# readers' own schema checks before anything is copied; `_OPTIONAL` ride along
# when present (provenance: the evaluation's config snapshot and source tags,
# the latter also read by `build_prior_frame` for renamed-config fallbacks).
_REQUIRED = {
    "model": ("fold_predictions.parquet", "backtest.csv"),
    "projection": (
        "fold_match_win.parquet", "total_games_pmf.parquet", "serve_model.joblib",
    ),
}
_OPTIONAL = ("source.txt", "config.yaml")
CUTOFFS_JSON = "cutoffs.json"
MANIFEST = PROMOTED_MANIFEST
TRAIN_COMMAND = "poetry run py -m mvp train"


class PreflightError(RuntimeError):
    """A production dependency is missing, unpromoted, incomplete or unloadable."""


def _clear_prior_caches() -> None:
    """Every in-process cache keyed on a prior stem, so a read after
    regeneration or promotion cannot return a frame built from the artifacts
    that were just replaced: the prior frame, the chain-shape frame, and the
    serving path's forward calibrator."""
    from mvp.model.projection_serving import _forward_calibrator

    _cached_frame.cache_clear()
    _cached_shape_frame.cache_clear()
    _forward_calibrator.cache_clear()


# ---------------------------------------------------------------------------
# What a config depends on
# ---------------------------------------------------------------------------


def prior_stem_of(spec: str) -> str | None:
    """The stem a prior-bearing spec or column names: `player_prior_logit(model=X)`,
    the engine column `player_prior_logit_X`, or `chain_shape(model=X)`."""
    stem = prior_model_of(spec)
    if stem is not None:
        return stem
    m = _CHAIN_SHAPE_RE.match(spec.strip())
    return m.group(1).strip().strip("'\"") if m else None


def declared_prior_specs(
    config: Any,
    resolved_specs: list[str] | None = None,
    entry_filters: dict[str, Any] | None = None,
) -> list[str]:
    """Every spec or column name through which a prior can enter a config's
    design matrix or its filters, in declaration order.

    Scans every site: `features.include`, `features.compute_only`, the keys of
    `data.filters` and of the production entry's own `filters` (a prior used
    only as a `not_null` clause), the offset feature, and `resolved_specs` --
    the union an ENSEMBLE's base configs contribute, whose own `features` block
    is typically absent. Filter keys are read directly rather than through
    `get_filter_feature_specs`, which strips the `player_` prefix and requires
    a registered feature name, so a prior COLUMN never comes back from it.
    """
    specs: list[str] = []
    feats = getattr(config, "features", None)
    if feats is not None:
        specs += list(getattr(feats, "include", None) or [])
        specs += list(getattr(feats, "compute_only", None) or [])
    data = getattr(config, "data", None)
    specs += list((getattr(data, "filters", None) or {}).keys())
    specs += list((entry_filters or {}).keys())
    offset = getattr(config, "offset", None)
    if offset is not None and getattr(offset, "feature", None):
        specs.append(offset.feature)
    specs += list(resolved_specs or [])
    return specs


def declared_prior_stems(
    config: Any,
    resolved_specs: list[str] | None = None,
    entry_filters: dict[str, Any] | None = None,
) -> list[str]:
    """Every prior stem `declared_prior_specs` reaches, deduplicated, in order."""
    out: list[str] = []
    for spec in declared_prior_specs(config, resolved_specs, entry_filters):
        stem = prior_stem_of(spec)
        if stem is not None and stem not in out:
            out.append(stem)
    return out


def _config_prior_stems(config_path: Path) -> list[str]:
    """Prior stems a MODEL config declares, including through an ensemble's
    base configs: an ensemble's design matrix is the union of its bases'
    `features.include` (plus `meta_features`), and its own `features` block is
    typically absent, so scanning the ensemble file alone misses every prior
    a base declares. Mirrors `ProductionPredictor._resolve_entry_features`.
    """
    from mvp.model.config import EnsembleParams

    config = _load_config(config_path)
    resolved: list[str] = []
    if config.model.type == "ensemble":
        params = EnsembleParams.model_validate(config.model.params)
        for ref in params.base_models:
            base = _load_config(Path(ref.config))
            if base.features is not None:
                resolved += list(base.features.include or [])
        resolved += list(params.meta_features or [])
    return declared_prior_stems(config, resolved)


def dependency_order(stems: list[str]) -> list[PriorSource]:
    """Resolve `stems` and everything THEY depend on, base-first, each once.

    A model-kind prior's own config may declare priors (a stage used as a
    prior offsets on the lead; an ensemble's bases may declare one). Those
    come first: regenerating the dependent runs its predictor, which reads
    them. A projection config declares none. Resolves the EVALUATION side
    (`promoted=False`); only `kind` and `config_path` are read here, and an
    unpromoted stem must still order.
    """
    ordered: list[PriorSource] = []
    seen: set[str] = set()

    def visit(stem: str, chain: tuple[str, ...]) -> None:
        if stem in seen:
            return
        if stem in chain:
            raise ValueError(
                f"prior dependency cycle: {' -> '.join(chain + (stem,))}"
            )
        source = resolve_prior(stem, promoted=False)
        if source.kind == "model":
            for base in _config_prior_stems(source.config_path):
                visit(base, chain + (stem,))
        seen.add(stem)
        ordered.append(source)

    for stem in stems:
        visit(stem, ())
    return ordered


# ---------------------------------------------------------------------------
# Regenerate
# ---------------------------------------------------------------------------


def regenerate_prior(source: PriorSource) -> None:
    """Re-run `source`'s evaluation AND forward artifact from current code and
    data, into its fingerprint dir. Unconditional, retrain forced.

    Model kind: the `model` command's evaluation (fold OOF with `y_prob_cal`),
    then the lead backtest (forward ledger plus the per-fold lead artifacts
    that date it). Projection kind: the walk-forward (`fold_match_win`), then
    the forward projection with `retrain=True` -- `train_or_load` would
    otherwise keep any projector whose stored config text still matches, which
    is exactly how a fit from before a serve-model fix survives.
    """
    if source.kind == "projection":
        from mvp.projection.iid.projection_run import run_projection
        from mvp.projection.iid.runner import IIDProjectionRunner

        logger.warning(
            "prior %s: regenerating projection walk-forward from %s",
            source.model, source.config_path,
        )
        IIDProjectionRunner(config_path=source.config_path).run()
        logger.warning(
            "prior %s: refitting projector and forward pmf", source.model,
        )
        run_projection(source.config_path, retrain=True)
    else:
        from mvp.model.backtest import artifact_dir, run_backtest
        from mvp.model.runner import ExperimentRunner

        logger.warning(
            "prior %s: regenerating evaluation from %s",
            source.model, source.config_path,
        )
        ExperimentRunner(config_path=source.config_path).run()
        # The backtest keys its per-fold lead artifacts by stem and never
        # clears the dir, so `lead_<date>.joblib` files from an earlier
        # validation schedule accumulate, and `_backtest_cutoffs` would date
        # the new ledger by a stale union of boundaries -- which promotion
        # would then freeze into cutoffs.json. Start clean: what remains is
        # this run's folds.
        stale = artifact_dir(source.config_path)
        if stale.exists():
            shutil.rmtree(stale)
        logger.warning(
            "prior %s: regenerating forward ledger (lead backtest)", source.model,
        )
        run_backtest(source.config_path, retrain=True)
    _clear_prior_caches()


# ---------------------------------------------------------------------------
# Promote
# ---------------------------------------------------------------------------


def check_unambiguous_config(source: PriorSource) -> None:
    """A stem present in more than one config dir must fingerprint the same in
    all of them.

    The search order puts the scratch dir (`models/`, `projections/`) before the
    versioned one (`.../production/`). The dev box has both; a checkout without
    scratch -- the serving box -- has only the versioned copy. A promotion made
    from the scratch copy is then not a promotion of the config the serving box
    resolves, and its preflight fails on a fingerprint the dev box never saw.
    Refused here, at promotion, where it is a config fix rather than a tick
    outage.
    """
    from mvp.model.features import prior as _prior

    dirs = (
        _prior.PROJECTION_CONFIG_DIRS if source.kind == "projection"
        else _prior.CONFIG_DIRS
    )
    paths = [
        Path(d) / f"{source.model}.yaml" for d in dirs
        if (Path(d) / f"{source.model}.yaml").exists()
    ]
    if len(paths) < 2:
        return
    fp_of = (
        _prior._snapshot_fingerprint_projection if source.kind == "projection"
        else _prior._snapshot_fingerprint_model
    )
    fps = {p: fp_of(p) for p in paths}
    if len(set(fps.values())) > 1:
        raise PreflightError(
            f"prior {source.model}: {len(paths)} configs for this stem "
            "fingerprint differently: "
            + ", ".join(f"{p} -> {f}" for p, f in fps.items())
            + ". A checkout without the scratch copy resolves a different "
            "config than this box; make them identical or remove the scratch copy."
        )


def _check_complete(source: PriorSource) -> None:
    """The readers' own readiness checks, raised instead of returned."""
    if not prior_artifacts_ready(source):
        raise PreflightError(
            f"prior {source.model}: fold OOF missing or incomplete at "
            f"{source.eval_dir}"
        )
    if not _forward_artifact_ready(source):
        raise PreflightError(
            f"prior {source.model}: forward artifact missing or incomplete at "
            f"{source.eval_dir}"
        )
    if source.kind == "projection":
        from mvp.model.projection_serving import check_served_projector

        try:
            check_served_projector(
                source.model, source.eval_dir / "serve_model.joblib",
            )
        except FileNotFoundError as e:
            raise PreflightError(str(e)) from e


def _replace_copy(src: Path, dst: Path) -> None:
    tmp = dst.with_name(dst.name + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def _replace_write(dst: Path, text: str) -> None:
    tmp = dst.with_name(dst.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, dst)


def promote_prior(stem: str) -> Path:
    """Copy `stem`'s regenerated artifacts into the promoted store.

    Reads the EVALUATION dir (`promoted=False`); the promoted copy, if one
    exists, is what this replaces. Refuses before copying anything if the
    evaluation is incomplete by the readers' schema checks or, for a
    projection, its projector does not load against the current config text.

    Each file is copied to a sibling temp name and `os.replace`d, so a reader
    on the shared drive never sees a torn file and the directory never
    disappears. The manifest is written LAST, so its presence means the set
    is complete. `PriorSource.salt()` keys on these mtimes, so the next
    feature read after promotion drops its cached column and re-reads.
    """
    source = resolve_prior(stem, promoted=False)
    check_unambiguous_config(source)
    _check_complete(source)
    dst = promoted_dir(stem)
    dst.mkdir(parents=True, exist_ok=True)
    # Invalidate the PREVIOUS promotion before touching any file. Its manifest
    # carries the same fingerprint, so while it stands `resolve_prior` keeps
    # redirecting readers into the copy window, and a crash mid-copy leaves a
    # mixed set (new OOF, old ledger) that every check accepts. Without a
    # manifest the dir reads as unpromoted: preflight refuses, nothing reads
    # it, and the new manifest written last is what makes the new set visible.
    (dst / MANIFEST).unlink(missing_ok=True)
    _clear_prior_caches()
    files = list(_REQUIRED[source.kind]) + [
        n for n in _OPTIONAL if (source.eval_dir / n).exists()
    ]
    for name in files:
        _replace_copy(source.eval_dir / name, dst / name)
    if source.kind == "model":
        stems = [source.stem] + [
            t for t in _source_tags(source.eval_dir) if t != source.stem
        ]
        cutoffs = _backtest_cutoffs(stems)
        if not cutoffs:
            raise PreflightError(
                f"prior {stem}: backtest ran but no per-fold lead artifacts "
                f"under backtests/lead/{{{', '.join(stems)}}}; the forward "
                "ledger cannot be dated"
            )
        _replace_write(
            dst / CUTOFFS_JSON, json.dumps([d.isoformat() for d in cutoffs]),
        )
        files.append(CUTOFFS_JSON)
    manifest = {
        "stem": stem,
        "kind": source.kind,
        "fingerprint": source.fp,
        "evaluation_dir": str(source.eval_dir),
        "config_path": str(source.config_path),
        "promoted_at": datetime.now(UTC).isoformat(),
        "files": files,
    }
    _replace_write(dst / MANIFEST, json.dumps(manifest, indent=2))
    _clear_prior_caches()
    logger.info(
        "prior %s: promoted %d file(s) from %s to %s",
        stem, len(files), source.eval_dir, dst,
    )
    return dst


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------


def verify_promoted(stem: str) -> PriorSource:
    """The promoted source for `stem`, complete and loadable, or PreflightError.

    Checks that resolution lands on the promoted dir at all (production must
    never read evaluation scratch) -- which requires a complete promotion
    (manifest present) made from the CURRENT config's fingerprint -- then that
    every reader's schema check passes on the copy, that a model kind carries
    its cutoffs, and that a projection kind's projector loads against the
    current config text. Never regenerates.
    """
    source = resolve_prior(stem)
    check_unambiguous_config(source)
    pdir = promoted_dir(stem)
    if source.eval_dir != pdir:
        made_from = promoted_fingerprint(pdir)
        if made_from is None:
            raise PreflightError(
                f"prior {stem}: not promoted (no complete copy at {pdir}); "
                f"production must not read evaluation scratch. "
                f"Run: {TRAIN_COMMAND}"
            )
        if made_from != source.fp:
            raise PreflightError(
                f"prior {stem}: the promoted copy at {pdir} was made from "
                f"config fingerprint {made_from}, but {source.config_path} now "
                f"fingerprints to {source.fp}; the config changed since "
                f"promotion. Run: {TRAIN_COMMAND}"
            )
        raise PreflightError(
            f"prior {stem}: the promoted copy at {pdir} is incomplete or "
            f"predates the current artifact schema, so resolution fell through "
            f"to evaluation scratch. Run: {TRAIN_COMMAND}"
        )
    _check_complete(source)
    if source.kind == "model" and not source.cutoffs_json.exists():
        raise PreflightError(
            f"prior {stem}: promoted copy at {pdir} has no {CUTOFFS_JSON}; "
            f"its forward rows cannot be dated. Run: {TRAIN_COMMAND}"
        )
    return source
