"""Pin a projection evaluation under projections/ so a model config can name it.

`player_prior_logit(model=<stem>)` and `chain_shape(model=<stem>)` resolve a
projection stem through projections/ and projections/production/ only
(`model/features/prior.py`). A sweep trial's config lives in the sweep dir --
scratch the next sweep of the same parent rewrites -- so a trial a model
should build on is copied out under its run tag. The copy is the evaluation's
own config snapshot, verified to fingerprint back to the evaluation it came
from: the pinned stem then resolves to the artifacts that already exist, not
to an empty dir the resolver would fall through to.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from mvp.projection.iid.artifacts import (
    FOLD_MATCH_WIN_PARQUET,
    PMF_PARQUET,
    SERVE_MODEL_JOBLIB,
    find_by_run_tag,
    iid_fingerprint_dir,
    read_sources,
)

logger = logging.getLogger(__name__)

PROJECTION_DIR = Path("projections")
PRODUCTION_DIRNAME = "production"
# The model namespace: a stem present there too is refused by the resolver,
# so a pin under that stem would make the referencing config unrunnable.
MODEL_DIRS = (Path("models"), Path("models") / "production")
SNAPSHOT = "config.yaml"
# What the prior layer reads from a projection evaluation: the walk-forward
# OOF (`iid-project`), and the forward pmf plus the projector (`iid-backtest`).
_PROJECT_ARTIFACTS = (FOLD_MATCH_WIN_PARQUET,)
_BACKTEST_ARTIFACTS = (PMF_PARQUET, SERVE_MODEL_JOBLIB)


@dataclass(frozen=True)
class PinResult:
    stem: str
    fp: str
    eval_dir: Path
    config_path: Path
    # Artifact names the evaluation dir lacks, so the caller can say which
    # producer still has to run before the stem is usable as a prior.
    missing: tuple[str, ...]

    @property
    def spec(self) -> str:
        return f"player_prior_logit(model={self.stem})"

    @property
    def missing_commands(self) -> list[str]:
        cmds = []
        if any(n in self.missing for n in _PROJECT_ARTIFACTS):
            cmds.append(f"poetry run py -m mvp iid-project {self.stem}")
        if any(n in self.missing for n in _BACKTEST_ARTIFACTS):
            cmds.append(f"poetry run py -m mvp iid-backtest {self.stem}")
        return cmds


def locate(ref: str) -> Path:
    """The evaluation dir `ref` names: a fingerprint (the dir name), else a
    run tag some dir's source.txt recorded (a sweep trial's unique stem)."""
    by_fp = iid_fingerprint_dir(ref)
    if by_fp.is_dir():
        return by_fp
    tagged = find_by_run_tag(ref)
    if tagged is not None:
        return tagged
    raise FileNotFoundError(
        f"{ref!r} is neither a projection evaluation fingerprint nor a run tag "
        f"recorded under {by_fp.parent}"
    )


def default_stem(eval_dir: Path) -> str:
    """The run tag of the evaluation's most recent source line."""
    sources = read_sources(eval_dir)
    if not sources:
        raise ValueError(
            f"{eval_dir}: no source.txt to take a stem from; pass --as <stem>"
        )
    return sources[-1][1]


def pin(
    ref: str,
    *,
    stem: str | None = None,
    production: bool = False,
    force: bool = False,
    projection_dir: Path | None = None,
    model_dirs: tuple[Path, ...] | None = None,
) -> PinResult:
    """Copy `ref`'s config snapshot to `<projection_dir>[/production]/<stem>.yaml`.

    Refuses a stem the model namespace already uses (the resolver refuses
    ambiguity, so the pin would be unusable), an existing file of different
    content unless `force`, and a production pin that a different scratch
    copy of the same stem would shadow (the resolver searches projections/
    first). The written file is fingerprinted through the resolver's own
    normalisation and must equal the evaluation's dir name; otherwise it is
    removed again, because a pin that resolves to an empty dir is worse than
    none.
    """
    eval_dir = locate(ref)
    stem = stem or default_stem(eval_dir)
    snapshot = eval_dir / SNAPSHOT
    if not snapshot.exists():
        raise FileNotFoundError(f"{eval_dir}: no {SNAPSHOT} to pin")

    for d in model_dirs or MODEL_DIRS:
        clash = Path(d) / f"{stem}.yaml"
        if clash.exists():
            raise ValueError(
                f"{stem!r} already names a model config at {clash}; the prior "
                "resolver refuses a stem in both namespaces. Pin under another "
                "name with --as."
            )

    scratch_dir = Path(projection_dir or PROJECTION_DIR)
    out_dir = scratch_dir / PRODUCTION_DIRNAME if production else scratch_dir
    target = out_dir / f"{stem}.yaml"
    text = snapshot.read_text(encoding="utf-8")

    if target.exists() and target.read_text(encoding="utf-8") != text and not force:
        raise FileExistsError(
            f"{target} exists with different content; --force to overwrite it "
            "(every config naming this stem then resolves to the new evaluation)"
        )
    if production:
        shadow = scratch_dir / f"{stem}.yaml"
        if shadow.exists() and shadow.read_text(encoding="utf-8") != text:
            raise FileExistsError(
                f"{shadow} exists with different content and the resolver "
                f"searches {scratch_dir} before {out_dir}, so the production pin "
                "would be shadowed. Remove or --force-pin the scratch copy first."
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")

    from mvp.model.features.prior import _snapshot_fingerprint_projection

    fp = _snapshot_fingerprint_projection(target)
    if fp != eval_dir.name:
        target.unlink()
        raise RuntimeError(
            f"{snapshot} fingerprints to {fp}, not {eval_dir.name}: the pinned "
            "copy would resolve to an empty evaluation, so it was removed. The "
            "snapshot predates a fingerprint change; re-run the evaluation."
        )

    missing = tuple(
        n for n in (*_PROJECT_ARTIFACTS, *_BACKTEST_ARTIFACTS)
        if not (eval_dir / n).exists()
    )
    logger.info("pinned projection evaluation %s as %s", fp, target)
    return PinResult(
        stem=stem, fp=fp, eval_dir=eval_dir, config_path=target, missing=missing,
    )
