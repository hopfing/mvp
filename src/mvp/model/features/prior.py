"""An earlier model's out-of-sample log-odds, as a feature — resolved from
the model's own config file and evaluation artifacts.

A residual stage is a model trained on another model's error: it offsets on
that model's honest (out-of-sample) log-odds and its trees fit what is left.
The base model is named by config stem in the stage's own config — the
stem of a config under models/ (or models/production/)::

    offset:
      prior: stage1_lead_residual__h19_t218

which is sugar for ``offset.feature: player_prior_logit(model=<stem>)``,
pinning that column in the base features and filtering rows to where it is
not null. Nothing is built by hand: the evaluation fingerprint is computed
from that config, so the artifacts are exactly the `model` command's for it
(``model_evaluations/<fp>/fold_predictions.parquet`` with the calibrated
``y_prob_cal``, plus ``backtest.csv`` in the same dir with fold cutoffs from
``backtests/lead/<stem>/lead_<date>.joblib`` for the walk-forward span). If
the fold OOF is missing, the discovery driver regenerates it from the config;
train/serve refuse with the command.

The transform is parameterised by ``model``, so each base model is its own
column (``player_prior_logit_<stem>``) and its own cache group; it declares a
cache salt from the resolved artifacts, so a re-evaluated stem recomputes
that one column and nothing else.

OOF rule: every row carries ``prior_train_end`` (the last date the base model
trained on for that row); stage training refuses rows dated on or before it
(predictor.py). Two calibration states are never spliced: the fold OOF must be
the post-nested-CV-Platt ``y_prob_cal`` and the backtest rows are post-Platt.

NULL WHERE NO OOF ROW. Left join, ``impute=None``: a match the base model never
scored out of sample stays null; the offset's logistic cannot take a null, so
a config offsetting on this filters its rows (the sugar does it).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

import numpy as np
import polars as pl

from mvp.common.base_job import get_data_root
from mvp.model.registry import register_transform

logger = logging.getLogger(__name__)

from mvp.model.prior_naming import prior_column, prior_model_of, prior_spec  # noqa: F401

_OUTPUTS = ["player_prior_prob", "player_prior_logit"]
_LOGIT_EPS = 1e-6


# Where a base model's config is looked up by stem. Tuned trial configs are
# copied to models/ (the `model` command's own dir); deployed ones live in
# models/production/. A stage names its base by stem only, so the column
# name stays a plain identifier and the reference is the same file the
# `model` command would run.
CONFIG_DIRS = (Path("models"), Path("models") / "production")
# Test seams, consulted at call time: where evaluations / backtests live
# (None = the data root's model_evaluations / backtests).
EVALUATIONS_ROOT: Path | None = None
BACKTESTS_ROOT: Path | None = None


@dataclass(frozen=True)
class PriorSource:
    model: str  # the config stem the stage named
    config_path: Path  # the base model's config file
    fp: str  # its evaluation fingerprint (a pure function of the config)
    eval_dir: Path

    @property
    def stem(self) -> str:
        return self.model

    @property
    def fold_predictions(self) -> Path:
        return self.eval_dir / "fold_predictions.parquet"

    @property
    def backtest_csv(self) -> Path:
        return self.eval_dir / "backtest.csv"

    def salt(self) -> str:
        """Identity of the artifacts behind the column, for the cache."""
        parts = [self.fp]
        for p in (self.fold_predictions, self.backtest_csv):
            parts.append(str(int(p.stat().st_mtime)) if p.exists() else "-")
        return ":".join(parts)

    @property
    def regenerate_command(self) -> str:
        return f"poetry run py -m mvp model {self.config_path.as_posix()}"


def find_prior_config(model: str, config_dirs=None) -> Path:
    """The base model's config file for a stem."""
    config_dirs = tuple(config_dirs) if config_dirs else CONFIG_DIRS
    for d in config_dirs:
        p = Path(d) / f"{model}.yaml"
        if p.exists():
            return p
    raise FileNotFoundError(
        f"offset.prior {model!r}: no {model}.yaml under "
        f"{', '.join(str(d) for d in config_dirs)}. The base model's config "
        "must be there (copy the tuned trial's config to models/)."
    )


def _load_config(path: Path):
    """ExperimentConfig from a config file, accepting the evaluation
    snapshot form: `write_config_snapshot` writes the canonical dump, whose
    top-level `metrics_objective` is `metrics.objective` flattened, and
    tuned trial configs copied from there carry it. Folded back so the
    fingerprint matches the evaluation the file came from."""
    import yaml

    from mvp.model.config import ExperimentConfig

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data.pop("name", None)
    data.pop("selection_history", None)
    objective = data.pop("metrics_objective", None)
    if objective is not None:
        metrics = data.setdefault("metrics", {}) or {}
        metrics.setdefault("objective", objective)
        data["metrics"] = metrics
    return ExperimentConfig.model_validate(data)


def _evaluations_root() -> Path:
    return EVALUATIONS_ROOT or (get_data_root() / "model_evaluations")


def _source_tag(eval_dir: Path) -> str | None:
    """First field of the evaluation's source.txt: the config stem/tag the
    `model` command ran."""
    src = eval_dir / "source.txt"
    if not src.exists():
        return None
    lines = src.read_text(encoding="utf-8").splitlines()
    return lines[0].split("\t")[0].strip() if lines else None


def resolve_prior(model: str, config_dirs=None) -> PriorSource:
    """Config stem -> its config file -> evaluation fingerprint -> eval dir.

    The fingerprint is computed from the config, so the lookup is exact when
    the file is the one the `model` command ran. A production copy can
    differ from that file in fields the fingerprint sees (e.g. a stripped
    `metrics_objective`); then the evaluation tagged with this stem in its
    source.txt is used instead, logged, and only if neither exists does the
    caller regenerate.
    """
    from mvp.common.config_hash import compute_fingerprint

    path = find_prior_config(model, config_dirs)
    fp = compute_fingerprint(_load_config(path), config_path=path)
    root = _evaluations_root()
    eval_dir = root / fp
    if not eval_dir.is_dir() and root.is_dir():
        tagged = [
            d for d in root.iterdir() if d.is_dir() and _source_tag(d) == model
        ]
        if tagged:
            tagged.sort(key=lambda d: d.stat().st_mtime, reverse=True)
            logger.info(
                "offset.prior %s: %s fingerprints to %s (no evaluation there); "
                "using the evaluation tagged with that stem, %s",
                model, path, fp, tagged[0].name,
            )
            fp, eval_dir = tagged[0].name, tagged[0]
    return PriorSource(model=model, config_path=path, fp=fp, eval_dir=eval_dir)


def prior_artifacts_ready(source: PriorSource) -> bool:
    """Fold OOF present with the calibrated column."""
    p = source.fold_predictions
    if not p.exists():
        return False
    return "y_prob_cal" in pl.scan_parquet(p).collect_schema().names()


def ensure_prior_artifacts(source: PriorSource, regenerate: bool) -> None:
    """Make sure the base model's fold OOF exists; regenerate it from its
    config when allowed (the discovery driver does; train/serve refuse with
    the command instead — a live box must never start an evaluation)."""
    if prior_artifacts_ready(source):
        return
    if not regenerate:
        raise FileNotFoundError(
            f"offset.prior {source.model}: no calibrated fold OOF at "
            f"{source.fold_predictions}. Evaluate the base model first: "
            f"{source.regenerate_command}"
        )
    from mvp.model.runner import ExperimentRunner

    # A chain regenerates in order: the base model's own prior (if it has
    # one) must exist before its evaluation can run.
    base_cfg = _load_config(source.config_path)
    if base_cfg.offset is not None and base_cfg.offset.prior is not None:
        ensure_prior_artifacts(resolve_prior(base_cfg.offset.prior), regenerate=True)
        _cached_frame.cache_clear()

    logger.warning(
        "offset.prior %s: no evaluation at %s -- regenerating from %s "
        "(a full `model` run; this is the base model's own evaluation, "
        "written where the `model` command would write it)",
        source.model, source.eval_dir, source.config_path,
    )
    ExperimentRunner(config_path=source.config_path).run()
    _cached_frame.cache_clear()
    if not prior_artifacts_ready(source):
        raise RuntimeError(
            f"offset.prior {source.model}: evaluation ran but "
            f"{source.fold_predictions} still lacks y_prob_cal"
        )


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, _LOGIT_EPS, 1 - _LOGIT_EPS)
    return np.log(p / (1 - p))


def _backtest_cutoffs(stems: list[str], backtests_root: Path | None = None) -> list[date]:
    """Fold test-start dates from the backtest's per-fold lead artifacts,
    under the first of ``stems`` that has any. The backtest keys its
    artifact dir by the config stem it ran under; a renamed config still
    finds them through the tags its evaluation's source.txt recorded."""
    root = backtests_root or BACKTESTS_ROOT or (get_data_root() / "backtests")
    for stem in stems:
        d = root / "lead" / stem
        tags = sorted(
            date.fromisoformat(p.stem.removeprefix("lead_"))
            for p in d.glob("lead_*.joblib")
            if "_cal_tiers" not in p.stem
        )
        if tags:
            return tags
    return []


def _source_tags(eval_dir: Path) -> list[str]:
    src = eval_dir / "source.txt"
    if not src.exists():
        return []
    return [ln.split("	")[0].strip() for ln in src.read_text(encoding="utf-8").splitlines() if ln.strip()]


def _fold_rows(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    if "y_prob_cal" not in df.columns:
        raise ValueError(
            f"{path} has no y_prob_cal (columns: {df.columns}); it predates the "
            "calibrated export. Re-run `model` on that config."
        )
    df = df.with_columns(pl.col("effective_match_date").cast(pl.Date).alias("day"))
    train_end = df.group_by("fold_idx").agg(
        (pl.col("day").min() - timedelta(days=1)).alias("prior_train_end")
    )
    return df.join(train_end, on="fold_idx").select(
        "match_uid", "player_id", "day",
        pl.col("y_prob_cal").alias("prior_prob"),
        "prior_train_end",
        pl.lit("fold_oof_nested_cal").alias("prior_kind"),
    )


def _backtest_rows(path: Path, cutoffs: list[date]) -> pl.DataFrame:
    bt = pl.read_csv(path, infer_schema_length=5000).with_columns(
        pl.col("effective_match_date").str.slice(0, 10).str.to_date().alias("day")
    )
    days = bt["day"].to_numpy().astype("datetime64[D]")
    starts = np.array(cutoffs, dtype="datetime64[D]")
    idx = np.searchsorted(starts, days, side="right") - 1
    dropped = int((idx < 0).sum())
    if dropped:
        logger.info(
            "prior: dropping %d backtest rows before the first fold cutoff %s",
            dropped, cutoffs[0],
        )
    train_end = [(cutoffs[i] - timedelta(days=1)) if i >= 0 else None for i in idx]
    return (
        bt.with_columns(pl.Series("prior_train_end", train_end, dtype=pl.Date))
        .filter(pl.col("prior_train_end").is_not_null())
        .select(
            "match_uid", "player_id", "day",
            pl.col("model_prob").alias("prior_prob"),
            "prior_train_end",
            pl.lit("backtest_fold_cal").alias("prior_kind"),
        )
    )


def build_prior_frame(
    source: PriorSource, backtests_root: Path | None = None
) -> pl.DataFrame:
    """One row per (match_uid, player_id) the base model scored out of sample:
    ``prior_prob``, ``prior_logit``, ``prior_train_end``, ``prior_kind``.
    Fold OOF spliced with the backtest's walk-forward rows; refuses overlaps,
    duplicates and any row dated on/before its own train end."""
    parts = [_fold_rows(source.fold_predictions)]
    if source.backtest_csv.exists():
        stems = [source.stem] + [t for t in _source_tags(source.eval_dir) if t != source.stem]
        cutoffs = _backtest_cutoffs(stems, backtests_root)
        if cutoffs:
            parts.append(_backtest_rows(source.backtest_csv, cutoffs))
        else:
            logger.warning(
                "prior %s: backtest.csv present but no fold artifacts under "
                "backtests/lead/{%s}; using fold OOF only", source.model, ", ".join(stems),
            )
    if len(parts) == 2:
        overlap = parts[0].join(parts[1], on=["match_uid", "player_id"], how="inner").height
        if overlap:
            raise ValueError(
                f"prior {source.model}: {overlap} (match, player) rows in both "
                "the fold OOF and the backtest; refusing to splice"
            )
    df = pl.concat(parts).sort(["day", "match_uid", "player_id"])
    if df.select(["match_uid", "player_id"]).is_duplicated().any():
        raise ValueError(f"prior {source.model}: duplicate (match_uid, player_id) rows")
    leaked = df.filter(pl.col("day") <= pl.col("prior_train_end")).height
    if leaked:
        raise ValueError(
            f"prior {source.model}: {leaked} rows dated on/before their train end"
        )
    return df.with_columns(
        pl.Series("prior_logit", _logit(df["prior_prob"].to_numpy()))
    ).rename({"day": "effective_match_date"})


@lru_cache(maxsize=8)
def _cached_frame(model: str) -> tuple[PriorSource, pl.DataFrame]:
    source = resolve_prior(model)
    # Never regenerates from here: this runs inside the feature engine (FS
    # precompute, tuning, production train). The discovery driver decides
    # to regenerate before precompute; everyone else gets the command.
    ensure_prior_artifacts(source, regenerate=False)
    return source, build_prior_frame(source)


def prior_frame(model: str) -> tuple[PriorSource, pl.DataFrame]:
    """Resolved source and its spliced OOF frame (in-process cache)."""
    return _cached_frame(model)


def _prior_transform(df: pl.DataFrame, model: str) -> pl.DataFrame:
    """Engine transform: the base model's OOF prior keyed to each row. Returns
    just the keyed outputs (bare names; the engine suffixes ``_<model>``)."""
    _source, frame = prior_frame(str(model))
    prior = frame.select(
        "match_uid", "player_id",
        pl.col("prior_prob").alias("player_prior_prob"),
        pl.col("prior_logit").alias("player_prior_logit"),
    )
    return (
        df.select("match_uid", "player_id")
        .join(prior, on=["match_uid", "player_id"], how="left")
        .select("match_uid", "player_id", *_OUTPUTS)
    )


def _prior_salt(model: str) -> str:
    return resolve_prior(str(model)).salt()


register_transform(
    name="prior",
    func=_prior_transform,
    outputs=_OUTPUTS,
    params=["model"],
    cache_salt=_prior_salt,
    description=(
        "An earlier model's out-of-sample win probability and log-odds, "
        "resolved from its evaluation by config stem (model=<stem>)"
    ),
)
