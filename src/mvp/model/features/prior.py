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
# A stem may instead name an IID PROJECTION config: the prior column is then
# the projector's OOF match-win probability (chain on the fitted serve
# model), resolved to B:/projection_evaluations via the iid fingerprint.
# Projection configs live where iid-project runs them (projections/); a
# specific sweep trial is pinned by placing its snapshot config there under
# its own stem. A stem present in BOTH namespaces is refused, never guessed.
PROJECTION_CONFIG_DIRS = (Path("projections"),)
# Test seams, consulted at call time: where evaluations / backtests live
# (None = the data root's model_evaluations / backtests /
# projection_evaluations).
EVALUATIONS_ROOT: Path | None = None
BACKTESTS_ROOT: Path | None = None
PROJECTION_EVALUATIONS_ROOT: Path | None = None


@dataclass(frozen=True)
class PriorSource:
    model: str  # the config stem the stage named
    config_path: Path  # the base model's config file
    fp: str  # its evaluation fingerprint (a pure function of the config)
    eval_dir: Path
    # "model" (an ExperimentConfig evaluation) or "projection" (an IID
    # projection evaluation; the probability is the chain's p_match_win_a).
    kind: str = "model"
    # Projection only: the single-fit cutoff behind the forward (pmf) rows —
    # the config's date_range.end. One fit, one honest train end.
    forward_train_end: date | None = None

    @property
    def stem(self) -> str:
        return self.model

    @property
    def fold_predictions(self) -> Path:
        return self.eval_dir / "fold_predictions.parquet"

    @property
    def backtest_csv(self) -> Path:
        return self.eval_dir / "backtest.csv"

    @property
    def fold_match_win(self) -> Path:
        return self.eval_dir / "fold_match_win.parquet"

    @property
    def pmf_parquet(self) -> Path:
        return self.eval_dir / "total_games_pmf.parquet"

    def _artifact_paths(self) -> tuple[Path, ...]:
        if self.kind == "projection":
            return (self.fold_match_win, self.pmf_parquet)
        return (self.fold_predictions, self.backtest_csv)

    def salt(self) -> str:
        """Identity of the artifacts behind the column, for the cache."""
        parts = [self.fp]
        for p in self._artifact_paths():
            parts.append(str(int(p.stat().st_mtime)) if p.exists() else "-")
        return ":".join(parts)

    @property
    def regenerate_command(self) -> str:
        if self.kind == "projection":
            return f"poetry run py -m mvp iid-project {self.config_path.stem}"
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


def _projection_evaluations_root() -> Path:
    return PROJECTION_EVALUATIONS_ROOT or (
        get_data_root() / "projection_evaluations"
    )


def _find_projection_config(model: str, projection_config_dirs=None) -> Path | None:
    dirs = (
        tuple(projection_config_dirs)
        if projection_config_dirs else PROJECTION_CONFIG_DIRS
    )
    for d in dirs:
        p = Path(d) / f"{model}.yaml"
        if p.exists():
            return p
    return None


def _source_tag(eval_dir: Path) -> str | None:
    """First field of the evaluation's source.txt: the config stem/tag the
    `model` command ran."""
    src = eval_dir / "source.txt"
    if not src.exists():
        return None
    lines = src.read_text(encoding="utf-8").splitlines()
    return lines[0].split("\t")[0].strip() if lines else None


def _projection_run_tag(eval_dir: Path) -> str | None:
    """The RUN tag of a projection evaluation: field 2 of source.txt. Field 1
    is the grouping source — the PARENT stem for every sweep trial — so
    matching on it would hand a parent stem some arbitrary trial's dir."""
    src = eval_dir / "source.txt"
    if not src.exists():
        return None
    lines = src.read_text(encoding="utf-8").splitlines()
    if not lines:
        return None
    fields = lines[0].split("	")
    return fields[1].strip() if len(fields) > 1 else None


def _snapshot_fingerprint_model(snapshot: Path) -> str:
    from mvp.common.config_hash import compute_fingerprint

    return compute_fingerprint(_load_config(snapshot), config_path=snapshot)


def _snapshot_fingerprint_projection(snapshot: Path) -> str:
    from mvp.common.config_hash import compute_iid_fingerprint
    from mvp.projection.iid.config import IIDProjectionConfig

    return compute_iid_fingerprint(
        IIDProjectionConfig.from_file(snapshot), config_path=snapshot
    )


def _tagged_fallback(
    root: Path, model: str, fp: str, path: Path,
    tag_of=_source_tag, snapshot_fp=_snapshot_fingerprint_model,
) -> tuple[str, Path]:
    """The eval dir for `fp`, or the newest EQUIVALENT one tagged with this
    stem when the fingerprint has no dir.

    A tag says an evaluation once ran under this name; only the dir's own
    stored config snapshot says what it evaluated. A candidate is accepted
    solely when its snapshot, through the same normalization as the current
    file, fingerprints identically — i.e. the mismatch was recoverable
    cosmetics. Anything else (a semantic edit, a copy that truly lost a
    fingerprinted field, an unreadable snapshot) is logged and IGNORED, so
    resolution lands on the true-fingerprint dir and the existing
    missing-artifact behaviour takes over: discovery regenerates,
    train/serve refuse with the command. Serving a stale evaluation after
    a config edit (2026-08-31) is what this test exists to prevent."""
    eval_dir = root / fp
    if not eval_dir.is_dir() and root.is_dir():
        tagged = [
            d for d in root.iterdir() if d.is_dir() and tag_of(d) == model
        ]
        tagged.sort(key=lambda d: d.stat().st_mtime, reverse=True)
        for d in tagged:
            snapshot = d / "config.yaml"
            try:
                snap_fp = snapshot_fp(snapshot)
            except Exception as e:  # noqa: BLE001 — never accept on faith
                logger.warning(
                    "offset.prior %s: candidate %s has no readable config "
                    "snapshot (%s); ignoring", model, d.name, e,
                )
                continue
            if snap_fp != fp:
                logger.warning(
                    "offset.prior %s: %s fingerprints to %s, but the "
                    "evaluation tagged with that stem (%s) is of DIFFERENT "
                    "content (its snapshot fingerprints to %s); ignoring it. "
                    "If the config was edited, regenerate; if a field was "
                    "lost in a copy, restore it.",
                    model, path, fp, d.name, snap_fp,
                )
                continue
            logger.info(
                "offset.prior %s: %s fingerprints to %s (no evaluation "
                "there); using the equivalent evaluation %s",
                model, path, fp, d.name,
            )
            return d.name, d
    return fp, eval_dir


def resolve_prior(
    model: str, config_dirs=None, projection_config_dirs=None,
) -> PriorSource:
    """Config stem -> its config file -> evaluation fingerprint -> eval dir.

    The stem is looked up in the model namespace (models/, models/production/)
    and the projection namespace (projections/); found in both, it is
    refused. Model stems fingerprint via `compute_fingerprint` into
    model_evaluations; projection stems via `compute_iid_fingerprint` into
    projection_evaluations. Either way, a config copy that differs in
    fingerprinted fields falls back to the evaluation tagged with the stem
    in its source.txt, logged.
    """
    proj_path = _find_projection_config(model, projection_config_dirs)
    try:
        model_path = find_prior_config(model, config_dirs)
    except FileNotFoundError:
        if proj_path is None:
            raise
        model_path = None
    if model_path is not None and proj_path is not None:
        raise ValueError(
            f"offset.prior {model!r} exists in both the model namespace "
            f"({model_path}) and the projection namespace ({proj_path}); "
            "rename one — the stem must be unambiguous."
        )

    if proj_path is None:
        from mvp.common.config_hash import compute_fingerprint

        fp = compute_fingerprint(_load_config(model_path), config_path=model_path)
        fp, eval_dir = _tagged_fallback(_evaluations_root(), model, fp, model_path)
        return PriorSource(
            model=model, config_path=model_path, fp=fp, eval_dir=eval_dir
        )

    from mvp.common.config_hash import compute_iid_fingerprint
    from mvp.projection.iid.config import IIDProjectionConfig

    cfg = IIDProjectionConfig.from_file(proj_path)
    fp = compute_iid_fingerprint(cfg, config_path=proj_path)
    fp, eval_dir = _tagged_fallback(
        _projection_evaluations_root(), model, fp, proj_path,
        tag_of=_projection_run_tag, snapshot_fp=_snapshot_fingerprint_projection,
    )
    return PriorSource(
        model=model, config_path=proj_path, fp=fp, eval_dir=eval_dir,
        kind="projection", forward_train_end=cfg.data.date_range.end,
    )


def prior_artifacts_ready(source: PriorSource) -> bool:
    """Fold OOF present with the probability column the kind requires."""
    if source.kind == "projection":
        p = source.fold_match_win
        if not p.exists():
            return False
        cols = pl.scan_parquet(p).collect_schema().names()
        return {"p_match_win_a", "player_id", "opp_id", "won_a"} <= set(cols)
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
    if source.kind == "projection":
        if not regenerate:
            raise FileNotFoundError(
                f"offset.prior {source.model}: no fold_match_win.parquet at "
                f"{source.fold_match_win} (or it predates the current "
                f"columns). Run the projection first: "
                f"{source.regenerate_command}"
            )
        # Same convention as model priors: the discovery driver regenerates,
        # train/serve refuse. The projection's own runner, never
        # ExperimentRunner.
        from mvp.projection.iid.runner import IIDProjectionRunner

        logger.warning(
            "offset.prior %s: no projection artifacts at %s -- regenerating "
            "from %s (a full iid-project run, written where that command "
            "would write it)",
            source.model, source.eval_dir, source.config_path,
        )
        IIDProjectionRunner(config_path=source.config_path).run()
        _cached_frame.cache_clear()
        if not prior_artifacts_ready(source):
            raise RuntimeError(
                f"offset.prior {source.model}: projection ran but "
                f"{source.fold_match_win} is still missing or incomplete"
            )
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


def _both_orientations(df: pl.DataFrame) -> pl.DataFrame:
    """Two rows per match from an A-oriented frame carrying
    (match_uid, player_id, opp_id, day, prior_prob, prior_train_end,
    prior_kind): the A row as-is, the mirror with 1 - p."""
    a = df.select(
        "match_uid", "player_id", "day", "prior_prob",
        "prior_train_end", "prior_kind",
    )
    b = df.select(
        "match_uid",
        pl.col("opp_id").alias("player_id"),
        "day",
        (1.0 - pl.col("prior_prob")).alias("prior_prob"),
        "prior_train_end", "prior_kind",
    )
    return pl.concat([a, b])


def _read_fold_match_win(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    required = {"match_uid", "player_id", "opp_id", "effective_match_date",
                "fold_idx", "p_match_win_a", "won_a"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{path} is missing columns {sorted(missing)}; re-run the "
            "projection to produce the current artifact."
        )
    return df


def _projection_fold_rows(path: Path) -> pl.DataFrame:
    """Walk-forward OOF rows from the projection's fold_match_win artifact,
    NESTED-CALIBRATED and in both orientations.

    The chain output is raw; model priors are nested-Platt-calibrated
    (`y_prob_cal`), and two calibration states are never spliced into one
    column. Same discipline here: fold i's probabilities are transformed by
    a Platt fit on the OTHER folds' rows, so every prior value comes from a
    calibrator that never saw it. Per-fold ``prior_train_end`` by the same
    day.min()-1 rule as the model path."""
    from mvp.model.calibration import PlattCalibrator

    df = _read_fold_match_win(path)
    folds = sorted(df["fold_idx"].unique().to_list())
    if len(folds) < 2:
        raise ValueError(
            f"{path}: nested calibration needs >= 2 folds, found {folds}"
        )
    cal_parts = []
    for f in folds:
        others = df.filter(pl.col("fold_idx") != f)
        mine = df.filter(pl.col("fold_idx") == f)
        cal = PlattCalibrator()
        try:
            cal.fit(
                others["p_match_win_a"].to_numpy(),
                others["won_a"].to_numpy().astype(np.int64),
            )
        except ValueError as e:
            # A raw-probability fallback would splice calibration states into
            # one column, so a degenerate complement is refused, not papered
            # over.
            raise ValueError(
                f"{path}: cannot nested-calibrate fold {f} — the other "
                f"folds' outcomes are single-class ({e}). The projection's "
                "folds are too thin for a calibrated prior."
            ) from e
        cal_parts.append(mine.with_columns(
            pl.Series("prior_prob", cal.transform(mine["p_match_win_a"].to_numpy()))
        ))
    out = pl.concat(cal_parts).with_columns(
        pl.col("effective_match_date").cast(pl.Date).alias("day"),
        pl.lit("proj_fold_oof_nested_cal").alias("prior_kind"),
    )
    train_end = out.group_by("fold_idx").agg(
        (pl.col("day").min() - timedelta(days=1)).alias("prior_train_end")
    )
    return _both_orientations(out.join(train_end, on="fold_idx"))


def _projection_forward_rows(
    pmf_path: Path, fold_path: Path, train_end: date, model: str
) -> pl.DataFrame | None:
    """Forward rows from the projection's live pmf artifact (one fit through
    ``train_end``), calibrated by a global Platt fit on ALL walk-forward OOF
    rows — the fit ends at ``train_end`` and these rows are dated after it,
    mirroring the model path's post-Platt backtest rows. Both orientations.
    None (with a log) when the pmf predates the id columns."""
    from mvp.model.calibration import PlattCalibrator

    cols = pl.scan_parquet(pmf_path).collect_schema().names()
    if not {"player_id", "opp_id", "p_match_win_a"} <= set(cols):
        logger.warning(
            "prior %s: %s lacks player_id/opp_id (predates the id columns); "
            "using fold OOF only — re-run iid-backtest for forward rows",
            model, pmf_path,
        )
        return None
    oof = _read_fold_match_win(fold_path)
    cal = PlattCalibrator()
    try:
        cal.fit(
            oof["p_match_win_a"].to_numpy(),
            oof["won_a"].to_numpy().astype(np.int64),
        )
    except ValueError as e:
        raise ValueError(
            f"{fold_path}: cannot calibrate forward rows — the walk-forward "
            f"outcomes are single-class ({e})."
        ) from e
    df = pl.read_parquet(pmf_path).select(
        "match_uid", "player_id", "opp_id",
        pl.col("effective_match_date").cast(pl.Date).alias("day")
        if "effective_match_date" in cols else pl.lit(None, dtype=pl.Date).alias("day"),
        pl.col("p_match_win_a"),
    )
    if df["day"].null_count():
        raise ValueError(
            f"prior {model}: {pmf_path} rows lack effective_match_date; cannot "
            "date the forward splice"
        )
    return _both_orientations(df.with_columns(
        pl.Series("prior_prob", cal.transform(df["p_match_win_a"].to_numpy())),
        pl.lit(train_end).alias("prior_train_end"),
        pl.lit("proj_forward_cal").alias("prior_kind"),
    ))


def build_prior_frame(
    source: PriorSource, backtests_root: Path | None = None
) -> pl.DataFrame:
    """One row per (match_uid, player_id) the base model scored out of sample:
    ``prior_prob``, ``prior_logit``, ``prior_train_end``, ``prior_kind``.
    Fold OOF spliced with the walk-forward/forward rows; refuses overlaps,
    duplicates and any row dated on/before its own train end."""
    if source.kind == "projection":
        parts = [_projection_fold_rows(source.fold_match_win)]
        if source.pmf_parquet.exists():
            fwd = _projection_forward_rows(
                source.pmf_parquet, source.fold_match_win,
                source.forward_train_end, source.model,
            )
            if fwd is not None:
                parts.append(fwd)
        return _splice_and_finalize(parts, source)
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
    return _splice_and_finalize(parts, source)


def _splice_and_finalize(
    parts: list[pl.DataFrame], source: PriorSource
) -> pl.DataFrame:
    if len(parts) == 2:
        overlap = parts[0].join(parts[1], on=["match_uid", "player_id"], how="inner").height
        if overlap:
            raise ValueError(
                f"prior {source.model}: {overlap} (match, player) rows in both "
                "the fold OOF and the backtest/forward rows; refusing to splice"
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
