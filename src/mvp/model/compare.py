"""Paired family comparison: block-bootstrap loss deltas on shared matches.

Family rankings were read off fold-metric differences of 0.001-0.003 with no
uncertainty attached, while per-fold noise is the same order. This module
answers "is A better than B?" the honest way: per-match paired loss deltas on
the intersection of what both sides scored (pairing cancels shared difficulty),
with a week-block bootstrap for the interval (rows are not independent --
same-event matches share surface, conditions, players; tennis events are
week-aligned, so ISO-week blocks approximate tournament blocks without a join).

Two levels, because an evaluation is one TRIAL from a FAMILY (a sweep's
variants): the family verdict is the trial-pair delta matrix + envelope +
family-mean interval; the deployable verdict is one picked pair. Trial-choice
variance is shown via the matrix rather than folded into one interval.

Orientation collapse: a `won`-target frame carries each match as two mirrored
rows whose per-row losses are identical by construction (symmetry.py forces
complementary probabilities). Frames are collapsed to ONE row per match after
verifying that invariant, so counts are matches and the intersection can never
keep one orientation of a match and drop the other.

Plan: mvp-docs/plans/2026-09-01-paired-family-comparison.md (rev 3).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

from mvp.common.base_job import get_data_root

logger = logging.getLogger(__name__)

# Test seam, consulted at call time (None = the data root's model_evaluations).
EVALUATIONS_ROOT: Path | None = None

_EPS = 1e-15
_REQUIRED = ["match_uid", "player_id", "effective_match_date", "y_test"]
# Percentile bootstrap under-covers at small block counts; cuts resting on
# fewer blocks than this carry a LOW-BLOCKS flag (a caveat, not a suppression).
LOW_BLOCKS = 30
# Orientation symmetry: mirrored rows' losses match to float64 rounding
# (measured max deviation ~2e-15 on live artifacts); anything beyond this is a
# frame that violates the invariant, not numerical noise.
_SYMMETRY_TOL = 1e-9


def _evaluations_root() -> Path:
    return EVALUATIONS_ROOT or (get_data_root() / "model_evaluations")


def _source_tags(eval_dir: Path) -> set[str]:
    src = eval_dir / "source.txt"
    if not src.exists():
        return set()
    return {
        ln.split("\t")[0].strip()
        for ln in src.read_text(encoding="utf-8").splitlines()
        if ln.strip()
    }


def resolve_side(spec: str) -> list[Path]:
    """A side is a comma-separated fingerprint list (primary input) or a
    source tag (best-effort: sweep trials are tag-patched only where
    frozen_backtest_sweep --report ran, so the match count is logged and an
    empty match is refused rather than silently compared as nothing)."""
    root = _evaluations_root()
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    if all(len(p) == 12 and all(c in "0123456789abcdef" for c in p) for p in parts):
        dirs = [root / p for p in parts]
        missing = [d.name for d in dirs if not d.is_dir()]
        if missing:
            raise FileNotFoundError(
                f"no evaluation dir for fingerprint(s) {missing} under {root}"
            )
        return dirs
    if len(parts) != 1:
        raise ValueError(
            f"{spec!r}: mixed or malformed side -- pass 12-hex fingerprints "
            "(comma-separated) or a single source tag"
        )
    tag = parts[0]
    dirs = sorted(
        (d for d in root.iterdir() if d.is_dir() and tag in _source_tags(d)),
        key=lambda d: d.stat().st_mtime,
    )
    logger.info("tag %r matched %d evaluation dir(s)", tag, len(dirs))
    if not dirs:
        raise FileNotFoundError(
            f"tag {tag!r} matched no evaluation dirs under {root}. "
            "Classification sweep trials carry temp-stem tags unless "
            "frozen_backtest_sweep --report patched them; pass fingerprints."
        )
    return dirs


def load_eval_predictions(fp_dir: Path, column: str) -> tuple[pl.DataFrame, str]:
    """One evaluation's per-row OOF predictions plus its GRAIN.

    Requires `column` (no fallback: mixed calibration states are never
    compared). Verifies the orientation-symmetry invariant, then keeps the
    lower-sorting player_id row per match -> ("match" grain). A frame
    violating the invariant is kept two-sided with a warning ("row" grain),
    never silently averaged — and the grain is returned so downstream counts
    and joins can never mislabel rows as matches."""
    path = fp_dir / "fold_predictions.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{fp_dir.name}: no fold_predictions.parquet")
    df = pl.read_parquet(path)
    missing = [c for c in [*_REQUIRED, column] if c not in df.columns]
    if missing:
        raise ValueError(
            f"{fp_dir.name}: fold_predictions.parquet lacks {missing} "
            f"(columns: {df.columns})"
        )
    p = df[column].to_numpy().astype(np.float64).clip(_EPS, 1 - _EPS)
    y = df["y_test"].to_numpy().astype(np.float64)
    df = df.with_columns(
        pl.Series("loss", -(y * np.log(p) + (1 - y) * np.log(1 - p))),
        pl.Series("brier", (p - y) ** 2),
        pl.col("effective_match_date").cast(pl.Date).alias("day"),
    )
    spread = (
        df.group_by("match_uid")
        .agg((pl.col("loss").max() - pl.col("loss").min()).alias("d"))["d"]
        .max()
    )
    if spread is not None and spread > _SYMMETRY_TOL:
        logger.warning(
            "%s: orientation losses differ up to %.3g (> %.0e); keeping both "
            "rows -- counts are ROWS for this side, not matches",
            fp_dir.name, spread, _SYMMETRY_TOL,
        )
        return df, "row"
    return df.sort("player_id").unique(subset=["match_uid"], keep="first"), "match"


@dataclass
class PairResult:
    fp_a: str
    fp_b: str
    grain: str  # "match", or "row" when both sides violated the invariant
    n_matches: int
    keep_a: float
    keep_b: float
    unmatched_a: int
    unmatched_b: int
    delta_ll: float
    ci: tuple[float, float]
    n_blocks: int
    delta_brier: float
    cuts: dict[str, tuple[float, float, float, int]] = field(default_factory=dict)
    # cuts: label -> (delta, lo, hi, n_blocks)


def block_bootstrap_ci(
    delta: np.ndarray, weeks: np.ndarray, *, reps: int = 2000, seed: int = 0,
) -> tuple[float, float, int]:
    """CI of the mean delta, resampling WHOLE ISO-week blocks with
    replacement. Never rows within a week -- whole-block resampling is what
    absorbs within-match/within-event correlation."""
    uniq = np.unique(weeks)
    sums = np.zeros(len(uniq))
    counts = np.zeros(len(uniq))
    idx = np.searchsorted(uniq, weeks)
    np.add.at(sums, idx, delta)
    np.add.at(counts, idx, 1)
    if len(uniq) == 1:
        # A one-block "interval" is a point; say so rather than print it
        # silently (the exact failure this tool exists to end).
        logger.warning(
            "bootstrap over a SINGLE block: the interval is a point, not an "
            "uncertainty estimate"
        )
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(uniq), size=(reps, len(uniq)))
    means = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    lo, hi = np.percentile(means, [2.5, 97.5])
    return float(lo), float(hi), len(uniq)


def _iso_week(days: pl.Series) -> np.ndarray:
    df = pl.DataFrame({"d": days})
    return (
        df.select(
            (pl.col("d").dt.iso_year() * 100 + pl.col("d").dt.week()).alias("w")
        )["w"].to_numpy()
    )


def _half_year(days: pl.Series) -> np.ndarray:
    df = pl.DataFrame({"d": days})
    return df.select(
        (
            pl.lit("H") + ((pl.col("d").dt.month() > 6).cast(pl.Int8) + 1).cast(pl.Utf8)
            + pl.lit("'") + (pl.col("d").dt.year() % 100).cast(pl.Utf8)
        ).alias("h")
    )["h"].to_numpy()


def compare_pair(
    df_a: pl.DataFrame, df_b: pl.DataFrame, fp_a: str, fp_b: str,
    *, grain_a: str = "match", grain_b: str = "match",
    min_overlap: float = 0.5, reps: int = 2000, seed: int = 0,
) -> PairResult:
    """Paired read for one trial pair on the intersection of their matches.

    Mixed grain is REFUSED: joining a collapsed side to a two-row side halves
    the two-row side's apparent keep-share and doubles its unmatched count —
    numbers that would read as population mismatch when they are an artifact."""
    if grain_a != grain_b:
        raise ValueError(
            f"{fp_a} vs {fp_b}: mixed grain ({grain_a} vs {grain_b}) -- one "
            "side violated the orientation-symmetry invariant and was kept "
            "two-sided. Investigate that side before comparing."
        )
    joined = df_a.join(df_b, on=["match_uid", "player_id"], how="inner", suffix="_b")
    n = joined.height
    keep_a, keep_b = n / max(df_a.height, 1), n / max(df_b.height, 1)
    if keep_a < min_overlap or keep_b < min_overlap:
        raise ValueError(
            f"{fp_a} vs {fp_b}: intersection keeps a={keep_a:.2f}, "
            f"b={keep_b:.2f} of matches (min_overlap={min_overlap}). The "
            "populations differ too much to compare implicitly -- resolve "
            "the population question first or lower --min-overlap knowingly."
        )
    delta = (joined["loss"] - joined["loss_b"]).to_numpy()
    weeks = _iso_week(joined["day"])
    lo, hi, nb = block_bootstrap_ci(delta, weeks, reps=reps, seed=seed)
    res = PairResult(
        fp_a=fp_a, fp_b=fp_b, grain=grain_a,
        n_matches=n, keep_a=keep_a, keep_b=keep_b,
        unmatched_a=df_a.height - n, unmatched_b=df_b.height - n,
        delta_ll=float(delta.mean()), ci=(lo, hi), n_blocks=nb,
        delta_brier=float((joined["brier"] - joined["brier_b"]).mean()),
    )
    for labels in (_half_year(joined["day"]),) + (
        (joined["circuit"].to_numpy(),) if "circuit" in joined.columns else ()
    ):
        for lab in np.unique(labels):
            m = labels == lab
            clo, chi, cnb = block_bootstrap_ci(delta[m], weeks[m], reps=reps, seed=seed)
            res.cuts[str(lab)] = (float(delta[m].mean()), clo, chi, cnb)
    return res


def compare_families(
    dirs_a: list[Path], dirs_b: list[Path],
    *, column: str = "y_prob_cal", pick_a: str | None = None,
    pick_b: str | None = None, min_overlap: float = 0.5,
    reps: int = 2000, seed: int = 0,
) -> dict:
    """Family verdict (delta matrix + envelope + family-mean CI) and
    deployable verdict (one picked pair, default: newest trial per side).

    The deployable interval is NOT corrected for post-hoc selection from the
    matrix; picking the standout cell and reading its CI as pre-specified is
    the fallacy this tool replaces, relocated -- the report flags when the
    picked pair IS the grid extreme."""
    loaded_a = {d.name: load_eval_predictions(d, column) for d in dirs_a}
    loaded_b = {d.name: load_eval_predictions(d, column) for d in dirs_b}
    frames_a = {k: df for k, (df, _) in loaded_a.items()}
    frames_b = {k: df for k, (df, _) in loaded_b.items()}
    grains_a = {k: g for k, (_, g) in loaded_a.items()}
    grains_b = {k: g for k, (_, g) in loaded_b.items()}

    matrix: dict[tuple[str, str], PairResult] = {}
    refused: dict[tuple[str, str], str] = {}
    for na, fa in frames_a.items():
        for nb_, fb in frames_b.items():
            try:
                matrix[(na, nb_)] = compare_pair(
                    fa, fb, na, nb_,
                    grain_a=grains_a[na], grain_b=grains_b[nb_],
                    min_overlap=min_overlap, reps=reps, seed=seed,
                )
            except ValueError as e:
                # One incomparable cell (a population-filter sweep variant,
                # a grain violation) must not abort the healthy cells.
                refused[(na, nb_)] = str(e)
    if not matrix:
        raise ValueError(
            "every trial pair was refused:\n  "
            + "\n  ".join(f"{a} vs {b}: {m}" for (a, b), m in refused.items())
        )
    deltas = {k: r.delta_ll for k, r in matrix.items()}
    env = (min(deltas.values()), max(deltas.values()))

    # Family-mean CI: one week-resampling per rep applied to EVERY pair, so
    # the interval reflects sampling noise of the mean-over-pairs, while
    # trial-choice variance stays visible in the matrix/envelope.
    # Pairs missing a drawn week contribute nothing to that rep, and a pair
    # matching zero drawn weeks drops out of that rep entirely — sparse-week
    # pairs therefore fluctuate in how much they weigh the family mean. The
    # min per-pair block count is reported so that thinness is visible.
    all_weeks: set[int] = set()
    pair_tables = {}
    for k, res in matrix.items():
        fa, fb = frames_a[k[0]], frames_b[k[1]]
        j = fa.join(fb, on=["match_uid", "player_id"], how="inner", suffix="_b")
        w = _iso_week(j["day"])
        d = (j["loss"] - j["loss_b"]).to_numpy()
        uniq = np.unique(w)
        sums = np.zeros(len(uniq))
        counts = np.zeros(len(uniq))
        idx = np.searchsorted(uniq, w)
        np.add.at(sums, idx, d)
        np.add.at(counts, idx, 1)
        pair_tables[k] = dict(zip(uniq.tolist(), zip(sums, counts)))
        all_weeks.update(uniq.tolist())
    weeks_u = sorted(all_weeks)
    rng = np.random.default_rng(seed)
    fam_means = []
    for _ in range(reps):
        chosen = rng.integers(0, len(weeks_u), size=len(weeks_u))
        pair_means = []
        for tab in pair_tables.values():
            s = c = 0.0
            for i in chosen:
                ent = tab.get(weeks_u[i])
                if ent:
                    s += ent[0]
                    c += ent[1]
            if c:
                pair_means.append(s / c)
        if pair_means:
            fam_means.append(float(np.mean(pair_means)))
    fam_lo, fam_hi = np.percentile(fam_means, [2.5, 97.5])

    def _newest(dirs: list[Path]) -> str:
        return max(dirs, key=lambda d: d.stat().st_mtime).name

    pa = pick_a or _newest(dirs_a)
    pb = pick_b or _newest(dirs_b)
    if pa not in frames_a or pb not in frames_b:
        raise ValueError(
            f"pick ({pa}, {pb}) is not among the loaded trials "
            f"(a: {sorted(frames_a)}, b: {sorted(frames_b)})"
        )
    if (pa, pb) not in matrix:
        raise ValueError(
            f"picked pair {pa} vs {pb} was refused: {refused[(pa, pb)]}"
        )
    deploy = matrix[(pa, pb)]
    extreme = (deploy.delta_ll in env) and len(matrix) > 1
    return {
        "column": column,
        "n_trials": (len(dirs_a), len(dirs_b)),
        "matrix": matrix,
        "refused": refused,
        "envelope": env,
        "family_mean": float(np.mean(list(deltas.values()))),
        "family_ci": (float(fam_lo), float(fam_hi)),
        "family_n_blocks": min(len(t) for t in pair_tables.values()),
        "deployable": deploy,
        "picked_is_extreme": extreme,
    }
