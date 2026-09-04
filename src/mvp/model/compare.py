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
import re
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

from mvp.common.base_job import get_data_root, get_tuning_state_dir

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


def _side_form(spec: str) -> str:
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    if all(
        len(p) == 12 and all(c in "0123456789abcdef" for c in p) for p in parts
    ):
        return "fps"
    return "tag"


def resolve_sides(
    spec_a: str, spec_b: str, top: int | None = None,
) -> tuple[list[Path], list[Path]]:
    """Resolve both sides with ONE input form: both tags or both fingerprint
    lists — a mixed run compares a curated subset against an enumeration,
    which is never a like-for-like question. --top therefore requires tag
    form on both sides."""
    form_a, form_b = _side_form(spec_a), _side_form(spec_b)
    if form_a != form_b:
        raise ValueError(
            f"mixed side forms (a={form_a}, b={form_b}): pass both sides as "
            "tags or both as fingerprint lists"
        )
    return resolve_side(spec_a, top=top), resolve_side(spec_b, top=top)


def resolve_side(spec: str, top: int | None = None) -> list[Path]:
    """A side is a comma-separated fingerprint list or a source tag
    (best-effort: sweep trials are tag-patched only where
    frozen_backtest_sweep --report ran, so the match count is logged and an
    empty match is refused rather than silently compared as nothing).

    ``top``: tag sides only — keep the tag's top-N trials by the tune
    study's own ranking (see `top_trial_dirs`), so a family subset never
    means hand-pasting fingerprints."""
    root = _evaluations_root()
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    if _side_form(spec) == "fps":
        if top is not None:
            raise ValueError(
                "top applies to tag sides; a fingerprint list IS the subset"
            )
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
    if top is not None:
        dirs = top_trial_dirs(tag, dirs, top)
        logger.info(
            "tag %r: top %d by tune ranking -> %s",
            tag, len(dirs), [d.name for d in dirs],
        )
    return dirs


def _trial_number_of(eval_dir: Path, tag: str) -> int | None:
    src = eval_dir / "source.txt"
    if not src.exists():
        return None
    m = re.search(
        rf"{re.escape(tag)}__h\d+_t(\d+)", src.read_text(encoding="utf-8")
    )
    return int(m.group(1)) if m else None


def top_trial_dirs(tag: str, dirs: list[Path], n: int) -> list[Path]:
    """The tag's top-n evaluation dirs by the tune study's own ranking key —
    the SAME ordering tune-review and frozen_backtest_sweep --select topn
    use (resolve_sort_keys/sort_trials), so "top" cannot drift between
    tools. Dirs map to trials via the __hNN_tNN tag the sweep writes."""
    import optuna

    from mvp.model.tune_review import resolve_sort_keys, sort_trials

    db = get_tuning_state_dir() / f"{tag}.db"
    if not db.exists():
        raise FileNotFoundError(
            f"--top for side {tag!r} needs its tune study at {db}; pass "
            "fingerprints instead for an untuned family"
        )
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    storage = f"sqlite:///{db}"
    names = [s.study_name for s in optuna.study.get_all_study_summaries(storage)]
    study_name = tag if tag in names else names[0] if len(names) == 1 else None
    if study_name is None:
        raise ValueError(f"{db} holds studies {names}; none matches {tag!r}")
    study = optuna.load_study(study_name=study_name, storage=storage)
    done = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    ranked = sort_trials(done, resolve_sort_keys(study, done, None))
    by_trial: dict[int, Path] = {}
    for d in dirs:
        num = _trial_number_of(d, tag)
        if num is not None and num not in by_trial:
            by_trial[num] = d
    out: list[Path] = []
    for t in ranked:
        d = by_trial.get(t.number)
        if d is not None:
            out.append(d)
            if len(out) == n:
                break
    if not out:
        raise ValueError(
            f"tag {tag!r}: no evaluation dir maps to a tuned trial (dirs "
            "carry __hNN_tNN tags only when the frozen sweep reported them)"
        )
    if len(out) < n:
        logger.warning(
            "tag %r: only %d of the requested top %d have evaluation dirs",
            tag, len(out), n,
        )
    return out


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
    *, column: str = "y_prob_cal", min_overlap: float = 0.5,
    reps: int = 2000, seed: int = 0,
) -> dict:
    """Side-vs-side verdict: trial-pair delta matrix, envelope, and
    family-mean CIs — pooled AND per segment cut (half-year, circuit).

    One interface, three usages: family vs family (both sides multi-trial),
    a head-to-head (one fingerprint per side — the 1x1 matrix's family
    verdict IS the pair verdict, cuts included), and within-family (both
    sides drawn from the same family's trials). There is no "picked
    deployable" concept: nominating one cell of a matrix you have already
    seen and reading its interval as pre-specified is the fallacy this tool
    replaces — run the 1v1 form for a head-to-head, knowing its interval is
    not corrected for how you chose the two."""
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

    # Family-mean CIs, pooled and per cut: one week-resampling per rep
    # applied to EVERY pair, so intervals reflect sampling noise of the
    # mean-over-pairs while trial-choice variance stays visible in the
    # matrix/envelope. Vectorized as (pairs x weeks) sum/count matrices per
    # cut label — a pair with no data in a drawn week contributes nothing
    # that rep, and drops out of that rep entirely at zero coverage; the
    # min per-pair block count per cut keeps that thinness visible.
    all_weeks: set[int] = set()
    per_pair_rows = []  # (weeks array, delta array, {label: mask})
    for k in matrix:
        fa, fb = frames_a[k[0]], frames_b[k[1]]
        j = fa.join(fb, on=["match_uid", "player_id"], how="inner", suffix="_b")
        w = _iso_week(j["day"])
        d = (j["loss"] - j["loss_b"]).to_numpy()
        labels: dict[str, np.ndarray] = {}
        hy = _half_year(j["day"])
        for lab in np.unique(hy):
            labels[str(lab)] = hy == lab
        if "circuit" in j.columns:
            circ = j["circuit"].to_numpy()
            for lab in np.unique(circ):
                labels[str(lab)] = circ == lab
        per_pair_rows.append((w, d, labels))
        all_weeks.update(np.unique(w).tolist())
    weeks_u = np.array(sorted(all_weeks))
    n_w, n_p = len(weeks_u), len(per_pair_rows)
    all_labels = sorted({lab for _, _, ls in per_pair_rows for lab in ls})

    def _matrices(label: str | None) -> tuple[np.ndarray, np.ndarray]:
        S = np.zeros((n_p, n_w))
        C = np.zeros((n_p, n_w))
        for pi, (w, d, ls) in enumerate(per_pair_rows):
            if label is None:
                m = np.ones(len(d), dtype=bool)
            else:
                m = ls.get(label, np.zeros(len(d), dtype=bool))
            idx = np.searchsorted(weeks_u, w[m])
            np.add.at(S[pi], idx, d[m])
            np.add.at(C[pi], idx, 1)
        return S, C

    rng = np.random.default_rng(seed)
    draws = rng.integers(0, n_w, size=(reps, n_w))

    def _fam_stats(S: np.ndarray, C: np.ndarray) -> tuple[float, float, float, int]:
        with np.errstate(invalid="ignore"):
            point = float(np.nanmean(
                np.where(C.sum(axis=1) > 0, S.sum(axis=1) / C.sum(axis=1), np.nan)
            ))
        means = []
        for r in range(reps):
            s = S[:, draws[r]].sum(axis=1)
            c = C[:, draws[r]].sum(axis=1)
            ok_p = c > 0
            if ok_p.any():
                means.append(float(np.mean(s[ok_p] / c[ok_p])))
        lo, hi = np.percentile(means, [2.5, 97.5])
        n_blocks = int(min((C[pi] > 0).sum() for pi in range(n_p)))
        return point, float(lo), float(hi), n_blocks

    fam_mean, fam_lo, fam_hi, fam_blocks = _fam_stats(*_matrices(None))
    family_cuts: dict[str, tuple[float, float, float, int]] = {}
    for lab in all_labels:
        family_cuts[lab] = _fam_stats(*_matrices(lab))

    return {
        "column": column,
        "n_trials": (len(dirs_a), len(dirs_b)),
        "matrix": matrix,
        "refused": refused,
        "envelope": env,
        "family_mean": fam_mean,
        "family_ci": (fam_lo, fam_hi),
        "family_n_blocks": fam_blocks,
        "family_cuts": family_cuts,
    }
