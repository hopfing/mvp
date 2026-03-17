"""Voter analysis — correlation, coverage curves, and marginal value."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import polars as pl

from mvp.model.confidence.metrics import ReliabilityProfile, compute_reliability_profile

logger = logging.getLogger(__name__)


# --- Voter Correlation ---


@dataclass(frozen=True)
class VoterPairStats:
    """Agreement statistics for a pair of voters."""

    agreement_pct: float  # % of matches where both pick the same side
    n_overlap: int  # matches where both have non-null predictions
    # When they disagree, who's right more often (fraction a wins)
    disagree_a_correct_pct: float | None
    disagree_b_correct_pct: float | None
    n_disagree: int


@dataclass
class VoterCorrelationResult:
    """Pairwise agreement matrix for all voters."""

    pairs: dict[tuple[str, str], VoterPairStats] = field(default_factory=dict)
    voter_names: list[str] = field(default_factory=list)


def compute_voter_correlation(
    oof_df: pl.DataFrame, voter_names: list[str]
) -> VoterCorrelationResult:
    """Compute binary pick agreement rate for each pair of voters.

    Includes "primary" as a voter alongside the named voters.
    """
    result = VoterCorrelationResult(voter_names=["primary"] + voter_names)

    # Build dict of binary picks per voter: name -> (picks, non_null_mask)
    picks: dict[str, tuple[np.ndarray, np.ndarray]] = {}

    primary_prob = oof_df["y_prob"].to_numpy()
    primary_binary = (primary_prob >= 0.5).astype(int)
    primary_mask = np.ones(len(oof_df), dtype=bool)
    picks["primary"] = (primary_binary, primary_mask)

    y_true = oof_df["y_true"].to_numpy().astype(int)

    for name in voter_names:
        col = f"_voter_{name}"
        if col not in oof_df.columns:
            continue
        arr = oof_df[col].to_numpy()
        non_null = ~np.isnan(arr.astype(float))
        binary = np.where(non_null, (arr >= 0.5).astype(int), 0)
        picks[name] = (binary, non_null)

    all_names = result.voter_names
    for i, a in enumerate(all_names):
        if a not in picks:
            continue
        for j, b in enumerate(all_names):
            if j <= i or b not in picks:
                continue

            a_binary, a_mask = picks[a]
            b_binary, b_mask = picks[b]
            overlap = a_mask & b_mask
            n_overlap = int(overlap.sum())
            if n_overlap == 0:
                continue

            same = (a_binary[overlap] == b_binary[overlap])
            agreement_pct = float(same.mean()) * 100

            disagree_mask = ~same
            n_disagree = int(disagree_mask.sum())
            if n_disagree > 0:
                # Among disagreements, who predicted the correct side?
                overlap_idx = np.where(overlap)[0]
                disagree_idx = overlap_idx[disagree_mask]
                a_correct = (a_binary[disagree_idx] == y_true[disagree_idx]).sum()
                b_correct = (b_binary[disagree_idx] == y_true[disagree_idx]).sum()
                disagree_a_pct = float(a_correct / n_disagree) * 100
                disagree_b_pct = float(b_correct / n_disagree) * 100
            else:
                disagree_a_pct = None
                disagree_b_pct = None

            result.pairs[(a, b)] = VoterPairStats(
                agreement_pct=agreement_pct,
                n_overlap=n_overlap,
                disagree_a_correct_pct=disagree_a_pct,
                disagree_b_correct_pct=disagree_b_pct,
                n_disagree=n_disagree,
            )

    return result


# --- Coverage Curve ---


@dataclass(frozen=True)
class CoveragePoint:
    """Metrics at a single consensus threshold."""

    threshold_pct: int  # e.g. 100, 90, 80, ...
    n_matches: int
    coverage_pct: float  # % of total matches
    profile: ReliabilityProfile


@dataclass
class CoverageCurveResult:
    """Coverage vs quality at different consensus thresholds."""

    points: list[CoveragePoint] = field(default_factory=list)
    n_total: int = 0


def compute_coverage_curve(
    oof_df: pl.DataFrame, voter_names: list[str]
) -> CoverageCurveResult:
    """Compute coverage and quality metrics at consensus thresholds.

    Parses voter_consensus "N-M" to get agree_pct = N/(N+M).
    """
    if "voter_consensus" not in oof_df.columns:
        return CoverageCurveResult()

    n_total = len(oof_df)
    result = CoverageCurveResult(n_total=n_total)

    # Parse consensus into agree_pct per match
    oof_df = oof_df.with_columns(
        pl.col("voter_consensus").str.split("-").list.get(0).cast(pl.Int64).alias("_agree"),
        pl.col("voter_consensus").str.split("-").list.get(1).cast(pl.Int64).alias("_disagree"),
    ).with_columns(
        (pl.col("_agree").cast(pl.Float64) / (pl.col("_agree") + pl.col("_disagree")).cast(pl.Float64) * 100)
        .alias("_agree_pct")
    )

    for threshold in [100, 90, 80, 70, 60, 50]:
        filtered = oof_df.filter(pl.col("_agree_pct") >= threshold)
        n = len(filtered)
        if n == 0:
            continue
        coverage_pct = n / n_total * 100
        profile = compute_reliability_profile(filtered)
        result.points.append(CoveragePoint(
            threshold_pct=threshold,
            n_matches=n,
            coverage_pct=coverage_pct,
            profile=profile,
        ))

    # Clean up temp columns
    return result


# --- Voter Marginal Value ---


@dataclass(frozen=True)
class VoterMarginalStats:
    """Leave-one-out impact of a single voter."""

    name: str
    scope_pct: float  # % of matches where this voter has a vote
    # At 100% consensus threshold
    cov_delta_100: float  # coverage change when removing this voter
    acc_delta_100: float
    cal_delta_100: float
    err80_delta_100: float
    # At 80% consensus threshold
    cov_delta_80: float
    acc_delta_80: float
    cal_delta_80: float
    err80_delta_80: float


@dataclass
class VoterMarginalResult:
    """Leave-one-out analysis for each voter."""

    voters: list[VoterMarginalStats] = field(default_factory=list)
    baseline_cov_100: float = 0.0
    baseline_acc_100: float = 0.0
    baseline_cov_80: float = 0.0
    baseline_acc_80: float = 0.0


def compute_voter_marginal_value(
    oof_df: pl.DataFrame, voter_names: list[str]
) -> VoterMarginalResult:
    """Compute leave-one-out impact of each voter on consensus filtering."""
    if "voter_consensus" not in oof_df.columns:
        return VoterMarginalResult()

    n_total = len(oof_df)
    if n_total == 0:
        return VoterMarginalResult()

    # Parse existing consensus
    oof_df = oof_df.with_columns(
        pl.col("voter_consensus").str.split("-").list.get(0).cast(pl.Int64).alias("_agree"),
        pl.col("voter_consensus").str.split("-").list.get(1).cast(pl.Int64).alias("_disagree"),
    ).with_columns(
        (pl.col("_agree") + pl.col("_disagree")).alias("_total"),
    ).with_columns(
        (pl.col("_agree").cast(pl.Float64) / pl.col("_total").cast(pl.Float64) * 100)
        .alias("_agree_pct")
    )

    # Baseline metrics at 100% and 80% thresholds
    baseline_100 = _threshold_metrics(oof_df, 100, n_total)
    baseline_80 = _threshold_metrics(oof_df, 80, n_total)

    result = VoterMarginalResult(
        baseline_cov_100=baseline_100[0],
        baseline_acc_100=baseline_100[1],
        baseline_cov_80=baseline_80[0],
        baseline_acc_80=baseline_80[1],
    )

    # Primary model's binary pick (used to determine if voter agreed or disagreed)
    primary_pick = (oof_df["y_prob"].to_numpy() >= 0.5).astype(int)

    for name in voter_names:
        col = f"_voter_{name}"
        if col not in oof_df.columns:
            result.voters.append(VoterMarginalStats(
                name=name, scope_pct=0.0,
                cov_delta_100=0.0, acc_delta_100=0.0,
                cal_delta_100=0.0, err80_delta_100=0.0,
                cov_delta_80=0.0, acc_delta_80=0.0,
                cal_delta_80=0.0, err80_delta_80=0.0,
            ))
            continue

        voter_probs = oof_df[col].to_numpy().astype(float)
        has_vote = ~np.isnan(voter_probs)
        voter_binary = (voter_probs >= 0.5).astype(int)
        voter_agreed = voter_binary == primary_pick

        scope_pct = float(has_vote.sum()) / n_total * 100

        # Recompute consensus without this voter
        agree = oof_df["_agree"].to_numpy().copy()
        total = oof_df["_total"].to_numpy().copy()

        new_agree = agree.copy()
        new_total = total.copy()

        # Where voter has a vote: remove their contribution
        voted_mask = has_vote
        agreed_mask = voted_mask & voter_agreed
        disagreed_mask = voted_mask & ~voter_agreed

        new_total[voted_mask] -= 1
        new_agree[agreed_mask] -= 1
        # disagreed voters: agree stays same, total decreases (already done)

        # Compute new agree_pct (avoid div by zero for total=0)
        with np.errstate(divide="ignore", invalid="ignore"):
            new_agree_pct = np.where(new_total > 0, new_agree / new_total * 100, 100)

        oof_with_new = oof_df.with_columns(
            pl.Series("_new_agree_pct", new_agree_pct)
        )

        # Metrics without this voter
        without_100 = _threshold_metrics_series(oof_with_new, "_new_agree_pct", 100, n_total)
        without_80 = _threshold_metrics_series(oof_with_new, "_new_agree_pct", 80, n_total)

        result.voters.append(VoterMarginalStats(
            name=name,
            scope_pct=scope_pct,
            cov_delta_100=without_100[0] - baseline_100[0],
            acc_delta_100=without_100[1] - baseline_100[1],
            cal_delta_100=without_100[2] - baseline_100[2],
            err80_delta_100=without_100[3] - baseline_100[3],
            cov_delta_80=without_80[0] - baseline_80[0],
            acc_delta_80=without_80[1] - baseline_80[1],
            cal_delta_80=without_80[2] - baseline_80[2],
            err80_delta_80=without_80[3] - baseline_80[3],
        ))

    return result


def _threshold_metrics(
    df: pl.DataFrame, threshold: int, n_total: int
) -> tuple[float, float, float, float]:
    """Return (coverage_pct, accuracy, signed_cal, err80) at a consensus threshold."""
    filtered = df.filter(pl.col("_agree_pct") >= threshold)
    n = len(filtered)
    if n == 0:
        return (0.0, 0.0, 0.0, 0.0)
    profile = compute_reliability_profile(filtered)
    return (n / n_total * 100, profile.accuracy, profile.signed_cal, profile.err80)


def _threshold_metrics_series(
    df: pl.DataFrame, pct_col: str, threshold: int, n_total: int
) -> tuple[float, float, float, float]:
    """Return (coverage_pct, accuracy, signed_cal, err80) using a custom pct column."""
    filtered = df.filter(pl.col(pct_col) >= threshold)
    n = len(filtered)
    if n == 0:
        return (0.0, 0.0, 0.0, 0.0)
    profile = compute_reliability_profile(filtered)
    return (n / n_total * 100, profile.accuracy, profile.signed_cal, profile.err80)
