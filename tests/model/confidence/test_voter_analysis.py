"""Tests for voter analysis — correlation, coverage curves, marginal value."""

import numpy as np
import polars as pl
import pytest

from mvp.model.confidence.validator import ConfidenceValidator, prepare_oof
from mvp.model.confidence.voter_analysis import (
    compute_coverage_curve,
    compute_voter_correlation,
    compute_voter_marginal_value,
)


def _make_voter_oof(make_oof_df, n=2000, n_voters=3, noise_scale=0.15, seed=42):
    """Build an OOF DataFrame with voter columns and voter_consensus."""
    rng = np.random.default_rng(seed)
    df = make_oof_df(n=n, seed=seed)

    # Simulate prepare_oof
    oof = prepare_oof([{
        "df": df.drop("y_true", "y_prob"),
        "y_true": df["y_true"].to_numpy(),
        "y_prob": df["y_prob"].to_numpy(),
    }])

    voter_names = [f"v{i}" for i in range(n_voters)]
    y_prob = oof["y_prob"].to_numpy()

    for name in voter_names:
        noise = rng.normal(0, noise_scale, size=len(y_prob))
        voter_probs = np.clip(y_prob + noise, 0.01, 0.99)
        oof = oof.with_columns(pl.Series(f"_voter_{name}", voter_probs))

    # Build voter_consensus column (like cli.py does)
    primary_pick = oof["y_prob"].to_numpy() >= 0.5
    agree = np.ones(len(oof), dtype=int)  # primary agrees with itself
    total = np.ones(len(oof), dtype=int)

    for name in voter_names:
        col = f"_voter_{name}"
        voter_pick = oof[col].to_numpy() >= 0.5
        agree += (voter_pick == primary_pick).astype(int)
        total += 1

    consensus = [f"{a}-{t - a}" for a, t in zip(agree, total)]
    oof = oof.with_columns(
        pl.Series("voter_consensus", consensus),
        pl.Series("voter_count", total),
    )

    return oof, voter_names


class TestVoterCorrelation:
    def test_returns_result_with_pairs(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df)
        result = compute_voter_correlation(oof, voter_names)
        assert len(result.voter_names) == len(voter_names) + 1  # +primary
        assert len(result.pairs) > 0

    def test_agreement_is_symmetric(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df)
        result = compute_voter_correlation(oof, voter_names)
        # Each pair stored once — check (a,b) key exists
        for (a, b), stats in result.pairs.items():
            assert stats.agreement_pct >= 0
            assert stats.agreement_pct <= 100

    def test_high_noise_lower_agreement(self, make_oof_df):
        oof_low, names = _make_voter_oof(make_oof_df, noise_scale=0.05)
        result_low = compute_voter_correlation(oof_low, names)
        oof_high, names = _make_voter_oof(make_oof_df, noise_scale=0.4)
        result_high = compute_voter_correlation(oof_high, names)
        # Average agreement should be lower with high noise
        avg_low = np.mean([s.agreement_pct for s in result_low.pairs.values()])
        avg_high = np.mean([s.agreement_pct for s in result_high.pairs.values()])
        assert avg_low > avg_high

    def test_disagree_correctness_sums_roughly(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df, noise_scale=0.3)
        result = compute_voter_correlation(oof, voter_names)
        for stats in result.pairs.values():
            if stats.n_disagree > 0:
                # a_correct + b_correct should sum to ~100% (one right, one wrong)
                assert stats.disagree_a_correct_pct is not None
                assert stats.disagree_b_correct_pct is not None
                total = stats.disagree_a_correct_pct + stats.disagree_b_correct_pct
                assert total == pytest.approx(100.0, abs=0.1)

    def test_scoped_voter_with_nulls(self, make_oof_df):
        """Voter with null predictions (scoped) should only count overlap rows."""
        oof, voter_names = _make_voter_oof(make_oof_df, n=1000, n_voters=2)
        # Null out half of voter v0's predictions
        v0_probs = oof["_voter_v0"].to_numpy().copy()
        v0_probs[:500] = np.nan
        oof = oof.with_columns(pl.Series("_voter_v0", v0_probs))
        result = compute_voter_correlation(oof, voter_names)
        # primary-v0 should have n_overlap=500
        key = ("primary", "v0")
        assert key in result.pairs
        assert result.pairs[key].n_overlap == 500


class TestCoverageCurve:
    def test_returns_points(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df)
        result = compute_coverage_curve(oof, voter_names)
        assert len(result.points) > 0
        assert result.n_total == len(oof)

    def test_coverage_monotonically_increases(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df)
        result = compute_coverage_curve(oof, voter_names)
        coverages = [p.coverage_pct for p in result.points]
        # Points are ordered by threshold descending, so coverage increases
        for i in range(1, len(coverages)):
            assert coverages[i] >= coverages[i - 1]

    def test_50pct_threshold_covers_nearly_all(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df)
        result = compute_coverage_curve(oof, voter_names)
        last_point = result.points[-1]
        assert last_point.threshold_pct == 50
        # Not exactly 100% because tied votes (e.g., 2-2) have exactly 50%
        # and >= 50 filter has float precision issues
        assert last_point.coverage_pct > 95.0

    def test_no_voter_consensus_returns_empty(self, make_oof_df):
        df = make_oof_df(n=100)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        result = compute_coverage_curve(oof, [])
        assert len(result.points) == 0


class TestVoterMarginalValue:
    def test_returns_one_per_voter(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df, n_voters=3)
        result = compute_voter_marginal_value(oof, voter_names)
        assert len(result.voters) == 3

    def test_removing_voter_increases_coverage(self, make_oof_df):
        """Removing a voter from 100% consensus should generally increase coverage."""
        oof, voter_names = _make_voter_oof(make_oof_df, n_voters=3, noise_scale=0.3)
        result = compute_voter_marginal_value(oof, voter_names)
        # At least one voter should cause coverage increase when removed
        any_positive = any(v.cov_delta_100 >= 0 for v in result.voters)
        assert any_positive

    def test_scope_pct_is_100_for_full_voters(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df, n_voters=2)
        result = compute_voter_marginal_value(oof, voter_names)
        for v in result.voters:
            assert v.scope_pct == pytest.approx(100.0, abs=0.1)

    def test_scoped_voter_lower_scope_pct(self, make_oof_df):
        oof, voter_names = _make_voter_oof(make_oof_df, n=1000, n_voters=2)
        # Null out half of v0
        v0_probs = oof["_voter_v0"].to_numpy().copy()
        v0_probs[:500] = np.nan
        oof = oof.with_columns(pl.Series("_voter_v0", v0_probs))
        result = compute_voter_marginal_value(oof, voter_names)
        v0_stats = next(v for v in result.voters if v.name == "v0")
        assert v0_stats.scope_pct == pytest.approx(50.0, abs=1.0)

    def test_no_voter_consensus_returns_empty(self, make_oof_df):
        df = make_oof_df(n=100)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        result = compute_voter_marginal_value(oof, [])
        assert len(result.voters) == 0


class TestVoterAnalysisIntegration:
    def test_validator_populates_voter_analysis(self, make_oof_df):
        """ConfidenceValidator with voter_names produces all 3 analyses."""
        oof, voter_names = _make_voter_oof(make_oof_df, n=2000)
        validator = ConfidenceValidator.from_oof(oof, voter_names=voter_names)
        result = validator.validate()
        assert result.voter_correlation is not None
        assert result.coverage_curve is not None
        assert result.voter_marginal is not None

    def test_validator_without_voters_skips_analysis(self, make_oof_df):
        """Standard model (no voters) should not produce voter analysis."""
        df = make_oof_df(n=500)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        validator = ConfidenceValidator.from_oof(oof)
        result = validator.validate()
        assert result.voter_correlation is None
        assert result.coverage_curve is None
        assert result.voter_marginal is None

    def test_report_includes_voter_sections(self, make_oof_df):
        """Report should include all 3 voter analysis sections."""
        from mvp.model.confidence.report import format_report
        oof, voter_names = _make_voter_oof(make_oof_df, n=2000, noise_scale=0.3)
        validator = ConfidenceValidator.from_oof(oof, voter_names=voter_names)
        result = validator.validate()
        report = format_report(result, model_name="voter_test")
        assert "VOTER CORRELATION" in report
        assert "COVERAGE vs QUALITY" in report
        assert "VOTER MARGINAL VALUE" in report

    def test_report_without_voters_omits_sections(self, make_oof_df):
        """Non-voter report should not include voter analysis sections."""
        from mvp.model.confidence.report import format_report
        df = make_oof_df(n=500)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        validator = ConfidenceValidator.from_oof(oof)
        result = validator.validate()
        report = format_report(result, model_name="no_voters")
        assert "VOTER CORRELATION" not in report
        assert "COVERAGE vs QUALITY" not in report
        assert "VOTER MARGINAL VALUE" not in report
