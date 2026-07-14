"""Tests for Glicko-2 interaction feature module (H58 family).

Coverage approach: one representative test per category for the uniform
ratio/product patterns; exhaustive numerical tests for the math-heavy ones
(z-scores, TrueSkill Φ(z), Bhattacharyya, logistic, Overlap Coefficient).
Plus two specific defensive checks the implementation review flagged:
  - Degenerate small-joint_rd case for `glicko_zscore_rd`
  - Player/opp asymmetric features produce different results under swap
    (catches copy-paste errors where both reference the same RD column).
"""

import math

import polars as pl
import pytest

from mvp.model import features as features_pkg  # noqa: F401 — triggers registration
from mvp.model.registry import get_registry


def _base_df() -> pl.DataFrame:
    """Standard fixture: known values for hand-computed assertions.

    player_mu=1600, opp_mu=1500 → mu_diff = 100
    player_rd=100, opp_rd=120 → rd_sum=220, joint_rd=sqrt(24400)≈156.205
    player_sigma=0.05, opp_sigma=0.06 → sigma_sum=0.11
    """
    return pl.DataFrame({
        "player_glicko_mu": [1600.0],
        "player_glicko_rd": [100.0],
        "player_glicko_sigma": [0.05],
        "opp_glicko_mu": [1500.0],
        "opp_glicko_rd": [120.0],
        "opp_glicko_sigma": [0.06],
    })


# ============================================================================
# Per-player ratios/products (one representative; pattern is uniform)
# ============================================================================

class TestPerPlayerRatios:
    def test_mu_over_rd(self):
        from mvp.model.features.glicko_interactions import glicko_mu_over_rd
        df = _base_df()
        result = df.select(glicko_mu_over_rd().alias("val"))
        # 1600 / 100 = 16
        assert result["val"][0] == pytest.approx(16.0)

    def test_log_rd(self):
        from mvp.model.features.glicko_interactions import glicko_log_rd
        df = _base_df()
        result = df.select(glicko_log_rd().alias("val"))
        # ln(100) ≈ 4.6052
        assert result["val"][0] == pytest.approx(math.log(100.0))

    def test_precision(self):
        from mvp.model.features.glicko_interactions import glicko_precision
        df = _base_df()
        result = df.select(glicko_precision().alias("val"))
        # 1/100 = 0.01
        assert result["val"][0] == pytest.approx(0.01)


# ============================================================================
# Pair-level differentials and sums
# ============================================================================

class TestPairLevelSums:
    def test_sigma_sum(self):
        from mvp.model.features.glicko_interactions import glicko_sigma_sum
        df = _base_df()
        result = df.select(glicko_sigma_sum().alias("val"))
        # 0.05 + 0.06 = 0.11
        assert result["val"][0] == pytest.approx(0.11)

    def test_rd_max(self):
        from mvp.model.features.glicko_interactions import glicko_rd_max
        df = _base_df()
        result = df.select(glicko_rd_max().alias("val"))
        assert result["val"][0] == pytest.approx(120.0)

    def test_rd_min(self):
        from mvp.model.features.glicko_interactions import glicko_rd_min
        df = _base_df()
        result = df.select(glicko_rd_min().alias("val"))
        assert result["val"][0] == pytest.approx(100.0)

    def test_rd_ratio(self):
        from mvp.model.features.glicko_interactions import glicko_rd_ratio
        df = _base_df()
        result = df.select(glicko_rd_ratio().alias("val"))
        # 120 / 100 = 1.2
        assert result["val"][0] == pytest.approx(1.2)

    def test_rd_ratio_zero_guard(self):
        """Defensive guard: min_rd=0 should return 1.0, not inf."""
        from mvp.model.features.glicko_interactions import glicko_rd_ratio
        df = pl.DataFrame({
            "player_glicko_rd": [100.0],
            "opp_glicko_rd": [0.0],
        })
        result = df.select(glicko_rd_ratio().alias("val"))
        assert result["val"][0] == pytest.approx(1.0)


# ============================================================================
# Joint uncertainty
# ============================================================================

class TestJointUncertainty:
    def test_joint_rd(self):
        from mvp.model.features.glicko_interactions import glicko_joint_rd
        df = _base_df()
        result = df.select(glicko_joint_rd().alias("val"))
        # sqrt(100^2 + 120^2) = sqrt(24400) ≈ 156.205
        assert result["val"][0] == pytest.approx(math.sqrt(24400.0))

    def test_joint_sigma(self):
        from mvp.model.features.glicko_interactions import glicko_joint_sigma
        df = _base_df()
        result = df.select(glicko_joint_sigma().alias("val"))
        # sqrt(0.05^2 + 0.06^2) = sqrt(0.0061) ≈ 0.0781
        assert result["val"][0] == pytest.approx(math.sqrt(0.0061))

    def test_joint_total(self):
        from mvp.model.features.glicko_interactions import glicko_joint_total
        df = _base_df()
        result = df.select(glicko_joint_total().alias("val"))
        # sqrt(100^2 + 120^2 + 0.05^2 + 0.06^2) ≈ sqrt(24400.0061)
        assert result["val"][0] == pytest.approx(math.sqrt(24400.0061))


# ============================================================================
# z-scores
# ============================================================================

class TestZScores:
    def test_zscore_rd(self):
        from mvp.model.features.glicko_interactions import glicko_zscore_rd
        df = _base_df()
        result = df.select(glicko_zscore_rd().alias("val"))
        # 100 / sqrt(24400) ≈ 0.6402
        assert result["val"][0] == pytest.approx(100.0 / math.sqrt(24400.0))

    def test_zscore_rd_small_joint_rd(self):
        """Degenerate case: very small joint_rd should produce large but
        finite z, not blow up or NaN."""
        from mvp.model.features.glicko_interactions import glicko_zscore_rd
        df = pl.DataFrame({
            "player_glicko_mu": [1600.0],
            "opp_glicko_mu": [1500.0],
            "player_glicko_rd": [0.01],
            "opp_glicko_rd": [0.01],
        })
        result = df.select(glicko_zscore_rd().alias("val"))
        # 100 / sqrt(0.0002) ≈ 7071
        expected = 100.0 / math.sqrt(0.0002)
        assert result["val"][0] == pytest.approx(expected, rel=1e-4)
        assert math.isfinite(result["val"][0])

    def test_diff_over_rd_sum(self):
        from mvp.model.features.glicko_interactions import glicko_diff_over_rd_sum
        df = _base_df()
        result = df.select(glicko_diff_over_rd_sum().alias("val"))
        # 100 / 220 ≈ 0.4545
        assert result["val"][0] == pytest.approx(100.0 / 220.0)


# ============================================================================
# TrueSkill-style P(win) — sigmoid approximation of Φ(z)
# ============================================================================

class TestTrueSkillPWin:
    def test_pwin_rd(self):
        from mvp.model.features.glicko_interactions import glicko_truesk_pwin_rd
        df = _base_df()
        result = df.select(glicko_truesk_pwin_rd().alias("val"))
        # z = 100 / sqrt(24400), P = 1/(1+exp(-1.702*z))
        z = 100.0 / math.sqrt(24400.0)
        expected = 1.0 / (1.0 + math.exp(-1.702 * z))
        assert result["val"][0] == pytest.approx(expected)

    def test_pwin_bounded(self):
        """P(win) is bounded [0, 1] (may saturate at boundaries for extreme inputs)
        and anti-symmetric under mu_diff sign flip."""
        from mvp.model.features.glicko_interactions import glicko_truesk_pwin_rd
        df = pl.DataFrame({
            "player_glicko_mu": [3000.0, 0.0],
            "opp_glicko_mu": [0.0, 3000.0],
            "player_glicko_rd": [50.0, 50.0],
            "opp_glicko_rd": [50.0, 50.0],
        })
        result = df.select(glicko_truesk_pwin_rd().alias("val"))
        assert 0.0 <= result["val"][0] <= 1.0
        assert 0.0 <= result["val"][1] <= 1.0
        # Anti-symmetry: opposite-sign mu_diff → P_a + P_b == 1 (even at saturation)
        assert result["val"][0] + result["val"][1] == pytest.approx(1.0, abs=1e-6)
        # Moderate input that doesn't saturate — P strictly in (0, 1)
        df_mod = pl.DataFrame({
            "player_glicko_mu": [1600.0],
            "opp_glicko_mu": [1400.0],
            "player_glicko_rd": [100.0],
            "opp_glicko_rd": [100.0],
        })
        mod_result = df_mod.select(glicko_truesk_pwin_rd().alias("val"))
        assert 0.0 < mod_result["val"][0] < 1.0


# ============================================================================
# Asymmetric uncertainty interactions
# ============================================================================

class TestAsymmetricInteractions:
    def test_mu_diff_x_player_rd(self):
        from mvp.model.features.glicko_interactions import glicko_mu_diff_x_player_rd
        df = _base_df()
        result = df.select(glicko_mu_diff_x_player_rd().alias("val"))
        # 100 × 100 = 10000
        assert result["val"][0] == pytest.approx(10000.0)

    def test_mu_diff_x_opp_rd(self):
        from mvp.model.features.glicko_interactions import glicko_mu_diff_x_opp_rd
        df = _base_df()
        result = df.select(glicko_mu_diff_x_opp_rd().alias("val"))
        # 100 × 120 = 12000
        assert result["val"][0] == pytest.approx(12000.0)

    def test_player_vs_opp_rd_distinct(self):
        """Critical: player_rd and opp_rd variants must produce different
        values. Catches copy-paste errors where both reference same column."""
        from mvp.model.features.glicko_interactions import (
            glicko_mu_diff_x_player_rd, glicko_mu_diff_x_opp_rd,
        )
        df = _base_df()
        player_val = df.select(glicko_mu_diff_x_player_rd().alias("v"))["v"][0]
        opp_val = df.select(glicko_mu_diff_x_opp_rd().alias("v"))["v"][0]
        # player_rd=100, opp_rd=120 → values must differ
        assert player_val != opp_val
        assert player_val == pytest.approx(10000.0)
        assert opp_val == pytest.approx(12000.0)

    def test_mu_diff_x_rd_asymmetry(self):
        from mvp.model.features.glicko_interactions import glicko_mu_diff_x_rd_asymmetry
        df = _base_df()
        result = df.select(glicko_mu_diff_x_rd_asymmetry().alias("val"))
        # 100 × (100 - 120) = -2000
        assert result["val"][0] == pytest.approx(-2000.0)


# ============================================================================
# Shrinkage forms
# ============================================================================

class TestShrinkage:
    def test_shrunk_diff_rd(self):
        from mvp.model.features.glicko_interactions import glicko_shrunk_diff_rd
        df = _base_df()
        result = df.select(glicko_shrunk_diff_rd().alias("val"))
        # 100 / (1 + 220) = 100/221
        assert result["val"][0] == pytest.approx(100.0 / 221.0)

    def test_shrunk_diff_rdsq(self):
        from mvp.model.features.glicko_interactions import glicko_shrunk_diff_rdsq
        df = _base_df()
        result = df.select(glicko_shrunk_diff_rdsq().alias("val"))
        # 100 / (1 + 100^2 + 120^2) = 100 / 24401
        assert result["val"][0] == pytest.approx(100.0 / 24401.0)


# ============================================================================
# Logistic-saturated (Elo-style P(win))
# ============================================================================

class TestLogistic:
    def test_logistic_diff(self):
        from mvp.model.features.glicko_interactions import (
            glicko_logistic_diff, GLICKO_SCALE,
        )
        df = _base_df()
        result = df.select(glicko_logistic_diff().alias("val"))
        # 1 / (1 + exp(-100 / 173.7178))
        expected = 1.0 / (1.0 + math.exp(-100.0 / GLICKO_SCALE))
        assert result["val"][0] == pytest.approx(expected)

    def test_logistic_at_zero_diff(self):
        """mu_diff = 0 → P(win) = 0.5"""
        from mvp.model.features.glicko_interactions import glicko_logistic_diff
        df = pl.DataFrame({
            "player_glicko_mu": [1500.0],
            "opp_glicko_mu": [1500.0],
        })
        result = df.select(glicko_logistic_diff().alias("val"))
        assert result["val"][0] == pytest.approx(0.5)

    def test_logistic_bounded(self):
        """Bounded (0, 1) for extreme inputs."""
        from mvp.model.features.glicko_interactions import glicko_logistic_diff
        df = pl.DataFrame({
            "player_glicko_mu": [5000.0, -5000.0],
            "opp_glicko_mu": [-5000.0, 5000.0],
        })
        result = df.select(glicko_logistic_diff().alias("val"))
        assert result["val"][0] > 0.999
        assert result["val"][1] < 0.001
        assert result["val"][0] + result["val"][1] == pytest.approx(1.0, abs=1e-6)


# ============================================================================
# Distribution overlap (Bhattacharyya, OVL)
# ============================================================================

class TestBhattacharyya:
    def test_bhattacharyya_rd(self):
        from mvp.model.features.glicko_interactions import glicko_bhattacharyya_rd
        df = _base_df()
        result = df.select(glicko_bhattacharyya_rd().alias("val"))
        # rd_sq_sum = 24400
        # coef = sqrt(2 * 100 * 120 / 24400) = sqrt(24000/24400)
        # exponent = -100^2 / (4 * 24400) = -10000 / 97600
        # BC = coef * exp(exponent)
        rd_sq_sum = 24400.0
        coef = math.sqrt(2.0 * 100.0 * 120.0 / rd_sq_sum)
        exponent = -10000.0 / (4.0 * rd_sq_sum)
        expected = coef * math.exp(exponent)
        assert result["val"][0] == pytest.approx(expected)

    def test_bhattacharyya_identical_distributions(self):
        """BC = 1 when both distributions are identical."""
        from mvp.model.features.glicko_interactions import glicko_bhattacharyya_rd
        df = pl.DataFrame({
            "player_glicko_mu": [1500.0],
            "opp_glicko_mu": [1500.0],
            "player_glicko_rd": [80.0],
            "opp_glicko_rd": [80.0],
        })
        result = df.select(glicko_bhattacharyya_rd().alias("val"))
        assert result["val"][0] == pytest.approx(1.0)


class TestOverlapCoefficient:
    def test_ovl_rd(self):
        from mvp.model.features.glicko_interactions import glicko_overlap_coefficient_rd
        df = _base_df()
        result = df.select(glicko_overlap_coefficient_rd().alias("val"))
        # abs_mu_diff = 100, half_rd_sq_sum = 12200
        # arg = -100 / (2 * sqrt(12200))
        # OVL = 2 / (1 + exp(-1.702 * arg))
        abs_mu_diff = 100.0
        half_rd_sq_sum = 12200.0
        arg = -abs_mu_diff / (2.0 * math.sqrt(half_rd_sq_sum))
        expected = 2.0 / (1.0 + math.exp(-1.702 * arg))
        assert result["val"][0] == pytest.approx(expected)

    def test_ovl_identical_means_equal_var(self):
        """When mu_p == mu_o (and equal variance), OVL should be ~1 (full overlap)."""
        from mvp.model.features.glicko_interactions import glicko_overlap_coefficient_rd
        df = pl.DataFrame({
            "player_glicko_mu": [1500.0],
            "opp_glicko_mu": [1500.0],
            "player_glicko_rd": [80.0],
            "opp_glicko_rd": [80.0],
        })
        result = df.select(glicko_overlap_coefficient_rd().alias("val"))
        # arg = 0 → OVL = 2 / (1 + exp(0)) = 2 / 2 = 1
        assert result["val"][0] == pytest.approx(1.0)


# ============================================================================
# Form-volatility interactions (category 10 — depend on registered form_volatility)
# ============================================================================

class TestFormVolInteractions:
    """The six live replacements for the frozen-sigma forms.

    These reference `player_form_volatility` (a registered feature), so the
    fixtures supply those columns directly — the arithmetic is what's under test;
    the engine's dependency/window wiring is covered generically in test_engine.
    """

    @staticmethod
    def _df_alltime() -> pl.DataFrame:
        # mu_diff = 100; fv_p = 1.0, fv_o = 0.5
        return pl.DataFrame({
            "player_glicko_mu": [1600.0],
            "opp_glicko_mu": [1500.0],
            "player_form_volatility": [1.0],
            "opp_form_volatility": [0.5],
        })

    @staticmethod
    def _df_windowed() -> pl.DataFrame:
        # mu_diff = 100; fv_p = 1.2, fv_o = 0.8 at the 90d window
        return pl.DataFrame({
            "player_glicko_mu": [1600.0],
            "opp_glicko_mu": [1500.0],
            "player_form_volatility_90d": [1.2],
            "opp_form_volatility_90d": [0.8],
        })

    def test_shrunk_diff_formvol(self):
        from mvp.model.features.glicko_interactions import glicko_shrunk_diff_formvol
        r = self._df_alltime().select(glicko_shrunk_diff_formvol().alias("v"))
        assert r["v"][0] == pytest.approx(100.0 / (1.0 + 1.0 + 0.5))  # 40.0

    def test_zscore_formvol(self):
        from mvp.model.features.glicko_interactions import (
            glicko_zscore_formvol, FORMVOL_RIDGE,
        )
        r = self._df_alltime().select(glicko_zscore_formvol().alias("v"))
        assert r["v"][0] == pytest.approx(
            100.0 / math.sqrt(1.0**2 + 0.5**2 + FORMVOL_RIDGE)
        )

    def test_diff_over_formvol_sum(self):
        from mvp.model.features.glicko_interactions import (
            glicko_diff_over_formvol_sum, FORMVOL_RIDGE,
        )
        r = self._df_alltime().select(glicko_diff_over_formvol_sum().alias("v"))
        assert r["v"][0] == pytest.approx(100.0 / (1.0 + 0.5 + FORMVOL_RIDGE))

    def test_mu_diff_x_player_and_opp_distinct(self):
        """player and opp variants must reference different fv columns."""
        from mvp.model.features.glicko_interactions import (
            glicko_mu_diff_x_player_formvol, glicko_mu_diff_x_opp_formvol,
        )
        df = self._df_alltime()
        pv = df.select(glicko_mu_diff_x_player_formvol().alias("v"))["v"][0]
        ov = df.select(glicko_mu_diff_x_opp_formvol().alias("v"))["v"][0]
        assert pv == pytest.approx(100.0 * 1.0)   # 100
        assert ov == pytest.approx(100.0 * 0.5)   # 50
        assert pv != ov

    def test_formvol_asymmetry(self):
        from mvp.model.features.glicko_interactions import glicko_mu_diff_x_formvol_asymmetry
        r = self._df_alltime().select(glicko_mu_diff_x_formvol_asymmetry().alias("v"))
        assert r["v"][0] == pytest.approx(100.0 * (1.0 - 0.5))  # 50.0

    def test_windowed_column_resolution(self):
        """days=90 reads the _90d columns, not the unsuffixed ones."""
        from mvp.model.features.glicko_interactions import glicko_shrunk_diff_formvol
        r = self._df_windowed().select(glicko_shrunk_diff_formvol(days=90).alias("v"))
        assert r["v"][0] == pytest.approx(100.0 / (1.0 + 1.2 + 0.8))  # 33.33

    def test_ridge_bounds_near_zero_denominator(self):
        """fv_p = fv_o = 0 → divide forms stay large but finite (ridge floor)."""
        from mvp.model.features.glicko_interactions import (
            glicko_zscore_formvol, glicko_diff_over_formvol_sum, FORMVOL_RIDGE,
        )
        df = pl.DataFrame({
            "player_glicko_mu": [1600.0],
            "opp_glicko_mu": [1500.0],
            "player_form_volatility": [0.0],
            "opp_form_volatility": [0.0],
        })
        z = df.select(glicko_zscore_formvol().alias("v"))["v"][0]
        s = df.select(glicko_diff_over_formvol_sum().alias("v"))["v"][0]
        assert z == pytest.approx(100.0 / math.sqrt(FORMVOL_RIDGE))
        assert s == pytest.approx(100.0 / FORMVOL_RIDGE)
        assert math.isfinite(z) and math.isfinite(s)

    def test_null_formvol_passes_through(self):
        """impute=None: a null form_volatility yields a null feature, not a value."""
        from mvp.model.features.glicko_interactions import (
            glicko_shrunk_diff_formvol, glicko_zscore_formvol,
        )
        df = pl.DataFrame({
            "player_glicko_mu": [1600.0],
            "opp_glicko_mu": [1500.0],
            "player_form_volatility": [None],
            "opp_form_volatility": [0.5],
        }, schema_overrides={"player_form_volatility": pl.Float64})
        assert df.select(glicko_shrunk_diff_formvol().alias("v"))["v"][0] is None
        assert df.select(glicko_zscore_formvol().alias("v"))["v"][0] is None

    def test_metadata(self):
        reg = get_registry()
        anti_symmetric = [
            "glicko_shrunk_diff_formvol", "glicko_zscore_formvol",
            "glicko_diff_over_formvol_sum",
            "glicko_mu_diff_x_player_formvol", "glicko_mu_diff_x_opp_formvol",
        ]
        for name in anti_symmetric:
            f = reg.get(name)
            assert f.mirror is False and f.match_level is False, name
            assert f.params == ["days"], name
            assert f.depends_on == ["form_volatility"], name
            assert f.impute is None, name
        asym = reg.get("glicko_mu_diff_x_formvol_asymmetry")
        assert asym.mirror is False and asym.match_level is True   # symmetric
        assert asym.params == ["days"]
        assert asym.depends_on == ["form_volatility"]
        assert asym.impute is None


# ============================================================================
# Registration check
# ============================================================================

class TestAllH58FeaturesRegistered:
    def test_all_features_in_registry(self):
        registry = get_registry()
        expected = [
            # Per-player ratios/products
            "glicko_mu_over_rd", "glicko_mu_over_sigma",
            "glicko_mu_x_rd", "glicko_mu_x_sigma",
            "glicko_rd_x_sigma", "glicko_rd_over_sigma",
            "glicko_log_rd", "glicko_log_sigma",
            "glicko_precision", "glicko_precision_sigma",
            # Pair-level
            "glicko_sigma_sum",
            "glicko_rd_max", "glicko_rd_min", "glicko_rd_ratio",
            "glicko_sigma_max", "glicko_sigma_min", "glicko_sigma_ratio",
            # Joint uncertainty
            "glicko_joint_rd", "glicko_joint_sigma", "glicko_joint_total",
            # z-scores
            "glicko_zscore_rd", "glicko_zscore_sigma", "glicko_zscore_total",
            "glicko_diff_over_rd_sum", "glicko_diff_over_sigma_sum",
            # TrueSkill
            "glicko_truesk_pwin_rd", "glicko_truesk_pwin_sigma", "glicko_truesk_pwin_total",
            # Asymmetric
            "glicko_mu_diff_x_player_rd", "glicko_mu_diff_x_opp_rd",
            "glicko_mu_diff_x_player_sigma", "glicko_mu_diff_x_opp_sigma",
            "glicko_mu_diff_x_rd_asymmetry", "glicko_mu_diff_x_sigma_asymmetry",
            # Shrinkage
            "glicko_shrunk_diff_rd", "glicko_shrunk_diff_rdsq", "glicko_shrunk_diff_sigma",
            # Logistic
            "glicko_logistic_diff",
            # Overlap
            "glicko_bhattacharyya_rd", "glicko_bhattacharyya_sigma",
            "glicko_overlap_coefficient_rd",
            # Form-volatility interactions (category 10)
            "glicko_shrunk_diff_formvol", "glicko_zscore_formvol",
            "glicko_diff_over_formvol_sum",
            "glicko_mu_diff_x_player_formvol", "glicko_mu_diff_x_opp_formvol",
            "glicko_mu_diff_x_formvol_asymmetry",
        ]
        registered = registry.list_features()
        for name in expected:
            assert name in registered, f"Feature '{name}' not registered"

    def test_per_player_features_mirror_correctly(self):
        """Per-player features should have mirror=True so engine generates opp_*."""
        registry = get_registry()
        for name in [
            "glicko_mu_over_rd", "glicko_mu_over_sigma",
            "glicko_log_rd", "glicko_precision",
        ]:
            feat = registry.get(name)
            assert feat.mirror is True, f"{name} should have mirror=True"
            assert feat.match_level is False, f"{name} should have match_level=False"

    def test_symmetric_features_are_match_level(self):
        """Symmetric pair-level features should be match_level=True."""
        registry = get_registry()
        for name in [
            "glicko_sigma_sum", "glicko_rd_max", "glicko_rd_min", "glicko_rd_ratio",
            "glicko_joint_rd", "glicko_joint_sigma", "glicko_joint_total",
            "glicko_bhattacharyya_rd", "glicko_overlap_coefficient_rd",
        ]:
            feat = registry.get(name)
            assert feat.match_level is True, f"{name} should have match_level=True"
            assert feat.mirror is False, f"{name} should have mirror=False"

    def test_all_h58_features_have_none_impute(self):
        """All H58 features should pass NaN through (XGBoost handles it)."""
        registry = get_registry()
        h58_features = [
            "glicko_mu_over_rd", "glicko_log_rd", "glicko_precision",
            "glicko_sigma_sum", "glicko_rd_ratio", "glicko_joint_rd",
            "glicko_zscore_rd", "glicko_truesk_pwin_rd",
            "glicko_mu_diff_x_player_rd", "glicko_shrunk_diff_rd",
            "glicko_logistic_diff", "glicko_bhattacharyya_rd",
            "glicko_overlap_coefficient_rd",
        ]
        for name in h58_features:
            feat = registry.get(name)
            assert feat.impute is None, f"{name} should have impute=None"
