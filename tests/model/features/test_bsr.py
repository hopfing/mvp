"""Tests for the serve/return skill filter feature module."""

import polars as pl
import pytest

from mvp.atptour.bsr.filter import BSR_NEW_VALUE_NAMES
from mvp.model.features import bsr as bsr_module  # noqa: F401
from mvp.model.registry import get_registry

BASES = (
    "bsr_serve_mu", "bsr_serve_sd", "bsr_return_mu", "bsr_return_sd",
    "bsr_serve_surface_mu", "bsr_serve_surface_sd",
    "bsr_return_surface_mu", "bsr_return_surface_sd",
    "bsr_n_serve_obs", "bsr_days_since_serve_obs",
    "bsr_pserve_logit", "bsr_pserve_logit_sd",
)


class TestRegistration:
    def test_all_bases_registered_null_preserving_and_mirrored(self):
        registry = get_registry()
        for name in BASES:
            fd = registry.get(name)
            assert fd.mirror is True, name
            assert fd.impute is None, name

    def test_diff_and_sum_forms(self):
        registry = get_registry()
        for name in ("bsr_serve_mu", "bsr_return_mu", "bsr_serve_surface_mu",
                     "bsr_return_surface_mu", "bsr_pserve_logit"):
            assert registry.get(f"{name}_diff").impute is None
        for name in ("bsr_serve_mu", "bsr_return_mu", "bsr_pserve_logit"):
            assert registry.get(f"{name}_sum").impute is None

    def test_passthrough_reads_the_player_column(self):
        df = pl.DataFrame({"player_bsr_serve_mu": [0.12, None], "opp_bsr_serve_mu": [-0.3, 0.0]})
        out = df.select(bsr_module.bsr_serve_mu().alias("v"))
        assert out["v"].to_list() == [0.12, None]


class TestNewStreams:
    """Every value name the filter emits for the other 20 streams has a
    feature, and nothing else does."""

    def test_every_emitted_name_is_registered(self):
        registry = get_registry()
        for name in BSR_NEW_VALUE_NAMES:
            fd = registry.get(name)
            assert fd.mirror is True, name
            assert fd.impute is None, name

    def test_the_two_name_sets_are_disjoint_and_complete(self):
        assert not set(BSR_NEW_VALUE_NAMES) & set(BASES)
        # 20 streams x (mu, sd, n_obs, days_since, logit, logit_sd), plus the
        # returner pair and the server- and returner-side surface and indoor
        # pairs where the stream carries them: 3x16 + 10 (fsi: no returner) + 12x8 + 4x6 = 178.
        assert len(BSR_NEW_VALUE_NAMES) == 178

    def test_every_stream_emits_its_axes_in_the_documented_order(self):
        """The name sequence per stream is a contract: the slab is written by
        position, so a reordering here silently shifts every column after it."""
        from mvp.atptour.bsr.constants import STREAMS

        expected: list[str] = []
        for st in STREAMS[1:]:
            b = f"bsr_{st.name}"
            expected += [f"{b}_mu", f"{b}_sd"]
            if st.has_surface:
                expected += [f"{b}_surface_mu", f"{b}_surface_sd"]
            if st.has_indoor:
                expected += [f"{b}_indoor_mu", f"{b}_indoor_sd"]
            if st.has_returner:
                expected += [f"{b}_r_mu", f"{b}_r_sd"]
                if st.has_surface:
                    expected += [f"{b}_r_surface_mu", f"{b}_r_surface_sd"]
                if st.has_indoor:
                    expected += [f"{b}_r_indoor_mu", f"{b}_r_indoor_sd"]
            expected += [
                f"{b}_n_obs", f"{b}_days_since", f"{b}_logit", f"{b}_logit_sd",
            ]
        assert list(BSR_NEW_VALUE_NAMES) == expected

    def test_diffs_and_sums_on_the_means_and_logits(self):
        registry = get_registry()
        for name in ("bsr_fsi_mu", "bsr_w1_surface_mu", "bsr_w2_indoor_mu",
                     "bsr_bp_r_mu", "bsr_bh_mu", "bsr_hold_r_surface_mu",
                     "bsr_w1_r_indoor_mu"):
            assert name in BSR_NEW_VALUE_NAMES
            assert registry.get(f"{name}_diff").impute is None
            assert registry.get(f"{name}_sum").impute is None
        for name in ("bsr_fsi_logit", "bsr_hold_logit"):
            assert registry.get(f"{name}_diff").impute is None

    def test_sd_and_n_obs_have_no_diff(self):
        """Only means and the matchup logit are differenced; a difference of
        two posterior sds, of two counts or of two clocks is not a quantity."""
        registry = get_registry()
        for name in ("bsr_fsi_sd", "bsr_bp_n_obs", "bsr_ace_logit_sd",
                     "bsr_ace_days_since", "bsr_hold_r_surface_sd"):
            with pytest.raises(Exception):
                registry.get(f"{name}_diff")

    def test_passthrough_reads_the_player_column(self):
        df = pl.DataFrame({
            "player_bsr_bp_r_mu": [0.4, None],
            "opp_bsr_bp_r_mu": [-0.4, 0.1],
        })
        out = df.select(registry_expr("bsr_bp_r_mu").alias("v"))
        assert out["v"].to_list() == [0.4, None]


def registry_expr(name: str) -> pl.Expr:
    return get_registry().get(name).func()
