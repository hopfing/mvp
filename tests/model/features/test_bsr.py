"""Tests for the serve/return skill filter feature module."""

import polars as pl

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
