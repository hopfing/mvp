"""#104: null hold probabilities in the IID table lookups must propagate to
NaN, not index the table at grid zero (a player who never holds)."""

import importlib

import numpy as np
import polars as pl
import pytest


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.iid
    import mvp.model.features.points

    importlib.reload(mvp.model.features.points)
    importlib.reload(mvp.model.features.iid)


from mvp.model.features import iid  # noqa: E402  (registry side effects above)


class TestNullLookupFix:
    def test_null_hold_prob_yields_nan_not_grid_zero(self):
        df = pl.DataFrame({
            "player_iid_hold_prob": [None, 0.80],
            "opp_iid_hold_prob": [0.80, 0.80],
        })
        for fn in (iid.iid_expected_games_per_set, iid.iid_tiebreak_prob):
            out = df.select(fn().alias("v"))["v"].to_numpy()
            assert np.isnan(out[0]), "null input must not index the table at 0"
            assert np.isfinite(out[1])

    def test_both_sides_null_and_valid_rows_unchanged(self):
        df = pl.DataFrame({
            "player_iid_hold_prob": [None, 0.75, 0.75],
            "opp_iid_hold_prob": [None, None, 0.80],
        })
        out = df.select(iid.iid_expected_games_per_set().alias("v"))["v"].to_numpy()
        assert np.isnan(out[0]) and np.isnan(out[1]) and np.isfinite(out[2])
