"""The live/train parity check runs ahead of a projection config's not_null
filters, so an all-null estimator input cannot be filtered into silence."""

from types import SimpleNamespace

import polars as pl
import pytest

from mvp.model.projection_serving import _pending_frame
from mvp.projection.iid.column_checks import ALL_NULL_MIN_ROWS


def _config(filters):
    return SimpleNamespace(data=SimpleNamespace(filters=filters))


def _frame(n: int, logit_values):
    rows = []
    for i in range(n):
        for side, (pid, opp) in enumerate((("A", "B"), ("B", "A"))):
            rows.append({
                "match_uid": f"m{i}", "player_id": f"{pid}{i}", "opp_id": f"{opp}{i}",
                "best_of": 3, "circuit": "tour",
                "player_bsr_pserve_logit": logit_values[i], "opp_bsr_pserve_logit": logit_values[i],
                "player_bsr_pserve_logit_sd": 0.2, "opp_bsr_pserve_logit_sd": 0.2,
            })
    return pl.DataFrame(rows).with_columns(pl.col("player_bsr_pserve_logit").cast(pl.Float64), pl.col("opp_bsr_pserve_logit").cast(pl.Float64))


FILTERS = {"circuit": ["tour", "chal"], "player_bsr_pserve_logit": "not_null", "opp_bsr_pserve_logit": "not_null"}
PARITY = ["player_bsr_pserve_logit", "opp_bsr_pserve_logit"]


class TestPendingFrameParity:
    def test_all_null_large_frame_raises_before_not_null(self):
        n = ALL_NULL_MIN_ROWS + 5
        df = _frame(n, [None] * n)
        with pytest.raises(ValueError, match="null on all"):
            _pending_frame(_config(FILTERS), df, [f"m{i}" for i in range(n)], parity_columns=PARITY, stem="t")

    def test_all_null_small_frame_returns_empty_without_raising(self):
        n = ALL_NULL_MIN_ROWS - 1
        df = _frame(n, [None] * n)
        out = _pending_frame(_config(FILTERS), df, [f"m{i}" for i in range(n)], parity_columns=PARITY, stem="t")
        assert out.height == 0

    def test_partial_null_passes_and_not_null_drops_rows(self):
        n = ALL_NULL_MIN_ROWS + 5
        vals = [0.4] * n
        vals[0] = vals[1] = None
        df = _frame(n, vals)
        out = _pending_frame(_config(FILTERS), df, [f"m{i}" for i in range(n)], parity_columns=PARITY, stem="t")
        assert out.height == n - 2

    def test_no_parity_columns_never_raises(self):
        """Estimators that do not opt in get the filters and no check, so
        structurally-null inputs of the score-state models cannot trip it."""
        n = ALL_NULL_MIN_ROWS + 5
        df = _frame(n, [None] * n)
        out = _pending_frame(_config({"circuit": ["tour", "chal"]}), df, [f"m{i}" for i in range(n)], parity_columns=[], stem="t")
        assert out.height == n
