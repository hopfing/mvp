"""Tests for the Model Performance page's Edge range slicing."""

import polars as pl


def _edge_ds() -> pl.DataFrame:
    """Six resolved rows spanning negative, zero and positive edge."""
    return pl.DataFrame(
        {
            "model_edge_open": [-0.03, -0.01, 0.0, 0.005, 0.02, None],
            "pred_odds_open": [2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
            "model_correct": [True, False, True, False, True, True],
            "surface": ["Hard"] * 6,
        }
    )


def test_cuts_rows_only_when_a_handle_moved():
    from mvp.analysis.dashboard.explorer import _cuts_rows

    assert not _cuts_rows((None, None))
    assert _cuts_rows((0.0, None))
    assert _cuts_rows((None, 0.01))
    assert _cuts_rows((0.0, 0.01))


def test_edge_slices_keeps_triplet_when_range_straddles_zero():
    from mvp.analysis.dashboard.explorer import _EDGE_SLICES, _edge_slices

    assert _edge_slices((None, None)) == _EDGE_SLICES
    # The user's example: >= 0.00 and < 1.00 keeps the edge == 0 rows on the
    # No Edge side, so both sides can still carry rows.
    assert _edge_slices((0.0, 0.01)) == _EDGE_SLICES
    assert _edge_slices((-0.02, 0.01)) == _EDGE_SLICES


def test_edge_slices_collapses_to_one_side():
    from mvp.analysis.dashboard.explorer import _edge_slices

    assert _edge_slices((0.005, None)) == [("Edge", None)]
    assert _edge_slices((0.005, 0.02)) == [("Edge", None)]
    assert _edge_slices((None, 0.0)) == [("No Edge", None)]
    assert _edge_slices((-0.03, 0.0)) == [("No Edge", None)]


def test_one_sided_tracks_the_collapse():
    from mvp.analysis.dashboard.explorer import _one_sided

    assert not _one_sided((None, None))
    assert not _one_sided((0.0, 0.01))
    assert _one_sided((0.005, None))
    assert _one_sided((None, 0.0))


def test_describe_range_names_both_cuts():
    from mvp.analysis.dashboard.explorer import _describe_range

    assert _describe_range((0.0, 0.01)) == "≥ 0.0% and < 1.0%"
    assert _describe_range((0.005, None)) == "≥ 0.5%"
    assert _describe_range((None, 0.01)) == "< 1.0%"


def test_range_filter_is_low_inclusive_high_exclusive():
    """Mirrors the filter render() applies, including the null-edge drop."""
    from mvp.analysis.dashboard.explorer import _cuts_rows

    ds = _edge_ds()
    edge_range = (0.0, 0.02)
    assert _cuts_rows(edge_range)

    low, high = edge_range
    kept = ds.filter(pl.col("model_edge_open") >= low).filter(
        pl.col("model_edge_open") < high
    )
    # 0.0 and 0.005 survive; -0.03/-0.01 are below, 0.02 is the exclusive
    # ceiling, and the null can't clear either bar.
    assert kept["model_edge_open"].to_list() == [0.0, 0.005]


def test_aggregate_by_collapses_rows_for_a_one_sided_range():
    from mvp.analysis.dashboard.explorer import _add_edge_flag, _aggregate_by

    flagged = _add_edge_flag(_edge_ds(), "model_edge_open")

    both = _aggregate_by(flagged, "surface", "pred_odds_open", (None, None))
    assert both["Edge"].to_list() == ["All", "Edge", "No Edge"]

    one = _aggregate_by(flagged, "surface", "pred_odds_open", (0.005, None))
    assert one["Edge"].to_list() == ["Edge"]
