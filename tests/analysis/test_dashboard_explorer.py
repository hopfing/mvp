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


def _movement_ds() -> pl.DataFrame:
    """One resolved row per reachable movement state, all past the floor.

    Edge at open and edge at close are set directly; odds are the price the
    close cell is scored at.
    """
    return pl.DataFrame(
        {
            # opened / moved / now:
            # +/up/+, +/flat/+, +/down/+, +/down/-, -/up/+, -/up/-, -/flat/-, -/down/-
            "model_edge_open": [
                0.02, 0.02, 0.08, 0.02, -0.02, -0.08, -0.02, -0.02,
            ],
            "model_edge_best_close": [
                0.06, 0.02, 0.03, -0.01, 0.03, -0.04, -0.02, -0.06,
            ],
            "pred_odds_best_close": [2.0] * 8,
            "model_correct": [True, False, True, False, True, False, True, False],
            "predicted_at": ["2026-05-01 12:00:00+00:00"] * 8,
            "circuit": ["tour"] * 8,
        }
    )


def test_classify_movement_assigns_every_reachable_state():
    from mvp.analysis.dashboard.explorer import _MOVEMENT_ROWS, _classify_movement

    tagged = _classify_movement(_movement_ds(), "model_edge_best_close")
    seen = set(
        zip(tagged["opened"].to_list(), tagged["moved"].to_list(), tagged["now"].to_list())
    )
    assert seen == {(o, m, n) for o, m, n, _ in _MOVEMENT_ROWS}


def test_classify_movement_treats_only_exact_ties_as_flat():
    from mvp.analysis.dashboard.explorer import _classify_movement

    df = pl.DataFrame(
        {
            "model_edge_open": [0.02, 0.02, 0.02],
            # exact tie, then a 0.1pp drift either side -- the drifts must
            # not be absorbed into flat.
            "model_edge_best_close": [0.02, 0.021, 0.019],
        }
    )
    tagged = _classify_movement(df, "model_edge_best_close")
    assert tagged["moved"].to_list() == ["flat", "up", "down"]


def test_movement_states_never_include_an_impossible_combination():
    from mvp.analysis.dashboard.explorer import _MOVEMENT_ROWS, _classify_movement

    # A flat line cannot change sign, and edge cannot move up from positive
    # into negative or down from negative into positive. Assert the row spec
    # excludes them, then that real classification agrees.
    impossible = {
        ("open +", "up", "now -"),
        ("open -", "down", "now +"),
        ("open +", "flat", "now -"),
        ("open -", "flat", "now +"),
    }
    assert impossible.isdisjoint({(o, m, n) for o, m, n, _ in _MOVEMENT_ROWS})

    tagged = _classify_movement(_movement_ds(), "model_edge_best_close")
    seen = set(
        zip(tagged["opened"].to_list(), tagged["moved"].to_list(), tagged["now"].to_list())
    )
    assert impossible.isdisjoint(seen)


def test_movement_table_holds_its_row_set_and_totals():
    from mvp.analysis.dashboard.explorer import _MOVEMENT_ROWS, _movement_table

    table = _movement_table(
        _movement_ds(), "pred_odds_best_close", "model_edge_best_close"
    )
    assert table["Movement"].to_list() == (
        [label for *_, label in _MOVEMENT_ROWS] + ["Total"]
    )
    assert table.filter(pl.col("Movement") == "Total")["N"].item() == 8
    # Every fixture row lands in exactly one state, so the state rows sum
    # to the total.
    assert table.filter(pl.col("Movement") != "Total")["N"].sum() == 8


def test_movement_table_keeps_empty_states_as_rows():
    from mvp.analysis.dashboard.explorer import _MOVEMENT_ROWS, _movement_table

    # Only an "Edge grew" row: the other seven states must still appear so
    # the table does not restructure under an active Edge cut.
    df = _movement_ds().head(1)
    table = _movement_table(df, "pred_odds_best_close", "model_edge_best_close")
    assert len(table) == len(_MOVEMENT_ROWS) + 1
    assert table.filter(pl.col("Movement") == "Edge grew")["N"].item() == 1
    assert table.filter(pl.col("Movement") == "Gap widened")["N"].item() == 0
    # An empty state has no edge means to report rather than a zero.
    assert table.filter(pl.col("Movement") == "Gap widened")["Edge"].item() is None


def test_movement_table_scores_cells_at_the_given_basis_price():
    from mvp.analysis.dashboard.explorer import _movement_table

    df = pl.DataFrame(
        {
            "model_edge_open": [0.08, 0.08],
            "model_edge_best_close": [0.03, 0.03],
            "pred_odds_best_close": [3.0, 3.0],
            "pred_odds_open": [1.1, 1.1],
            "model_correct": [True, False],
            "predicted_at": ["2026-05-01 12:00:00+00:00"] * 2,
        }
    )
    close = _movement_table(df, "pred_odds_best_close", "model_edge_best_close")
    row = close.filter(pl.col("Movement") == "Edge decayed")
    # One win at 3.0 and one loss on two flat $1 bets: +$1.00, ROI +50%.
    assert row["P&L"].item() == 1.0
    assert row["ROI %"].item() == 50.0

    # Same rows scored at the open price instead: 1.1 - 2 = -$0.90.
    at_open = _movement_table(df, "pred_odds_open", "model_edge_best_close")
    assert at_open.filter(pl.col("Movement") == "Edge decayed")["P&L"].item() == -0.9


def test_movement_eligible_drops_pre_floor_and_null_edge_rows():
    from mvp.analysis.dashboard.explorer import (
        _OPENING_RELIABLE_AFTER,
        _movement_eligible,
    )

    df = pl.DataFrame(
        {
            "model_edge_open": [0.02, 0.02, None, 0.02],
            "model_edge_best_close": [0.03, 0.03, 0.03, None],
            "predicted_at": [
                "2026-05-01 12:00:00+00:00",   # keep
                "2026-03-01 12:00:00+00:00",   # pre-floor
                "2026-05-01 12:00:00+00:00",   # null open edge
                "2026-05-01 12:00:00+00:00",   # null close edge
            ],
        }
    )
    kept = _movement_eligible(df, "model_edge_best_close")
    assert len(kept) == 1
    assert kept["predicted_at"].item() > _OPENING_RELIABLE_AFTER


def test_movement_eligible_returns_empty_when_open_edge_is_absent():
    from mvp.analysis.dashboard.explorer import _movement_eligible

    df = pl.DataFrame({"model_edge_best_close": [0.03], "pred_odds_best_close": [2.0]})
    assert len(_movement_eligible(df, "model_edge_best_close")) == 0


def test_movement_table_reports_flat_rows_as_positive_zero_drift():
    from mvp.analysis.dashboard.explorer import _movement_table

    # Exact ties whose float mean lands on -0.0 must not render as -0.00pp
    # on a row that by definition did not move.
    df = pl.DataFrame(
        {
            "model_edge_open": [0.07, 0.09],
            "model_edge_best_close": [0.07, 0.09],
            "pred_odds_best_close": [2.0, 2.0],
            "model_correct": [True, False],
            "predicted_at": ["2026-05-01 12:00:00+00:00"] * 2,
        }
    )
    drift = _movement_table(
        df, "pred_odds_best_close", "model_edge_best_close"
    ).filter(pl.col("Movement") == "Edge unmoved")["Δ Edge"].item()
    assert drift == 0.0
    assert str(drift) == "0.0"
