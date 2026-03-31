"""Tests for edge analysis page data prep."""

import polars as pl


def _make_sims():
    """Simulations with edge band scenarios across two consensus levels."""
    rows = []
    for scenario in ["edge_10pct", "edge_5pct", "edge_0pct", "neg_5pct", "neg_10pct"]:
        for basis_suffix in ["", "_open", "_mkt_formed"]:
            rows.append({
                "model_version": "all",
                "scenario": f"{scenario}{basis_suffix}",
                "segment": "overall",
                "segment_value": "overall",
                "n_bets": 20,
                "n_wins": 14,
                "n_losses": 6,
                "accuracy": 0.70,
                "total_staked": 20.0,
                "total_returned": 23.0,
                "net_pnl": 3.0,
                "roi": 0.15,
                "yield_pct": 0.15,
                "filter_desc": f"{scenario}",
            })
    # Add consensus and flat scenarios
    for s in ["consensus_100", "consensus_80", "flat_best_close",
              "flat_best_intraday", "flat_worst_intraday"]:
        rows.append({
            "model_version": "all",
            "scenario": s,
            "segment": "overall",
            "segment_value": "overall",
            "n_bets": 50,
            "n_wins": 35,
            "n_losses": 15,
            "accuracy": 0.70,
            "total_staked": 50.0,
            "total_returned": 57.5,
            "net_pnl": 7.5,
            "roi": 0.15,
            "yield_pct": 0.15,
            "filter_desc": s,
        })
    return pl.DataFrame(rows)


def test_filter_edge_scenarios_by_basis():
    from mvp.analysis.dashboard.edge import filter_edge_scenarios

    sims = _make_sims()

    # Close basis = no suffix
    close = filter_edge_scenarios(sims, basis="close")
    scenarios = close["scenario"].to_list()
    assert "edge_10pct" in scenarios
    assert "edge_10pct_open" not in scenarios

    # Open basis
    opens = filter_edge_scenarios(sims, basis="open")
    scenarios = opens["scenario"].to_list()
    assert "edge_10pct_open" in scenarios
    assert "edge_10pct" not in scenarios


def test_filter_edge_scenarios_by_consensus():
    from mvp.analysis.dashboard.edge import filter_edge_scenarios

    # Build sims with consensus segment
    sims = _make_sims()
    # Add consensus-segmented rows
    rows = []
    for scenario in ["edge_10pct", "edge_5pct"]:
        for cv in ["1.0", "0.8"]:
            rows.append({
                "model_version": "all",
                "scenario": scenario,
                "segment": "consensus",
                "segment_value": cv,
                "n_bets": 10,
                "n_wins": 7,
                "n_losses": 3,
                "accuracy": 0.70,
                "total_staked": 10.0,
                "total_returned": 11.5,
                "net_pnl": 1.5,
                "roi": 0.15,
                "yield_pct": 0.15,
                "filter_desc": scenario,
            })
    sims_ext = pl.concat([sims, pl.DataFrame(rows)], how="diagonal_relaxed")

    result = filter_edge_scenarios(sims_ext, basis="close", consensus="0.8")
    assert all(
        (r["segment"] == "consensus" and r["segment_value"] == "0.8")
        for r in result.iter_rows(named=True)
    )
