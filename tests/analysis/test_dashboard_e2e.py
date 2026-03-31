"""Smoke test: all dashboard pages render without error on realistic data."""

import polars as pl


def _make_full_ds():
    """Analysis dataset with all columns the dashboard pages expect."""
    import random

    random.seed(42)
    n = 50
    return pl.DataFrame(
        {
            "match_uid": [f"m{i}" for i in range(n)],
            "p1_win_prob": [random.uniform(0.45, 0.80) for _ in range(n)],
            "p2_win_prob": [random.uniform(0.20, 0.55) for _ in range(n)],
            "status": ["resolved"] * (n - 5) + ["pending"] * 5,
            "model_correct": [random.choice([True, False]) for _ in range(n - 5)]
            + [None] * 5,
            "pred_side": ["P1"] * n,
            "pred_prob": [random.uniform(0.50, 0.80) for _ in range(n)],
            "pred_odds_best_close": [random.uniform(1.2, 3.5) for _ in range(n)],
            "pred_odds_open": [random.uniform(1.2, 3.5) for _ in range(n)],
            "pred_odds_market_formed": [random.uniform(1.2, 3.5) for _ in range(n)],
            "model_edge_best_close": [random.uniform(-0.15, 0.20) for _ in range(n)],
            "model_edge_open": [random.uniform(-0.15, 0.20) for _ in range(n)],
            "model_edge_market_formed": [random.uniform(-0.15, 0.20) for _ in range(n)],
            "consensus": [random.choice([1.0, 0.8, 0.6]) for _ in range(n)],
            "circuit": [random.choice(["chal", "tour"]) for _ in range(n)],
            "surface": [random.choice(["Hard", "Clay", "Grass"]) for _ in range(n)],
            "bet_side": ["P1" if i < 20 else None for i in range(n)],
            "stake": [
                str(random.randint(5, 20)) if i < 20 else None for i in range(n)
            ],
            "net": [
                str(round(random.uniform(-15, 20), 2)) if i < 20 else None
                for i in range(n)
            ],
            "clv_vs_avg": [
                random.uniform(-0.05, 0.10) if i < 20 else None for i in range(n)
            ],
            "bet_odds": [
                random.uniform(1.5, 3.0) if i < 20 else None for i in range(n)
            ],
        }
    )


def _make_full_sims():
    """Minimal simulations DataFrame covering the scenarios pages need."""
    from mvp.analysis.simulations import SCENARIOS

    rows = []
    for scenario_def in SCENARIOS:
        name = scenario_def["name"]
        rows.append(
            {
                "model_version": "all",
                "scenario": name,
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
                "filter_desc": name,
            }
        )
    # Add consensus segments
    for name in ["edge_10pct", "edge_5pct"]:
        for cv in ["1.0", "0.8", "0.6"]:
            rows.append(
                {
                    "model_version": "all",
                    "scenario": name,
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
                    "filter_desc": name,
                }
            )
    return pl.DataFrame(rows)


def test_overview_computes_without_error():
    from mvp.analysis.dashboard.overview import compute_headlines

    ds = _make_full_ds()
    h = compute_headlines(ds)
    assert h["n_predictions"] == 50
    assert h["accuracy"] is not None


def test_edge_filters_without_error():
    from mvp.analysis.dashboard.edge import filter_edge_scenarios

    sims = _make_full_sims()
    for basis in ["close", "open", "formed"]:
        result = filter_edge_scenarios(sims, basis=basis)
        assert len(result) > 0


def test_odds_bucketing_without_error():
    from mvp.analysis.dashboard.odds import odds_range_summary

    ds = _make_full_ds()
    summary = odds_range_summary(ds, odds_col="pred_odds_best_close")
    assert len(summary) > 0


def test_execution_clv_by_group():
    from mvp.analysis.dashboard.execution import clv_by_group

    ds = _make_full_ds()
    result = clv_by_group(ds, group_col="consensus", clv_col="clv_vs_avg")
    assert "group" in result.columns


def test_execution_summary_on_full_ds():
    from mvp.analysis.dashboard.execution import execution_summary

    ds = _make_full_ds()
    ex = execution_summary(ds)
    assert ex["n_bets"] > 0


def test_sharpness_detect_books():
    from mvp.analysis.dashboard.sharpness import detect_books

    sims = _make_full_sims()
    books = detect_books(sims)
    assert isinstance(books, list)
