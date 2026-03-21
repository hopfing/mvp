"""Tests for flat-bet simulation engine."""

import polars as pl
import pytest
from datetime import datetime


def _make_analysis_ds():
    """Minimal analysis dataset with resolved matches and odds."""
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3", "m4"],
        "p1_win_prob": [0.65, 0.55, 0.70, 0.60],
        "p2_win_prob": [0.35, 0.45, 0.30, 0.40],
        "status": ["resolved", "resolved", "resolved", "resolved"],
        "model_correct": [True, True, False, True],
        "pred_side": ["P1", "P1", "P1", "P1"],
        "pred_prob": [0.65, 0.55, 0.70, 0.60],
        "pred_odds_best_close": [2.00, 1.80, 1.50, 2.20],
        "pred_odds_best_open": [2.10, 1.85, 1.55, 2.25],
        "pred_odds_best_intraday": [2.15, 1.90, 1.60, 2.30],
        "pred_odds_worst_intraday": [1.90, 1.75, 1.45, 2.10],
        "model_edge_best_close": [0.15, 0.0, 0.03, 0.15],
        "circuit": ["chal", "tour", "chal", "tour"],
        "surface": ["Hard", "Clay", "Hard", "Clay"],
        "consensus": [1.0, 0.8, 0.6, 1.0],
    })


class TestSimulations:
    def test_flat_best_close_overall(self):
        from mvp.analysis.simulations import run_simulations

        ds = _make_analysis_ds()
        sims = run_simulations(ds)

        overall = sims.filter(
            (pl.col("scenario") == "flat_best_close")
            & (pl.col("segment") == "overall")
        )
        assert len(overall) == 1
        row = overall.row(0, named=True)
        assert row["n_bets"] == 4
        assert row["n_wins"] == 3
        assert row["accuracy"] == pytest.approx(0.75)

    def test_flat_best_close_pnl(self):
        from mvp.analysis.simulations import run_simulations

        ds = _make_analysis_ds()
        sims = run_simulations(ds)

        overall = sims.filter(
            (pl.col("scenario") == "flat_best_close")
            & (pl.col("segment") == "overall")
        )
        row = overall.row(0, named=True)
        # Won 3 bets at odds 2.00, 1.80, 2.20 = returned 6.00
        # Staked 4 × 1.0 = 4.00
        # P&L = 6.00 - 4.00 = 2.00
        expected_return = 2.00 + 1.80 + 2.20
        expected_pnl = expected_return - 4.0
        assert row["net_pnl"] == pytest.approx(expected_pnl)
        assert row["roi"] == pytest.approx(expected_pnl / 4.0)

    def test_edge_filter(self):
        from mvp.analysis.simulations import run_simulations

        ds = _make_analysis_ds()
        sims = run_simulations(ds)

        edge_5 = sims.filter(
            (pl.col("scenario") == "edge_5pct")
            & (pl.col("segment") == "overall")
        )
        assert len(edge_5) == 1
        # model_edge_best_close > 0.05: m1 (0.15), m4 (0.15)
        assert edge_5["n_bets"][0] == 2

    def test_consensus_filter(self):
        from mvp.analysis.simulations import run_simulations

        ds = _make_analysis_ds()
        sims = run_simulations(ds)

        c100 = sims.filter(
            (pl.col("scenario") == "consensus_100")
            & (pl.col("segment") == "overall")
        )
        assert len(c100) == 1
        # consensus == 1.0: m1, m4
        assert c100["n_bets"][0] == 2

    def test_segment_by_circuit(self):
        from mvp.analysis.simulations import run_simulations

        ds = _make_analysis_ds()
        sims = run_simulations(ds)

        chal = sims.filter(
            (pl.col("scenario") == "flat_best_close")
            & (pl.col("segment") == "circuit")
            & (pl.col("segment_value") == "chal")
        )
        assert len(chal) == 1
        assert chal["n_bets"][0] == 2  # m1, m3

    def test_empty_dataset_returns_empty(self):
        from mvp.analysis.simulations import run_simulations

        ds = pl.DataFrame(schema={
            "match_uid": pl.Utf8,
            "status": pl.Utf8,
            "model_correct": pl.Boolean,
        })
        sims = run_simulations(ds)
        assert len(sims) == 0

    def test_missing_odds_col_skipped(self):
        from mvp.analysis.simulations import run_simulations

        ds = pl.DataFrame({
            "match_uid": ["m1"],
            "status": ["resolved"],
            "model_correct": [True],
            "p1_win_prob": [0.65],
            "p2_win_prob": [0.35],
        })
        sims = run_simulations(ds)
        # No odds columns → no simulations
        assert len(sims) == 0
