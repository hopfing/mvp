"""Tests for unified analysis dataset."""

import polars as pl
import pytest


class TestUnifiedDataset:
    def test_joins_predictions_with_results(self, sample_predictions):
        from mvp.analysis.dataset import build_analysis_dataset

        results = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "result": ["P1", "P2"],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            results=results,
        )

        assert "result" in ds.columns
        assert "model_correct" in ds.columns
        assert "status" in ds.columns

        m1 = ds.filter(pl.col("match_uid") == "m1")
        assert m1["result"][0] == "P1"
        # p1_prob=0.65 > 0.5, predicted P1, result P1
        assert m1["model_correct"][0] is True
        assert m1["status"][0] == "resolved"

        m3 = ds.filter(pl.col("match_uid") == "m3")
        assert m3["status"][0] == "pending"

    def test_joins_sheet_data(self, sample_predictions, sample_sheet_data):
        from mvp.analysis.dataset import build_analysis_dataset

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            sheet_data=sample_sheet_data,
        )

        m1 = ds.filter(pl.col("match_uid") == "m1")
        assert m1["bet_side"][0] == "P1"
        assert m1["stake"][0] == "10"

    def test_circuit_normalization_from_sheet(
        self, sample_predictions, sample_sheet_data
    ):
        from mvp.analysis.dataset import build_analysis_dataset

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            sheet_data=sample_sheet_data,
        )

        # Circuit comes from predictions (chal/tour), NOT from sheet (CH/ATP)
        assert set(ds["circuit"].to_list()) == {"chal", "tour"}

    def test_joins_odds_by_book(self, sample_predictions):
        from mvp.analysis.dataset import build_analysis_dataset

        odds = pl.DataFrame({
            "match_uid": ["m1", "m1", "m2"],
            "book": ["dk", "br", "dk"],
            "has_prematch": [True, True, True],
            "closing_odds_p1": [2.10, 2.15, 1.85],
            "closing_odds_p2": [1.75, 1.72, 1.95],
            "closing_implied_p1": [1/2.10, 1/2.15, 1/1.85],
            "closing_implied_p2": [1/1.75, 1/1.72, 1/1.95],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            odds_by_book=odds,
        )

        m1 = ds.filter(pl.col("match_uid") == "m1")
        assert m1["dk_closing_odds_p1"][0] == pytest.approx(2.10)
        assert m1["br_closing_odds_p1"][0] == pytest.approx(2.15)

    def test_derived_metrics(self, sample_predictions):
        from mvp.analysis.dataset import build_analysis_dataset

        odds = pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "book": ["dk", "br"],
            "has_prematch": [True, True],
            "closing_odds_p1": [2.00, 2.10],
            "closing_odds_p2": [1.80, 1.75],
            "closing_implied_p1": [0.50, 1/2.10],
            "closing_implied_p2": [1/1.80, 1/1.75],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            odds_by_book=odds,
        )

        m1 = ds.filter(pl.col("match_uid") == "m1")
        # best closing = best (highest) odds across books for p1
        assert m1["best_closing_odds_p1"][0] == pytest.approx(2.10)
        # model_edge_vs_best = p1_prob - best_closing_implied_p1
        expected_edge = 0.65 - (1/2.10)
        assert m1["model_edge_vs_best_p1"][0] == pytest.approx(expected_edge, abs=0.01)

    def test_books_showing_edge(self, sample_predictions):
        from mvp.analysis.dataset import build_analysis_dataset

        odds = pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "book": ["dk", "br"],
            "has_prematch": [True, True],
            # DK: model=0.65 > implied=0.50 (edge)
            # BR: 0.65 < 0.71 (no edge)
            "closing_odds_p1": [2.00, 1.40],
            "closing_odds_p2": [1.80, 2.80],
            "closing_implied_p1": [0.50, 1/1.40],
            "closing_implied_p2": [1/1.80, 1/2.80],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            odds_by_book=odds,
        )

        m1 = ds.filter(pl.col("match_uid") == "m1")
        # Prediction is P1 (p1_prob=0.65). DK shows edge for P1, BR does not.
        assert m1["books_showing_edge"][0] == 1

    def test_empty_inputs_handled(self, sample_predictions):
        from mvp.analysis.dataset import build_analysis_dataset

        ds = build_analysis_dataset(predictions=sample_predictions)
        assert len(ds) == 3
        assert "status" in ds.columns

    def test_pred_side_metrics_with_cross_book(self, sample_predictions):
        from mvp.analysis.dataset import build_analysis_dataset

        cross_book = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "best_closing_odds_p1": [2.10, 1.85],
            "best_closing_odds_p2": [1.75, 1.95],
            "worst_closing_odds_p1": [2.00, 1.85],
            "worst_closing_odds_p2": [1.72, 1.95],
            "avg_closing_odds_p1": [2.05, 1.85],
            "avg_closing_odds_p2": [1.735, 1.95],
            "best_opening_odds_p1": [2.20, 1.90],
            "best_opening_odds_p2": [1.80, 2.00],
            "best_intraday_odds_p1": [2.25, 1.95],
            "best_intraday_odds_p2": [1.82, 2.05],
            "worst_intraday_odds_p1": [1.95, 1.80],
            "worst_intraday_odds_p2": [1.68, 1.90],
            "n_books": [2, 1],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            cross_book_odds=cross_book,
        )

        assert "pred_side" in ds.columns
        assert "pred_prob" in ds.columns
        assert "pred_odds_best_close" in ds.columns
        assert "model_edge_best_close" in ds.columns

        m1 = ds.filter(pl.col("match_uid") == "m1")
        # p1_win_prob=0.65 > 0.5, so pred_side=P1
        assert m1["pred_side"][0] == "P1"
        assert m1["pred_prob"][0] == pytest.approx(0.65)
        # pred_odds_best_close = best_closing_odds_p1 (because pred is P1)
        assert m1["pred_odds_best_close"][0] == pytest.approx(2.10)
        # model_edge = 0.65 - 1/2.10
        expected = 0.65 - (1.0 / 2.10)
        assert m1["model_edge_best_close"][0] == pytest.approx(expected, abs=0.01)

    def test_clv_computation(self, sample_predictions):
        from mvp.analysis.dataset import build_analysis_dataset

        cross_book = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "best_closing_odds_p1": [2.10, 1.85],
            "best_closing_odds_p2": [1.75, 1.95],
            "worst_closing_odds_p1": [2.00, 1.85],
            "worst_closing_odds_p2": [1.72, 1.95],
            "avg_closing_odds_p1": [2.05, 1.85],
            "avg_closing_odds_p2": [1.735, 1.95],
            "best_opening_odds_p1": [2.20, 1.90],
            "best_opening_odds_p2": [1.80, 2.00],
            "best_intraday_odds_p1": [2.25, 1.95],
            "best_intraday_odds_p2": [1.82, 2.05],
            "worst_intraday_odds_p1": [1.95, 1.80],
            "worst_intraday_odds_p2": [1.68, 1.90],
            "n_books": [2, 1],
        })

        sheet = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "bet_side": ["P1", "P2"],
            "bet_odds": ["2.20", "1.90"],
            "stake": ["10", "15"],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            sheet_data=sheet,
            cross_book_odds=cross_book,
        )

        assert "clv_vs_best" in ds.columns
        assert "clv_vs_avg" in ds.columns

        m1 = ds.filter(pl.col("match_uid") == "m1")
        # bet_side=P1, bet_odds=2.20, best_closing_p1=2.10
        # clv_vs_best = (2.20 - 2.10) / 2.10
        expected_clv = (2.20 - 2.10) / 2.10
        assert m1["clv_vs_best"][0] == pytest.approx(expected_clv, abs=0.01)

    def test_pred_side_metrics_legacy_path(self, sample_predictions):
        """Pred-side metrics also work with legacy odds_by_book path."""
        from mvp.analysis.dataset import build_analysis_dataset

        odds = pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "book": ["dk", "br"],
            "has_prematch": [True, True],
            "closing_odds_p1": [2.00, 2.10],
            "closing_odds_p2": [1.80, 1.75],
            "closing_implied_p1": [0.50, 1/2.10],
            "closing_implied_p2": [1/1.80, 1/1.75],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            odds_by_book=odds,
        )

        # Legacy path produces best_closing_odds_* so pred_side metrics work
        assert "pred_side" in ds.columns
        assert "pred_odds_best_close" in ds.columns
        m1 = ds.filter(pl.col("match_uid") == "m1")
        assert m1["pred_side"][0] == "P1"
        assert m1["pred_odds_best_close"][0] == pytest.approx(2.10)
