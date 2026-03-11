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
