"""Tests for unified analysis dataset."""

from datetime import datetime, timezone

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

    def test_sheet_prediction_carried_as_bet_pred_side(
        self, sample_predictions, sample_sheet_data
    ):
        """The sheet's frozen `prediction` is joined as `bet_pred_side`."""
        from mvp.analysis.dataset import build_analysis_dataset

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            sheet_data=sample_sheet_data,
        )

        assert "bet_pred_side" in ds.columns
        assert "prediction" not in ds.columns
        m1 = ds.filter(pl.col("match_uid") == "m1")
        assert m1["bet_pred_side"][0] == "P1"
        m2 = ds.filter(pl.col("match_uid") == "m2")
        assert m2["bet_pred_side"][0] == "P2"

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

        # Long format: one row per (match_uid, player_id)
        cross_book = pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m2"],
            "player_id": ["A001", "A002", "B001", "B002"],
            "best_closing_odds": [2.10, 1.75, 1.85, 1.95],
            "worst_closing_odds": [2.00, 1.72, 1.85, 1.95],
            "avg_closing_odds": [2.05, 1.735, 1.85, 1.95],
            "best_opening_odds": [2.20, 1.80, 1.90, 2.00],
            "best_intraday_odds": [2.25, 1.82, 1.95, 2.05],
            "worst_intraday_odds": [1.95, 1.68, 1.80, 1.90],
            "n_books": [2, 2, 1, 1],
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
            "match_uid": ["m1", "m1", "m2", "m2"],
            "player_id": ["A001", "A002", "B001", "B002"],
            "best_closing_odds": [2.10, 1.75, 1.85, 1.95],
            "worst_closing_odds": [2.00, 1.72, 1.85, 1.95],
            "avg_closing_odds": [2.05, 1.735, 1.85, 1.95],
            "best_opening_odds": [2.20, 1.80, 1.90, 2.00],
            "best_intraday_odds": [2.25, 1.82, 1.95, 2.05],
            "worst_intraday_odds": [1.95, 1.68, 1.80, 1.90],
            "n_books": [2, 2, 1, 1],
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

    def test_first_live_fetched_at_from_snapshots(self, sample_predictions):
        """first_live_fetched_at = first non-NS snapshot strictly after the last
        NOT_STARTED snapshot across all books."""
        from mvp.analysis.dataset import build_analysis_dataset

        snapshots = pl.DataFrame({
            "match_uid": [
                # m1: dk flips STARTED at 10:15 but mgm still NOT_STARTED at 10:30
                # → first_live should be AFTER 10:30, not 10:15
                "m1", "m1", "m1", "m1", "m1", "m1",
                # m2: only NOT_STARTED snapshots → first_live null
                "m2", "m2",
            ],
            "book": ["dk", "dk", "dk", "mgm", "mgm", "mgm", "dk", "mgm"],
            "player_id": ["A001"] * 8,
            "side": ["p1"] * 8,
            "odds": [2.10, 2.12, 2.08, 2.11, 2.10, 2.09, 1.75, 1.76],
            "fetched_at": [
                datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 10, 10, 15, tzinfo=timezone.utc),
                datetime(2026, 3, 10, 10, 45, tzinfo=timezone.utc),
                datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 10, 10, 30, tzinfo=timezone.utc),
                datetime(2026, 3, 10, 10, 45, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 9, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 11, 9, 30, tzinfo=timezone.utc),
            ],
            "event_status": [
                "NOT_STARTED", "STARTED", "IN_PLAY",
                "NOT_STARTED", "NOT_STARTED", "STARTED",
                "NOT_STARTED", "NOT_STARTED",
            ],
        })

        ds = build_analysis_dataset(
            predictions=sample_predictions,
            all_snapshots=snapshots,
        )

        assert "first_live_fetched_at" in ds.columns
        m1 = ds.filter(pl.col("match_uid") == "m1")
        # Last NS across all books = mgm @ 10:30. First non-NS strictly after
        # that is dk IN_PLAY @ 10:45 (mgm STARTED is also @ 10:45). Tie →
        # 10:45 is the answer.
        assert m1["first_live_fetched_at"][0] == datetime(
            2026, 3, 10, 10, 45, tzinfo=timezone.utc
        )
        # m2 has no live snapshots — should be null
        m2 = ds.filter(pl.col("match_uid") == "m2")
        assert m2["first_live_fetched_at"][0] is None


class TestBetEdgeDerivation:
    """Contract tests for derive_bet_edge_cols.

    Locks the bet_side-aware derivation: betting on the model's pick
    ("Model Fav") consumes fav_edge; betting against ("Model Dog")
    consumes dog_edge. Same conditioning applies to bet_edge_open.
    """

    def _frame(self) -> pl.DataFrame:
        # m1: bet_side == pred_side (Model Fav). pred_side=P1 because
        #     p1_win_prob > 0.5.
        # m2: bet_side != pred_side (Model Dog). pred_side=P1, bet_side=P2.
        # m3: pred_side=P2 (p2_win_prob > 0.5), bet on P2 → Model Fav.
        return pl.DataFrame({
            "match_uid": ["m1", "m2", "m3"],
            "pred_side": ["P1", "P1", "P2"],
            "bet_side": ["P1", "P2", "P2"],
            "fav_edge": [0.05, 0.03, 0.04],
            "dog_edge": [-0.02, -0.04, -0.01],
            "p1_win_prob": [0.65, 0.55, 0.30],
            "p2_win_prob": [0.35, 0.45, 0.70],
            "best_opening_odds_p1": [2.00, 1.90, 3.50],
            "best_opening_odds_p2": [2.10, 2.20, 1.40],
        })

    def test_bet_edge_picks_fav_when_betting_model_pick(self):
        from mvp.analysis.dataset import derive_bet_edge_cols

        ds = derive_bet_edge_cols(self._frame())

        assert "bet_edge" in ds.columns
        m1 = ds.filter(pl.col("match_uid") == "m1")
        assert m1["bet_edge"][0] == pytest.approx(0.05)
        m3 = ds.filter(pl.col("match_uid") == "m3")
        assert m3["bet_edge"][0] == pytest.approx(0.04)

    def test_bet_edge_picks_dog_when_betting_against_model(self):
        from mvp.analysis.dataset import derive_bet_edge_cols

        ds = derive_bet_edge_cols(self._frame())

        m2 = ds.filter(pl.col("match_uid") == "m2")
        assert m2["bet_edge"][0] == pytest.approx(-0.04)

    def test_bet_edge_open_uses_bet_side_columns(self):
        from mvp.analysis.dataset import derive_bet_edge_cols

        ds = derive_bet_edge_cols(self._frame())

        assert "bet_edge_open" in ds.columns
        # m1: bet_side=P1 → p1_win_prob - 1/best_opening_odds_p1
        m1 = ds.filter(pl.col("match_uid") == "m1")
        assert m1["bet_edge_open"][0] == pytest.approx(0.65 - 1.0 / 2.00)
        # m2: bet_side=P2 → p2_win_prob - 1/best_opening_odds_p2
        m2 = ds.filter(pl.col("match_uid") == "m2")
        assert m2["bet_edge_open"][0] == pytest.approx(0.45 - 1.0 / 2.20)
        # m3: bet_side=P2 → p2_win_prob - 1/best_opening_odds_p2
        m3 = ds.filter(pl.col("match_uid") == "m3")
        assert m3["bet_edge_open"][0] == pytest.approx(0.70 - 1.0 / 1.40)

    def test_bet_edge_anchors_to_as_bet_pick_over_flipped_pred_side(self):
        """A bet the model later flipped away from stays a Model Fav.

        bet_pred_side (as-bet pick) = P1 and the bet was on P1, but the live
        pred_side later flipped to P2. bet_edge must still take fav_edge, and
        bet_pick_side must reflect the as-bet pick.
        """
        from mvp.analysis.dataset import derive_bet_edge_cols

        df = pl.DataFrame({
            "match_uid": ["m1"],
            "bet_pred_side": ["P1"],
            "pred_side": ["P2"],
            "bet_side": ["P1"],
            "fav_edge": [0.06],
            "dog_edge": [-0.05],
        })
        ds = derive_bet_edge_cols(df)
        assert ds["bet_pick_side"][0] == "P1"
        assert ds["bet_edge"][0] == pytest.approx(0.06)

    def test_bet_pick_side_coalesces_null_bet_pred_to_pred_side(self):
        """Null as-bet pick falls back to live pred_side row-wise."""
        from mvp.analysis.dataset import derive_bet_edge_cols

        df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "bet_pred_side": ["P1", None],
            "pred_side": ["P2", "P2"],
            "bet_side": ["P1", "P2"],
            "fav_edge": [0.06, 0.04],
            "dog_edge": [-0.05, -0.03],
        })
        ds = derive_bet_edge_cols(df)
        # m1 uses the (present) as-bet pick P1
        assert ds.filter(pl.col("match_uid") == "m1")["bet_pick_side"][0] == "P1"
        # m2's as-bet pick is null → falls back to pred_side P2
        m2 = ds.filter(pl.col("match_uid") == "m2")
        assert m2["bet_pick_side"][0] == "P2"
        assert m2["bet_edge"][0] == pytest.approx(0.04)

    def test_bet_pick_side_falls_back_when_only_pred_side(self):
        """Pre-rebuild datasets (no bet_pred_side) keep the old behavior."""
        from mvp.analysis.dataset import derive_bet_edge_cols

        ds = derive_bet_edge_cols(self._frame())
        # _frame has pred_side only; bet_pick_side mirrors it
        assert ds["bet_pick_side"].to_list() == ["P1", "P1", "P2"]

    def test_returns_unchanged_when_edge_sources_missing(self):
        from mvp.analysis.dataset import derive_bet_edge_cols

        df = pl.DataFrame({
            "match_uid": ["m1"],
            "bet_side": ["P1"],
            # missing fav_edge, dog_edge, pred_side
        })
        out = derive_bet_edge_cols(df)
        assert "bet_edge" not in out.columns
        assert "bet_edge_open" not in out.columns

    def test_edge_added_when_open_sources_missing(self):
        """Partial inputs: bet_edge derives, bet_edge_open skipped."""
        from mvp.analysis.dataset import derive_bet_edge_cols

        df = pl.DataFrame({
            "match_uid": ["m1"],
            "pred_side": ["P1"],
            "bet_side": ["P1"],
            "fav_edge": [0.05],
            "dog_edge": [-0.02],
            # missing best_opening_odds_p1/p2 and win_prob cols
        })
        out = derive_bet_edge_cols(df)
        assert "bet_edge" in out.columns
        assert out["bet_edge"][0] == pytest.approx(0.05)
        assert "bet_edge_open" not in out.columns

    def test_bet_edge_null_when_bet_side_blank(self):
        """No bet placed → both derived columns null for that row."""
        from mvp.analysis.dataset import derive_bet_edge_cols

        df = pl.DataFrame({
            "match_uid": ["m1"],
            "pred_side": ["P1"],
            "bet_side": [""],
            "fav_edge": [0.05],
            "dog_edge": [-0.02],
            "p1_win_prob": [0.65],
            "p2_win_prob": [0.35],
            "best_opening_odds_p1": [2.00],
            "best_opening_odds_p2": [2.10],
        })
        out = derive_bet_edge_cols(df)
        # bet_side not in {P1, P2} → both derived columns null.
        # Guards against accidentally tagging non-bet rows with a
        # meaningless dog_edge value.
        assert out["bet_edge"][0] is None
        assert out["bet_edge_open"][0] is None
