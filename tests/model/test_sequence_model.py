"""Tests for SequenceModel — GRU over per-player match history."""

from __future__ import annotations

import pickle
from datetime import date

import numpy as np
import polars as pl
import pytest

from mvp.model.sequence_model import (
    HIST_FEAT_DIM,
    HIST_FEAT_DIM_PROJECTED,
    HISTORY_RAW_COLUMNS,
    SequenceModel,
)


def _make_history_df(n_matches: int = 50, n_players: int = 5, seed: int = 0) -> pl.DataFrame:
    """Build a synthetic history DataFrame with all HISTORY_RAW_COLUMNS."""
    rng = np.random.default_rng(seed)
    base_date = date(2018, 1, 1)
    rows = []
    for i in range(n_matches):
        pid = rng.integers(1, n_players + 1)
        # Ensure opp != player
        opp = rng.integers(1, n_players + 1)
        while opp == pid:
            opp = rng.integers(1, n_players + 1)
        day_offset = i * 7  # one match per week
        match_date = base_date.replace().toordinal() + day_offset
        match_date_obj = date.fromordinal(match_date)
        row = {
            "player_id": int(pid),
            "effective_match_date": match_date_obj,
            "won": int(rng.integers(0, 2)),
            "best_of": int(rng.choice([3, 5])),
            "reason": None,
            "surface": rng.choice(["Hard", "Clay", "Grass", "Carpet"]).item(),
            "tournament_level": rng.choice(["GS", "M1000", "ATP500", "ATP250", "CH125", "FU"]).item(),
            "player_set1_games": int(rng.integers(3, 7)),
            "player_set2_games": int(rng.integers(3, 7)),
            "player_set3_games": None,
            "player_set4_games": None,
            "player_set5_games": None,
            "opp_set1_games": int(rng.integers(3, 7)),
            "opp_set2_games": int(rng.integers(3, 7)),
            "opp_set3_games": None,
            "opp_set4_games": None,
            "opp_set5_games": None,
            "player_elo": float(rng.uniform(1500, 2000)),
            "opp_elo": float(rng.uniform(1500, 2000)),
            "player_glicko_mu": float(rng.uniform(1500, 2000)),
            "player_glicko_rd": float(rng.uniform(50, 200)),
            "opp_glicko_mu": float(rng.uniform(1500, 2000)),
            "opp_glicko_rd": float(rng.uniform(50, 200)),
        }
        rows.append(row)
    df = pl.DataFrame(rows)
    return df


def _make_model_inputs(
    n_rows: int = 30, n_features: int = 5, n_players: int = 5, seed: int = 1,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Build (X, y, params) suitable for SequenceModel.fit.

    X layout: n_features feature columns + [player_id, opp_id, match_date_int].
    Returns the params dict with the three identifier indices pre-set.
    """
    rng = np.random.default_rng(seed)
    feats = rng.standard_normal((n_rows, n_features)).astype(np.float64)
    pids = rng.integers(1, n_players + 1, size=n_rows).astype(np.float64)
    opps = np.array([
        rng.choice([p for p in range(1, n_players + 1) if p != int(pids[i])])
        for i in range(n_rows)
    ], dtype=np.float64)
    # Dates: monotonically increasing days-since-epoch
    base = date(2022, 1, 1).toordinal()
    dates = np.array([base + i * 7 for i in range(n_rows)], dtype=np.float64)
    X = np.hstack([feats, pids.reshape(-1, 1), opps.reshape(-1, 1), dates.reshape(-1, 1)])
    y = rng.integers(0, 2, size=n_rows).astype(int)
    params = {
        "player_id_col_idx": n_features,
        "opp_id_col_idx": n_features + 1,
        "match_date_col_idx": n_features + 2,
        "seq_len": 5,
        "encoder_hidden": 8,
        "encoder_layers": 1,
        "head_hidden": [8],
        "epochs": 3,
        "patience": 2,
        "batch_size": 16,
        "random_state": 0,
    }
    return X, y, params


class TestHistoryLookup:
    def test_no_leakage_strict_less_than(self):
        """_lookup_sequence with before_date=d must return only entries < d."""
        df = _make_history_df(n_matches=40, n_players=3, seed=2)
        model = SequenceModel({
            "player_id_col_idx": 0, "opp_id_col_idx": 1, "match_date_col_idx": 2,
            "seq_len": 10,
        })
        model.set_history_features(df)
        # Pick player 1 and a cutoff that has some entries before
        all_dates = sorted(set(
            d.toordinal() for d in df.filter(pl.col("player_id") == 1)["effective_match_date"].to_list()
        ))
        cutoff = all_dates[len(all_dates) // 2]
        seq, mask = model._lookup_sequence(1, cutoff)
        n_real = int(mask.sum())
        # The non-pad rows are the last n_real of seq; they should all reflect
        # dates strictly less than cutoff. We verify via days_ago_log > 0
        # (positive days-since means strictly earlier).
        days_ago_log_col = HIST_FEAT_DIM_PROJECTED  # last column
        non_pad = seq[-n_real:] if n_real > 0 else seq[:0]
        # days_ago_log encodes log1p(cutoff - hist_date) / 8.0, which must be > 0
        # for strictly earlier dates.
        if n_real > 0:
            assert (non_pad[:, days_ago_log_col] > 0).all()

    def test_padding_short_history(self):
        """Player with 3 matches, seq_len=10 → 7 padded entries at front, mask correct."""
        # Build a history df where player 1 has exactly 3 matches
        rows = []
        for i in range(3):
            rows.append({
                "player_id": 1,
                "effective_match_date": date(2020, 1, 1 + i * 7),
                "won": 1,
                "best_of": 3,
                "reason": None,
                "surface": "Hard",
                "tournament_level": "ATP250",
                "player_set1_games": 6, "player_set2_games": 4,
                "player_set3_games": None, "player_set4_games": None, "player_set5_games": None,
                "opp_set1_games": 3, "opp_set2_games": 6,
                "opp_set3_games": None, "opp_set4_games": None, "opp_set5_games": None,
                "player_elo": 1800.0, "opp_elo": 1800.0,
                "player_glicko_mu": 1800.0, "player_glicko_rd": 100.0,
                "opp_glicko_mu": 1800.0, "opp_glicko_rd": 100.0,
            })
        df = pl.DataFrame(rows)
        model = SequenceModel({
            "player_id_col_idx": 0, "opp_id_col_idx": 1, "match_date_col_idx": 2,
            "seq_len": 10,
        })
        model.set_history_features(df)
        cutoff = date(2020, 12, 31).toordinal()
        seq, mask = model._lookup_sequence(1, cutoff)
        assert seq.shape == (10, HIST_FEAT_DIM)
        assert mask.shape == (10,)
        assert int(mask.sum()) == 3
        # Front 7 must be padded (mask 0); last 3 must be real (mask 1)
        assert mask[:7].sum() == 0
        assert mask[7:].sum() == 3
        # Padded rows should be all zeros
        assert (seq[:7] == 0).all()

    def test_cold_start_unknown_player(self):
        """Unknown player_id → all-zero seq, all-zero mask."""
        df = _make_history_df(n_matches=10, n_players=2)
        model = SequenceModel({
            "player_id_col_idx": 0, "opp_id_col_idx": 1, "match_date_col_idx": 2,
            "seq_len": 5,
        })
        model.set_history_features(df)
        seq, mask = model._lookup_sequence(99999, date(2023, 1, 1).toordinal())
        assert (seq == 0).all()
        assert (mask == 0).all()


class TestFitPredict:
    def test_fit_predict_shapes(self):
        X, y, params = _make_model_inputs(n_rows=40, n_features=4, n_players=4)
        model = SequenceModel(params)
        df = _make_history_df(n_matches=60, n_players=4, seed=99)
        model.set_history_features(df)
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (40,)
        assert (probs >= 0).all() and (probs <= 1).all()

    def test_id_cols_required_raises(self):
        """Missing identifier indices raises a clear error."""
        model = SequenceModel({})  # no indices
        X = np.random.randn(10, 5)
        y = np.random.randint(0, 2, size=10)
        with pytest.raises(ValueError, match="player_id_col_idx"):
            model.fit(X, y)


class TestSerialization:
    def test_pickle_round_trip_predictions_identical(self):
        X, y, params = _make_model_inputs(n_rows=30, n_features=4, n_players=4)
        model = SequenceModel(params)
        df = _make_history_df(n_matches=60, n_players=4, seed=99)
        model.set_history_features(df)
        model.fit(X, y)
        probs_before = model.predict_proba(X)

        # Round-trip via pickle
        blob = pickle.dumps(model)
        loaded = pickle.loads(blob)
        probs_after = loaded.predict_proba(X)

        np.testing.assert_allclose(probs_before, probs_after, atol=1e-6)


class TestProjection:
    def test_projected_dim_matches_constant(self):
        df = _make_history_df(n_matches=10)
        from mvp.model.sequence_model import _project_history_features
        pids, dates, feats = _project_history_features(df)
        assert pids.shape == (10,)
        assert dates.shape == (10,)
        assert feats.shape == (10, HIST_FEAT_DIM_PROJECTED)

    def test_missing_columns_raises(self):
        """set_history_features with missing raw columns raises."""
        bad_df = pl.DataFrame({"player_id": [1, 2], "effective_match_date": [date(2020, 1, 1), date(2020, 1, 2)]})
        model = SequenceModel({
            "player_id_col_idx": 0, "opp_id_col_idx": 1, "match_date_col_idx": 2,
            "seq_len": 5,
        })
        with pytest.raises(ValueError, match="missing required columns"):
            model.set_history_features(bad_df)
