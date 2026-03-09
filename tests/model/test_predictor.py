"""Tests for production predictor."""


import importlib
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest
import yaml


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    """Re-register features before each test."""
    import mvp.model.features.elo
    import mvp.model.features.static

    importlib.reload(mvp.model.features.elo)
    importlib.reload(mvp.model.features.static)


@pytest.fixture
def sample_matches(tmp_path: Path) -> Path:
    """Create sample matches parquet with Elo and age columns.

    200 rows = 100 matches x 2 player perspectives.
    Matches 0-79 have outcomes (won set), matches 80-99 are pending (won null).
    """
    n_matches = 100
    n = n_matches * 2  # 2 rows per match (player perspectives)

    # Build paired rows: row 2k is p1 perspective, row 2k+1 is p2 perspective
    match_uids, player_ids, opp_ids = [], [], []
    won_values: list[bool | None] = []
    first_names, last_names, opp_fnames, opp_lnames = [], [], [], []
    p_elo, o_elo, p_serve, o_serve, p_ret, o_ret = [], [], [], [], [], []
    dates, surfaces, circuits = [], [], []
    draw_p1_ids: list[str | None] = []
    scheduled_datetimes: list[datetime | None] = []

    for m in range(n_matches):
        p1 = f"P{m % 10}"
        p2 = f"P{(m + 5) % 10}"
        uid = f"M{m}"
        d = datetime(2024, 1, (m % 28) + 1)
        surf = "Hard" if m % 2 == 0 else "Clay"
        circ = "tour" if m % 3 != 0 else "chal"

        sched = datetime(2024, 1, (m % 28) + 1, 10 + (m % 8), 0)

        # Row for p1 perspective
        match_uids.append(uid)
        player_ids.append(p1)
        opp_ids.append(p2)
        first_names.append(f"First{p1}")
        last_names.append(f"Last{p1}")
        opp_fnames.append(f"First{p2}")
        opp_lnames.append(f"Last{p2}")
        dates.append(d)
        surfaces.append(surf)
        circuits.append(circ)
        p_elo.append(1500.0 + m)
        o_elo.append(1500.0 - m)
        p_serve.append(1500.0 + m * 0.5)
        o_serve.append(1500.0 - m * 0.5)
        p_ret.append(1500.0 + m * 0.3)
        o_ret.append(1500.0 - m * 0.3)
        draw_p1_ids.append(p1)
        scheduled_datetimes.append(sched)

        # Row for p2 perspective (swap everything)
        match_uids.append(uid)
        player_ids.append(p2)
        opp_ids.append(p1)
        first_names.append(f"First{p2}")
        last_names.append(f"Last{p2}")
        opp_fnames.append(f"First{p1}")
        opp_lnames.append(f"Last{p1}")
        dates.append(d)
        surfaces.append(surf)
        circuits.append(circ)
        p_elo.append(1500.0 - m)
        o_elo.append(1500.0 + m)
        p_serve.append(1500.0 - m * 0.5)
        o_serve.append(1500.0 + m * 0.5)
        p_ret.append(1500.0 - m * 0.3)
        o_ret.append(1500.0 + m * 0.3)
        draw_p1_ids.append(p1)
        scheduled_datetimes.append(sched)

        # Outcomes
        if m < 80:
            won_values.extend([True, False])
        else:
            won_values.extend([None, None])

    df = pl.DataFrame(
        {
            "match_uid": match_uids,
            "player_id": player_ids,
            "opp_id": opp_ids,
            "effective_match_date": dates,
            "won": won_values,
            "draw_type": ["singles"] * n,
            "circuit": circuits,
            "surface": surfaces,
            "round": ["R32"] * n,
            "tournament_id": ["580"] * n,
            "tournament_name": ["Test Open"] * n,
            "player_first_name": first_names,
            "player_last_name": last_names,
            "opp_first_name": opp_fnames,
            "opp_last_name": opp_lnames,
            "player_elo": p_elo,
            "opp_elo": o_elo,
            "player_hard_adj": [10.0] * n,
            "opp_hard_adj": [-10.0] * n,
            "player_clay_adj": [5.0] * n,
            "opp_clay_adj": [-5.0] * n,
            "player_grass_adj": [0.0] * n,
            "opp_grass_adj": [0.0] * n,
            "player_serve_elo": p_serve,
            "opp_serve_elo": o_serve,
            "player_return_elo": p_ret,
            "opp_return_elo": o_ret,
            "player_birth_date": [date(1995, 6, 15)] * n,
            "opp_birth_date": [date(1998, 3, 20)] * n,
            "draw_p1_id": draw_p1_ids,
            "scheduled_datetime": scheduled_datetimes,
        }
    )
    path = tmp_path / "matches.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def production_config(tmp_path: Path, sample_matches: Path) -> Path:
    """Create a production.yaml pointing to a model config."""
    # Create the model config
    model_config = {
        "data": {
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "filters": {"draw_type": "singles", "circuit": ["tour", "chal"]},
        },
        "features": {
            "include": ["player_elo_surface_diff", "player_svc_elo_diff", "player_ret_elo_diff", "player_age_diff"]
        },
        "model": {"type": "logistic"},
    }
    model_config_path = tmp_path / "model.yaml"
    model_config_path.write_text(yaml.dump(model_config))

    # Create production.yaml
    prod_config = {
        "active": {
            "config": str(model_config_path),
            "artifact": str(tmp_path / "production.joblib"),
            "trained_at": None,
            "train_date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "filters": {"draw_type": "singles", "circuit": ["tour", "chal"]},
        },
        "history": [],
    }
    prod_path = tmp_path / "production.yaml"
    prod_path.write_text(yaml.dump(prod_config))
    return prod_path


class TestTrainProductionModel:
    def test_train_saves_artifact(self, production_config, sample_matches, tmp_path):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()

        artifact_path = Path(predictor.config["active"]["artifact"])
        assert artifact_path.exists()

    def test_train_updates_trained_at(self, production_config, sample_matches, tmp_path):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()

        # Reload config
        with open(production_config) as f:
            config = yaml.safe_load(f)
        assert config["active"]["trained_at"] is not None

    def test_train_creates_model_dir(self, production_config, sample_matches, tmp_path):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()

        artifact_path = Path(predictor.config["active"]["artifact"])
        assert artifact_path.parent.exists()


class TestLoadProductionModel:
    def test_load_after_train(self, production_config, sample_matches, tmp_path):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()

        model, medians, feature_cols, calibrator = predictor.load()
        assert model is not None
        assert len(medians) == 4
        assert len(feature_cols) == 4
        assert calibrator is not None
        assert calibrator.is_fitted

    def test_load_without_train_raises(self, production_config, sample_matches, tmp_path):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        with pytest.raises(FileNotFoundError):
            predictor.load()


class TestPredictMatches:
    def test_predict_returns_one_row_per_match(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()
        predictions = predictor.predict()

        # Each match_uid should appear exactly once
        assert predictions["match_uid"].n_unique() == len(predictions)

    def test_predict_only_pending_matches(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()
        predictions = predictor.predict()

        # Should only contain matches where won was null
        assert len(predictions) > 0
        assert "p1_win_prob" in predictions.columns

    def test_predict_probabilities_valid(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()
        predictions = predictor.predict()

        probs = predictions["p1_win_prob"].to_numpy()
        assert np.all(probs >= 0)
        assert np.all(probs <= 1)

    def test_predict_has_required_columns(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()
        predictions = predictor.predict()

        required = [
            "match_uid",
            "p1_id",
            "p2_id",
            "p1_name",
            "p2_name",
            "p1_win_prob",
            "p2_win_prob",
            "p1_elo",
            "p2_elo",
            "tournament_id",
            "tournament_name",
            "circuit",
            "surface",
            "round",
            "effective_match_date",
            "model_version",
            "predicted_at",
            "scheduled_datetime",
        ]
        for col in required:
            assert col in predictions.columns, f"Missing column: {col}"

    def test_p1_p2_probs_sum_to_one(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()
        predictions = predictor.predict()

        sums = (predictions["p1_win_prob"] + predictions["p2_win_prob"]).to_numpy()
        np.testing.assert_allclose(sums, 1.0, atol=1e-6)

    def test_predict_uses_draw_order(self, tmp_path):
        """Verify draw_p1_id overrides alphabetical ordering."""
        from mvp.model.predictor import ProductionPredictor

        # Create a match where draw order differs from alphabetical
        # Z001 vs A001: alphabetical would pick A001 as p1, but draw says Z001
        rows = []
        for m in range(11):
            if m < 10:
                p1, p2 = f"P{m}", f"P{(m + 5) % 10}"
                d = datetime(2024, 1, (m % 28) + 1)
                won_p1, won_p2 = True, False
                draw_p1 = p1
            else:
                p1, p2 = "Z001", "A001"
                d = datetime(2024, 1, 29)
                won_p1, won_p2 = None, None
                draw_p1 = "Z001"

            surf = "Hard" if m % 2 == 0 else "Clay"
            circ = "tour"
            uid = f"M{m}"
            sched = datetime(2024, 1, (m % 28) + 1, 12, 0)

            for player_id, opp_id, won, elo_sign in [
                (p1, p2, won_p1, 1),
                (p2, p1, won_p2, -1),
            ]:
                rows.append({
                    "match_uid": uid,
                    "player_id": player_id,
                    "opp_id": opp_id,
                    "effective_match_date": d,
                    "won": won,
                    "draw_type": "singles",
                    "circuit": circ,
                    "surface": surf,
                    "round": "R32",
                    "tournament_id": "580",
                    "tournament_name": "Test Open",
                    "player_first_name": f"First{player_id}",
                    "player_last_name": f"Last{player_id}",
                    "opp_first_name": f"First{opp_id}",
                    "opp_last_name": f"Last{opp_id}",
                    "player_elo": 1500.0 + m * elo_sign,
                    "opp_elo": 1500.0 - m * elo_sign,
                    "player_hard_adj": 10.0,
                    "opp_hard_adj": -10.0,
                    "player_clay_adj": 5.0,
                    "opp_clay_adj": -5.0,
                    "player_grass_adj": 0.0,
                    "opp_grass_adj": 0.0,
                    "player_serve_elo": 1500.0 + m * 0.5 * elo_sign,
                    "opp_serve_elo": 1500.0 - m * 0.5 * elo_sign,
                    "player_return_elo": 1500.0 + m * 0.3 * elo_sign,
                    "opp_return_elo": 1500.0 - m * 0.3 * elo_sign,
                    "player_birth_date": date(1995, 6, 15),
                    "opp_birth_date": date(1998, 3, 20),
                    "draw_p1_id": draw_p1,
                    "scheduled_datetime": sched,
                })

        df = pl.DataFrame(rows)
        matches_path = tmp_path / "draw_matches.parquet"
        df.write_parquet(matches_path)

        model_config = {
            "data": {
                "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
                "filters": {"draw_type": "singles"},
            },
            "features": {"include": ["player_elo_surface_diff", "player_svc_elo_diff", "player_ret_elo_diff", "player_age_diff"]},
            "model": {"type": "logistic"},
        }
        model_config_path = tmp_path / "draw_model.yaml"
        model_config_path.write_text(yaml.dump(model_config))

        prod_config = {
            "active": {
                "config": str(model_config_path),
                "artifact": str(tmp_path / "draw.joblib"),
                "trained_at": None,
                "train_date_range": {"start": "2024-01-01", "end": "2024-12-31"},
                "filters": {"draw_type": "singles"},
            },
            "history": [],
        }
        prod_path = tmp_path / "draw_production.yaml"
        prod_path.write_text(yaml.dump(prod_config))

        predictor = ProductionPredictor(
            production_config_path=prod_path,
            matches_path=matches_path,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()
        predictions = predictor.predict()

        # The pending match (M10) should use draw order: Z001 as p1
        row = predictions.filter(pl.col("match_uid") == "M10")
        assert len(row) == 1
        assert row["p1_id"][0] == "Z001"
        assert row["p2_id"][0] == "A001"

    def test_predict_includes_surface_elo(
        self, production_config, sample_matches, tmp_path
    ):
        """Verify p1_elo and p2_elo are in the output with reasonable values."""
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
        )
        predictor.train()
        predictions = predictor.predict()

        assert "p1_elo" in predictions.columns
        assert "p2_elo" in predictions.columns
        # Elo values should be in a reasonable range (base ~1500 +/- adjustments)
        assert predictions["p1_elo"].min() > 1000
        assert predictions["p1_elo"].max() < 2000
        assert predictions["p2_elo"].min() > 1000
        assert predictions["p2_elo"].max() < 2000


class TestSavePredictions:
    def test_save_creates_parquet(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            predictions_path=tmp_path / "predictions.parquet",
        )
        predictor.train()
        predictions = predictor.predict()
        predictor.save_predictions(predictions)

        assert (tmp_path / "predictions.parquet").exists()

    def test_save_appends_new_matches(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            predictions_path=tmp_path / "predictions.parquet",
        )
        predictor.train()
        predictions = predictor.predict()
        n_first = len(predictions)

        predictor.save_predictions(predictions)

        # Save same predictions again — should not duplicate
        predictor.save_predictions(predictions)
        stored = pl.read_parquet(tmp_path / "predictions.parquet")
        assert len(stored) == n_first

    def test_save_updates_changed_predictions(
        self, production_config, sample_matches, tmp_path
    ):
        from mvp.model.predictor import ProductionPredictor

        predictor = ProductionPredictor(
            production_config_path=production_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            predictions_path=tmp_path / "predictions.parquet",
        )
        predictor.train()
        predictions = predictor.predict()
        predictor.save_predictions(predictions)

        # Tamper with a prediction
        tampered = predictions.with_columns(
            pl.lit(0.999).alias("p1_win_prob")
        )
        predictor.save_predictions(tampered)

        stored = pl.read_parquet(tmp_path / "predictions.parquet")
        assert len(stored) == len(predictions)
        # All stored predictions should now have the updated probability
        assert (stored["p1_win_prob"] - 0.999).abs().max() < 1e-4

        # Drift log should exist with one entry per changed prediction
        drift_log = pl.read_parquet(tmp_path / "prediction_drift.parquet")
        assert len(drift_log) == len(predictions)
        assert set(drift_log.columns) == {
            "match_uid", "p1_win_prob", "p2_win_prob",
            "prev_p1_win_prob", "prev_p2_win_prob",
            "prev_predicted_at", "updated_at",
        }
        # New prob should be 0.999, prev should be the original
        assert (drift_log["p1_win_prob"] - 0.999).abs().max() < 1e-4
        assert (drift_log["prev_p1_win_prob"] - predictions["p1_win_prob"]).abs().max() < 1e-4

        # Second drift: change again, log should accumulate
        tampered2 = predictions.with_columns(
            pl.lit(0.500).alias("p1_win_prob")
        )
        predictor.save_predictions(tampered2)
        drift_log2 = pl.read_parquet(tmp_path / "prediction_drift.parquet")
        assert len(drift_log2) == 2 * len(predictions)
        # Latest entries should show prev=0.999, new=0.500
        latest = drift_log2.tail(len(predictions))
        assert (latest["prev_p1_win_prob"] - 0.999).abs().max() < 1e-4
        assert (latest["p1_win_prob"] - 0.500).abs().max() < 1e-4
