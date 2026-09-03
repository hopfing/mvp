"""Residual-stage serving path (residual-stage plan rows 3 and 4).

A stage is a production entry whose config offsets on its base model's prior
logit (`offset.prior: <stem>` -> `player_prior_logit_<stem>`). Training must
only see the base's out-of-sample prior (rule 1, enforced by
`_assert_prior_is_oof`); serving must fill the prior for pending matches from
the base's live prediction with the right orientation (`_apply_stages` ->
`_predict_raw(fill_prior_logit=...)`), and the shipped probability must be
attributed to the stage.

Fixtures are PAIRED (two rows per match) like the symmetry tests, and the
pending matches deliberately have NO prior rows: if the fill were missing the
offset's finite-input guard would raise, so a passing predict() is the proof.
"""

import importlib
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import joblib
import numpy as np
import polars as pl
import pytest
import yaml


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.elo
    import mvp.model.features.prior
    import mvp.model.features.static

    importlib.reload(mvp.model.features.elo)
    importlib.reload(mvp.model.features.static)
    importlib.reload(mvp.model.features.prior)


def _frame(n_settled: int, n_pending: int) -> tuple[pl.DataFrame, np.ndarray]:
    """Two rows per match. Settled matches in 2024 carry results; pending ones
    in 2025-01 have won=None. Returns the frame and the per-match Elo-based
    p(p1 wins), which the synthetic base-model OOF is built from."""
    rng = np.random.default_rng(7)
    rows: list[dict] = []
    p_true: list[float] = []
    base = date(2024, 1, 1)
    for m in range(n_settled + n_pending):
        pending = m >= n_settled
        p1, p2 = f"P{m % 40:02d}", f"P{(m + 17) % 40:02d}"
        if p1 == p2:
            p2 = f"P{(m + 18) % 40:02d}"
        if pending:
            d = datetime(2025, 1, 10) + timedelta(days=(m - n_settled) // 8)
        else:
            d = datetime.combine(
                base + timedelta(days=m // 3), datetime.min.time()
            )
        elo_a = 1500.0 + rng.normal(0, 120)
        elo_b = 1500.0 + rng.normal(0, 120)
        p_win = 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))
        p_true.append(p_win)
        a_won = None if pending else bool(rng.random() < p_win)
        sides = [
            (p1, p2, a_won, elo_a, elo_b, date(1995, 1, 1), date(1993, 1, 1)),
            (p2, p1, None if pending else (not a_won), elo_b, elo_a,
             date(1993, 1, 1), date(1995, 1, 1)),
        ]
        for pid, oid, won, e_p, e_o, bd_p, bd_o in sides:
            rows.append({
                "match_uid": f"M{m:05d}",
                "player_id": pid,
                "opp_id": oid,
                "effective_match_date": d,
                "won": won,
                "draw_type": "singles",
                "circuit": "tour" if m % 3 else "chal",
                "surface": "Hard" if m % 2 == 0 else "Clay",
                "round": "R32",
                "reason": None,
                "result_type": None if pending else "COMPLETED",
                "sets_played": None if pending else (3 if m % 4 == 0 else 2),
                "best_of": 3,
                "tournament_id": "580",
                "tournament_name": "Test Open",
                "player_first_name": f"F{pid}", "player_last_name": f"L{pid}",
                "opp_first_name": f"F{oid}", "opp_last_name": f"L{oid}",
                "player_display_name": f"F{pid} L{pid}",
                "opp_display_name": f"F{oid} L{oid}",
                "player_elo": e_p, "opp_elo": e_o,
                "player_hard_adj": e_p * 0.01, "opp_hard_adj": e_o * 0.01,
                "player_clay_adj": e_p * -0.01, "opp_clay_adj": e_o * -0.01,
                "player_grass_adj": 0.0, "opp_grass_adj": 0.0,
                "player_serve_elo": e_p + rng.normal(0, 40),
                "opp_serve_elo": e_o + rng.normal(0, 40),
                "player_return_elo": e_p + rng.normal(0, 40),
                "opp_return_elo": e_o + rng.normal(0, 40),
                "player_birth_date": bd_p, "opp_birth_date": bd_o,
                "draw_p1_id": p1,
                "scheduled_datetime": d,
            })
    return pl.DataFrame(rows), np.array(p_true)


def _fold_oof(frame: pl.DataFrame, p_true: np.ndarray) -> pl.DataFrame:
    """Synthetic calibrated fold OOF for the SETTLED rows, both orientations:
    p(this row's player wins) from the Elo truth plus noise. One fold, so
    every row's prior_train_end derives to the earliest settled day - 1."""
    rng = np.random.default_rng(3)
    settled = frame.filter(pl.col("won").is_not_null())
    idx = settled["match_uid"].str.slice(1).cast(pl.Int64).to_numpy()
    is_p1 = (settled["player_id"] == settled["draw_p1_id"]).to_numpy()
    p = np.where(is_p1, p_true[idx], 1 - p_true[idx])
    p = np.clip(p + rng.normal(0, 0.05, size=p.size), 0.02, 0.98)
    return pl.DataFrame({
        "match_uid": settled["match_uid"],
        "player_id": settled["player_id"],
        "effective_match_date": settled["effective_match_date"],
        "fold_idx": 0,
        "y_prob_cal": p,
    })


def _wire_prior(world: dict, stem: str) -> None:
    """Write a one-fold calibrated OOF where the `model` command would have
    left it for `stem`, so `offset.prior: <stem>` resolves. The stem's config
    must already exist in CONFIG_DIRS (call after `_write_configs`)."""
    import mvp.model.features.prior as pr

    src = pr.resolve_prior(stem)
    src.eval_dir.mkdir(parents=True, exist_ok=True)
    _fold_oof(world["frame"], world["p_true"]).write_parquet(src.fold_predictions)


def _lg(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


_XGB = {
    "type": "xgboost",
    "params": {
        "n_estimators": 30, "max_depth": 3, "learning_rate": 0.2,
        "random_state": 42, "n_jobs": 1,
    },
}
_RANGE = {"start": "2024-01-01", "end": "2024-12-31"}
_FILTERS = {"draw_type": "singles", "circuit": ["tour", "chal"]}


def _write_configs(
    tmp_path: Path, *, with_stage: bool, stage_circuit: list[str] | None = None,
) -> Path:
    lead_cfg = {
        "data": {"date_range": _RANGE, "filters": _FILTERS},
        "features": {"include": ["player_elo_surface_diff", "player_age_diff"]},
        "model": _XGB,
        "target": "won",
    }
    (tmp_path / "lead.yaml").write_text(yaml.dump(lead_cfg))
    stage_filters = dict(_FILTERS)
    if stage_circuit is not None:
        stage_filters["circuit"] = stage_circuit
    stage_cfg = {
        # no prior filter: the offset.prior sugar covers training
        "data": {"date_range": _RANGE, "filters": stage_filters},
        "features": {"include": ["player_elo_surface_diff"]},
        "model": _XGB,
        "offset": {"prior": "lead"},
        "target": "won",
    }
    (tmp_path / "stage1.yaml").write_text(yaml.dump(stage_cfg))
    prod = {
        "active": {
            "config": str(tmp_path / "lead.yaml"),
            "artifact": str(tmp_path / "lead.joblib"),
            "train_date_range": _RANGE,
            "filters": _FILTERS,
        },
        "history": [],
    }
    if with_stage:
        prod["stages"] = [{
            "config": str(tmp_path / "stage1.yaml"),
            "artifact": str(tmp_path / "stage1.joblib"),
            "train_date_range": _RANGE,
            "filters": stage_filters,
        }]
    prod_path = tmp_path / "production.yaml"
    prod_path.write_text(yaml.dump(prod))
    return prod_path


@pytest.fixture
def world(tmp_path: Path, monkeypatch):
    """Matches parquet, prior resolution pointed at tmp, configs."""
    import mvp.model.features.prior as pr

    frame, p_true = _frame(n_settled=600, n_pending=40)
    matches = tmp_path / "matches.parquet"
    frame.write_parquet(matches)
    monkeypatch.setattr(pr, "CONFIG_DIRS", (tmp_path,))
    monkeypatch.setattr(pr, "EVALUATIONS_ROOT", tmp_path / "evals")
    pr._cached_frame.cache_clear()
    return {"tmp": tmp_path, "matches": matches, "frame": frame,
            "p_true": p_true}


def _predictor(prod_path: Path, world: dict):
    from mvp.model.predictor import ProductionPredictor

    return ProductionPredictor(
        production_config_path=prod_path,
        matches_path=world["matches"],
        cache_dir=world["tmp"] / "cache",
        predictions_path=world["tmp"] / "predictions.parquet",
    )


class TestTrain:
    def test_trains_lead_then_stage(self, world):
        prod = _write_configs(world["tmp"], with_stage=True)
        _wire_prior(world, "lead")
        _predictor(prod, world).train()

        assert (world["tmp"] / "lead.joblib").exists()
        stage = joblib.load(world["tmp"] / "stage1.joblib")
        assert stage["offset_feature"] == "player_prior_logit_lead"
        assert "player_prior_logit_lead" in stage["feature_cols"]

    def test_refuses_in_sample_prior(self, world, monkeypatch):
        """`build_prior_frame` derives every train end and refuses leaked rows
        itself, so an in-sample prior cannot arrive through the real path; this
        feeds `_assert_prior_is_oof` a hostile frame (train end == match day)
        to prove the predictor-level guard holds on its own."""
        import mvp.model.predictor as mp

        prod = _write_configs(world["tmp"], with_stage=True)
        _wire_prior(world, "lead")
        settled = world["frame"].filter(pl.col("won").is_not_null())
        leaked = settled.select(
            "match_uid", "player_id",
            pl.col("effective_match_date").cast(pl.Date).alias("prior_train_end"),
        )
        monkeypatch.setattr(mp, "_prior_rows", lambda binding: leaked)

        with pytest.raises(ValueError, match="OOF rule"):
            _predictor(prod, world).train()

    def test_lead_without_prior_is_unaffected(self, world):
        prod = _write_configs(world["tmp"], with_stage=False)
        _predictor(prod, world).train()
        assert (world["tmp"] / "lead.joblib").exists()


class TestPredict:
    def test_stage_scores_pending_matches_from_supplied_lead(self, world):
        prod = _write_configs(world["tmp"], with_stage=True)
        _wire_prior(world, "lead")
        predictor = _predictor(prod, world)
        predictor.train()

        preds = predictor.predict()

        assert len(preds) == 40  # every pending match, none had a prior row
        assert set(preds["model_version"].to_list()) == {"stage1"}
        assert "lead_p1_win_prob" in preds.columns
        p1 = preds["p1_win_prob"].to_numpy()
        lead = preds["lead_p1_win_prob"].to_numpy()
        np.testing.assert_allclose(p1 + preds["p2_win_prob"].to_numpy(), 1.0)
        # the stage moved the number (it is not a pass-through) ...
        assert np.abs(p1 - lead).max() > 1e-4
        # ... but conditions on the lead with the right orientation: a flipped
        # sign would drive this strongly negative.
        assert np.corrcoef(_lg(p1), _lg(lead))[0, 1] > 0.9

    def test_no_stages_leaves_predict_unchanged(self, world):
        prod = _write_configs(world["tmp"], with_stage=False)
        predictor = _predictor(prod, world)
        predictor.train()

        preds = predictor.predict()

        assert "lead_p1_win_prob" not in preds.columns
        assert set(preds["model_version"].to_list()) == {"lead"}


class TestFallbacks:
    def test_out_of_domain_matches_keep_the_lead(self, world):
        """A stage scores only its own domain (its config filters, applied at
        serve time); matches outside it keep the lead's number and version."""
        prod = _write_configs(world["tmp"], with_stage=True, stage_circuit=["tour"])
        _wire_prior(world, "lead")
        predictor = _predictor(prod, world)
        predictor.train()

        preds = predictor.predict()

        tour = preds.filter(pl.col("circuit") == "tour")
        chal = preds.filter(pl.col("circuit") == "chal")
        assert len(tour) and len(chal)
        assert set(tour["model_version"].to_list()) == {"stage1"}
        assert set(chal["model_version"].to_list()) == {"lead"}
        np.testing.assert_allclose(
            chal["p1_win_prob"].to_numpy(), chal["lead_p1_win_prob"].to_numpy()
        )
        assert predictor._stage_errors == []

    def test_stage_failure_degrades_to_lead(self, world):
        """The live loop must never lose the lead's predictions to a stage
        problem: a missing stage artifact is reported, not raised."""
        prod = _write_configs(world["tmp"], with_stage=True)
        _wire_prior(world, "lead")
        predictor = _predictor(prod, world)
        predictor.train()
        (world["tmp"] / "stage1.joblib").unlink()

        preds = predictor.predict()

        assert len(preds) == 40
        assert set(preds["model_version"].to_list()) == {"lead"}
        np.testing.assert_allclose(
            preds["p1_win_prob"].to_numpy(), preds["lead_p1_win_prob"].to_numpy()
        )
        assert len(predictor._stage_errors) == 1
        assert "stage1" in predictor._stage_errors[0]


class TestChain:
    def test_second_stage_offsets_on_the_first_by_stem(self, world, caplog):
        """Stage 2 says `offset.prior: stage1`; its prior resolves from
        stage 1's config file to that config's evaluation fingerprint, the
        fold OOF there is spliced into the prior column, the OOF rule is
        checked against it, and at serve time stage 2 conditions on stage
        1's live output (chain), so pending matches ship as stage2."""
        tmp = world["tmp"]
        prod = _write_configs(tmp, with_stage=True)
        (tmp / "stage2.yaml").write_text(yaml.dump({
            "data": {"date_range": _RANGE, "filters": _FILTERS},
            "features": {"include": ["player_elo_surface_diff"]},
            "model": _XGB,
            "offset": {"prior": "stage1"},
            "target": "won",
        }))
        prod_d = yaml.safe_load(prod.read_text())
        prod_d["stages"].append({
            "config": str(tmp / "stage2.yaml"),
            "artifact": str(tmp / "stage2.joblib"),
            "train_date_range": _RANGE,
            "filters": _FILTERS,
        })
        prod.write_text(yaml.dump(prod_d))
        _wire_prior(world, "lead")
        _wire_prior(world, "stage1")

        predictor = _predictor(prod, world)
        with caplog.at_level(logging.INFO, logger="mvp.model.predictor"):
            predictor.train()
        assert (tmp / "stage2.joblib").exists()
        assert any(
            "stage2: OOF prior check passed" in r.message for r in caplog.records
        )
        assert not any("model before it in the chain" in r.message for r in caplog.records)

        preds = predictor.predict()
        assert len(preds) == 40
        assert set(preds["model_version"].to_list()) == {"stage2"}
        assert predictor._stage_errors == []
        p1 = preds["p1_win_prob"].to_numpy()
        lead = preds["lead_p1_win_prob"].to_numpy()
        np.testing.assert_allclose(p1 + preds["p2_win_prob"].to_numpy(), 1.0)
        assert np.corrcoef(_lg(p1), _lg(lead))[0, 1] > 0.9

    def test_stage_pointing_at_the_wrong_base_warns(self, world, caplog):
        """A chain check: stage 2's prior must be the model before it."""
        tmp = world["tmp"]
        prod = _write_configs(tmp, with_stage=True)
        # a stage that offsets on the LEAD's stem while sitting after stage 1
        (tmp / "stage2.yaml").write_text(yaml.dump({
            "data": {"date_range": _RANGE, "filters": _FILTERS},
            "features": {"include": ["player_elo_surface_diff"]},
            "model": _XGB,
            "offset": {"prior": "lead"},
            "target": "won",
        }))
        prod_d = yaml.safe_load(prod.read_text())
        prod_d["stages"].append({
            "config": str(tmp / "stage2.yaml"),
            "artifact": str(tmp / "stage2.joblib"),
            "train_date_range": _RANGE, "filters": _FILTERS,
        })
        prod.write_text(yaml.dump(prod_d))
        _wire_prior(world, "lead")  # both stages offset on "lead"; no stage1 OOF needed

        with caplog.at_level(logging.WARNING, logger="mvp.model.predictor"):
            _predictor(prod, world).train()
        assert any(
            "stage2 offsets on the prior of lead but the model before it in the "
            "chain is stage1" in r.message
            for r in caplog.records
        )
