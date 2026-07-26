"""Fingerprint tests for IID projection configs.

Companion to test_config_hash.py, which covers the classification schema. These
two canonicalizers are deliberately parallel implementations, not one generalized
one — see the module docstring in mvp.common.config_hash.
"""

from __future__ import annotations

import copy
import json

from mvp.common.config_hash import (
    PROJECTION_EVAL_ROOT,
    _canonicalize_iid_config,
    compute_iid_fingerprint,
    fingerprint_dir,
    iid_fingerprint_dir,
)
from mvp.projection.iid.config import IIDProjectionConfig


def _make_base_config_dict() -> dict:
    return {
        "description": "test iid config",
        "data": {
            "date_range": {"start": "2023-01-01", "end": "2026-01-01"},
            "filters": {"draw_type": "singles", "circuit": ["tour", "chal"]},
        },
        "features": {
            "include": [
                "player_tourn_pts_service_won_per_game",
                "opp_tourn_pts_service_won_per_game",
                "total_games_sum(days=365)",
            ],
        },
        "serve_model": {
            "type": "score_state",
            "model_type": "xgboost",
            "match_level_features": [
                "player_tourn_pts_service_won_per_game",
                "total_games_sum(days=365)",
            ],
            "point_level_features": ["sets_won_asymmetry", "set_score_asymmetry"],
            "params": {"n_estimators": 100, "max_depth": 3, "n_jobs": 4},
        },
        "validation": {
            "type": "date_expanding",
            "initial_train_months": 12,
            "test_months": 6,
        },
    }


def _from_dict(d: dict) -> IIDProjectionConfig:
    return IIDProjectionConfig.model_validate(d)


def _fp(d: dict) -> str:
    return compute_iid_fingerprint(_from_dict(d))


def test_deterministic():
    d = _make_base_config_dict()
    assert _fp(d) == _fp(copy.deepcopy(d))


def test_canonical_dict_is_serializable():
    canon = _canonicalize_iid_config(_from_dict(_make_base_config_dict()))
    json.dumps(canon, sort_keys=True, default=str)


def test_param_key_order_invariant():
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["serve_model"]["params"] = {
        "max_depth": 3, "n_jobs": 4, "n_estimators": 100,
    }
    assert _fp(a) == _fp(b)


def test_description_does_not_affect_fingerprint():
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["description"] = "something else entirely"
    assert _fp(a) == _fp(b)


def test_n_jobs_does_not_affect_fingerprint():
    """Thread count is operational — it must not re-key the artifact dir."""
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["serve_model"]["params"]["n_jobs"] = 16
    assert _fp(a) == _fp(b)


def test_hp_change_produces_different_fingerprint():
    """The whole point: two HP sets of one config must not share an artifact dir."""
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["serve_model"]["params"]["max_depth"] = 6
    assert _fp(a) != _fp(b)


def test_gap_shrink_changes_fingerprint():
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["serve_model"]["gap_shrink"] = 0.7
    assert _fp(a) != _fp(b)


def test_match_feature_order_changes_fingerprint():
    """Match-level feature ORDER fixes the model's column order, so a reordered
    list is a different model — unlike features.include, which is set-like."""
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["serve_model"]["match_level_features"] = list(
        reversed(a["serve_model"]["match_level_features"])
    )
    assert _fp(a) != _fp(b)


def test_features_include_order_invariant():
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["features"]["include"] = list(reversed(a["features"]["include"]))
    assert _fp(a) == _fp(b)


def test_point_feature_change_produces_different_fingerprint():
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["serve_model"]["point_level_features"] = ["sets_won_asymmetry"]
    assert _fp(a) != _fp(b)


def test_validation_change_produces_different_fingerprint():
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["validation"]["test_months"] = 3
    assert _fp(a) != _fp(b)


def test_metrics_lines_change_produces_different_fingerprint():
    """metrics doesn't change the trained model, but it selects which line
    metrics get emitted — so it must not share an artifact dir."""
    a = _make_base_config_dict()
    b = copy.deepcopy(a)
    b["metrics"] = {"total_lines": [20.5, 21.5]}
    assert _fp(a) != _fp(b)


def test_objective_is_in_the_fingerprint():
    """Mirrors the classification canonical form's `metrics_objective`. Neither
    changes the trained model once early stopping is off; both key the run."""
    a = _make_base_config_dict()
    a["metrics"] = {"objective": ["iid_crps_total_games"]}
    b = copy.deepcopy(a)
    b["metrics"] = {"objective": ["mae"]}
    assert _fp(a) != _fp(b)


def test_scalar_objective_equals_its_one_element_list():
    a = _make_base_config_dict()
    a["metrics"] = {"objective": "iid_crps_total_games"}
    b = copy.deepcopy(a)
    b["metrics"] = {"objective": ["iid_crps_total_games"]}
    assert _fp(a) == _fp(b)


def test_projection_eval_root_is_separate_from_model_evaluations():
    """IID artifacts must not live under model_evaluations/ — wipe_stale_evaluations
    clears that root weekly and would delete far more expensive runs."""
    fp = _fp(_make_base_config_dict())
    assert iid_fingerprint_dir(fp).parent.name == PROJECTION_EVAL_ROOT
    assert iid_fingerprint_dir(fp) != fingerprint_dir(fp)


def test_fingerprint_length():
    fp = _fp(_make_base_config_dict())
    assert len(fp) == 12
    assert all(c in "0123456789abcdef" for c in fp)


def test_real_promoted_config_hashes(tmp_path):
    """The config the harness was built for must hash without error."""
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    cfg_path = repo_root / "projections" / "totals_de1206_23_crt.yaml"
    if not cfg_path.exists():
        import pytest

        pytest.skip("promoted totals config not present")
    cfg = IIDProjectionConfig.from_file(str(cfg_path))
    assert len(compute_iid_fingerprint(cfg, config_path=cfg_path)) == 12
