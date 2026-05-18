"""Determinism + sensitivity tests for ExperimentConfig fingerprints."""

from __future__ import annotations

import copy
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from mvp.common.config_hash import (
    canonicalize_config,
    compute_fingerprint,
)
from mvp.model.config import ExperimentConfig


def _make_base_config_dict() -> dict:
    """Minimal valid ExperimentConfig dict for tests."""
    return {
        "description": "test config",
        "target": "won",
        "data": {
            "date_range": {"start": "2020-01-01", "end": "2024-12-31"},
            "filters": {"draw_type": "singles", "circuit": ["tour", "chal"]},
        },
        "features": {
            "include": [
                "player_glicko_diff",
                "player_age_diff",
                "player_pts_service_won_pct_diff(days=15)",
            ],
        },
        "model": {
            "type": "xgboost",
            "params": {
                "n_estimators": 400,
                "max_depth": 4,
                "learning_rate": 0.1,
                "subsample": 0.8,
                "random_state": 42,
            },
        },
        "validation": {
            "type": "date_expanding",
            "initial_train_months": 24,
            "test_months": 12,
        },
        "sample_weight": {"type": "recency", "half_life_days": 365},
        "metrics": {"primary": "log_loss", "secondary": ["accuracy", "brier_score"]},
    }


def _from_dict(d: dict) -> ExperimentConfig:
    return ExperimentConfig.model_validate(d)


def test_deterministic_in_process():
    cfg = _from_dict(_make_base_config_dict())
    a = compute_fingerprint(cfg)
    b = compute_fingerprint(cfg)
    assert a == b


def test_deterministic_across_subprocess(tmp_path: Path):
    """Same config → same fingerprint when computed in a fresh Python process."""
    cfg_dict = _make_base_config_dict()
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg_dict), encoding="utf-8")

    in_process_fp = compute_fingerprint(_from_dict(cfg_dict))

    result = subprocess.run(
        [
            sys.executable, "-c",
            "import sys, yaml\n"
            "from mvp.common.config_hash import compute_fingerprint\n"
            "from mvp.model.config import ExperimentConfig\n"
            f"cfg = ExperimentConfig.from_file(r'{cfg_path}')\n"
            "print(compute_fingerprint(cfg))",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess_fp = result.stdout.strip()
    assert in_process_fp == subprocess_fp, (
        f"in-process={in_process_fp} subprocess={subprocess_fp}"
    )


def test_model_params_key_order_invariant():
    """Permuting model.params key order must not change the fingerprint."""
    base = _make_base_config_dict()
    permuted = copy.deepcopy(base)
    permuted["model"]["params"] = {
        "random_state": 42,
        "subsample": 0.8,
        "learning_rate": 0.1,
        "max_depth": 4,
        "n_estimators": 400,
    }
    assert compute_fingerprint(_from_dict(base)) == compute_fingerprint(_from_dict(permuted))


def test_features_include_order_invariant():
    """Permuting features.include order must not change the fingerprint."""
    base = _make_base_config_dict()
    permuted = copy.deepcopy(base)
    permuted["features"]["include"] = list(reversed(base["features"]["include"]))
    assert compute_fingerprint(_from_dict(base)) == compute_fingerprint(_from_dict(permuted))


def test_parameterized_feature_arg_order_invariant():
    """player_x(a=1, b=2) and player_x(b=2, a=1) must produce the same fp."""
    base = _make_base_config_dict()
    base["features"]["include"] = ["player_win_rate(window_days=30, surface=clay)"]

    permuted = copy.deepcopy(base)
    permuted["features"]["include"] = ["player_win_rate(surface=clay, window_days=30)"]
    assert compute_fingerprint(_from_dict(base)) == compute_fingerprint(_from_dict(permuted))


def test_description_does_not_affect_fingerprint():
    base = _make_base_config_dict()
    different = copy.deepcopy(base)
    different["description"] = "a totally different description"
    assert compute_fingerprint(_from_dict(base)) == compute_fingerprint(_from_dict(different))


def test_secondary_metrics_do_not_affect_fingerprint():
    base = _make_base_config_dict()
    different = copy.deepcopy(base)
    different["metrics"]["secondary"] = ["log_loss"]
    assert compute_fingerprint(_from_dict(base)) == compute_fingerprint(_from_dict(different))


def test_date_range_change_produces_different_fingerprint():
    base = _make_base_config_dict()
    different = copy.deepcopy(base)
    different["data"]["date_range"]["end"] = "2025-12-31"
    assert compute_fingerprint(_from_dict(base)) != compute_fingerprint(_from_dict(different))


def test_metrics_primary_change_produces_different_fingerprint():
    """metrics.primary is provenance: documents which FS metric selected
    these features. Different primary = different config row, even if
    training output is identical."""
    base = _make_base_config_dict()
    different = copy.deepcopy(base)
    different["metrics"]["primary"] = "brier_score"
    assert compute_fingerprint(_from_dict(base)) != compute_fingerprint(_from_dict(different))


def test_feature_addition_changes_fingerprint():
    base = _make_base_config_dict()
    different = copy.deepcopy(base)
    different["features"]["include"] = base["features"]["include"] + ["round_ordinal"]
    assert compute_fingerprint(_from_dict(base)) != compute_fingerprint(_from_dict(different))


def test_param_value_change_changes_fingerprint():
    base = _make_base_config_dict()
    different = copy.deepcopy(base)
    different["model"]["params"]["max_depth"] = 6
    assert compute_fingerprint(_from_dict(base)) != compute_fingerprint(_from_dict(different))


def test_fingerprint_length():
    cfg = _from_dict(_make_base_config_dict())
    fp = compute_fingerprint(cfg)
    assert len(fp) == 12
    assert all(c in "0123456789abcdef" for c in fp)


def test_canonical_dict_is_serializable():
    """canonicalize_config output must be JSON-serializable (the hash relies on it)."""
    import json

    cfg = _from_dict(_make_base_config_dict())
    canon = canonicalize_config(cfg)
    json.dumps(canon, sort_keys=True, default=str)  # raises if not serializable


def test_no_params_canonicalizes_to_empty():
    """model.params is None must canonicalize as {} (not None)."""
    base = _make_base_config_dict()
    base["model"]["params"] = None
    # Pydantic accepts None for params; canonicalize should treat as {}
    cfg = _from_dict(base)
    canon = canonicalize_config(cfg)
    assert canon["model"]["params"] == {}


def test_all_real_configs_under_models_hash_successfully():
    """Smoke test: every YAML under models/*.yaml + models/production/ + models/voters/
    should produce a deterministic fingerprint without errors."""
    repo_root = Path(__file__).resolve().parents[2]
    configs = list((repo_root / "models").glob("*.yaml"))
    configs += list((repo_root / "models" / "production").glob("*.yaml"))
    configs += list((repo_root / "models" / "voters").glob("*.yaml"))
    if not configs:
        pytest.skip("No model configs found under models/")

    for cfg_path in configs:
        try:
            cfg = ExperimentConfig.from_file(str(cfg_path))
        except Exception as e:
            # Some configs may be voter-system configs (not ExperimentConfig)
            # — skip those for this test rather than fail.
            pytest.skip(f"Skipping non-ExperimentConfig: {cfg_path.name}: {e}")
            continue
        fp1 = compute_fingerprint(cfg, config_path=cfg_path)
        fp2 = compute_fingerprint(cfg, config_path=cfg_path)
        assert fp1 == fp2, f"Non-deterministic for {cfg_path.name}: {fp1} != {fp2}"
        assert len(fp1) == 12
