"""Content-hash fingerprints for ExperimentConfig and IIDProjectionConfig.

Same (data, features, params, validation, sample_weight, calibration,
metrics.objective) → same fingerprint → shared evaluation artifacts. Any
training-affecting change produces a new fingerprint and a new artifact dir.

IID projection configs get a parallel canonicalizer and their own artifact root
(`projection_evaluations/` vs `model_evaluations/`) — see `iid_fingerprint_dir`.
The classification canonical form is frozen: changing it re-keys every existing
fingerprint dir, so IID support is strictly additive.

Excluded from the hash: `description`, `metrics.primary` / `metrics.secondary`
(descriptive / non-training-affecting). `name` and `selection_history` are
already stripped by `ExperimentConfig.from_yaml` and never reach this module.

See spec at mvp-docs/specs/2026-05-17-model-evaluation-cli.md (and plan at
~/.claude/plans/linked-crafting-wadler.md).
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from mvp.common.base_job import get_data_root
from mvp.model.config import ExperimentConfig
from mvp.model.discovery.sweeps import build_feature_spec, parse_feature_spec

if TYPE_CHECKING:
    from mvp.projection.iid.config import IIDProjectionConfig

logger = logging.getLogger(__name__)

FINGERPRINT_LEN = 12


def _normalize_feature_spec(spec: str) -> str:
    """Canonicalize a parameterized feature spec to a stable string form.

    `win_rate(b=2, a=1)` and `win_rate(a=1,b=2)` both normalize to the same
    output. Non-parameterized features pass through unchanged.
    """
    name, params = parse_feature_spec(spec)
    if not params:
        return name
    sorted_params = {k: params[k] for k in sorted(params.keys())}
    return build_feature_spec(name, sorted_params)


def _canonicalize_features(features_dump: dict | None) -> dict:
    if not features_dump:
        return {}
    include = features_dump.get("include") or []
    compute_only = features_dump.get("compute_only") or []
    return {
        "include": sorted(_normalize_feature_spec(s) for s in include),
        "compute_only": sorted(_normalize_feature_spec(s) for s in compute_only),
    }


# Operational params that change only runtime, not the trained model, so they
# must NOT affect the fingerprint — otherwise tuning the thread count silently
# invalidates every content-addressed artifact and forces a full recompute.
_NON_MODELING_PARAMS = frozenset({"n_jobs"})


def _canonicalize_params(params: dict | None) -> dict:
    """Deep-sort dict keys; leaves values as-is. Drops non-modeling operational
    params (thread count) so they don't enter the fingerprint."""
    if params is None:
        return {}
    return {
        k: _deep_sort(params[k])
        for k in sorted(params.keys())
        if k not in _NON_MODELING_PARAMS
    }


def _deep_sort(obj):
    if isinstance(obj, dict):
        return {k: _deep_sort(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [_deep_sort(x) for x in obj]
    return obj


def _canonicalize_ensemble_base_models(
    base_models: list[dict],
) -> list[dict]:
    """Recursively canonicalize each base config and sort by base fingerprint.

    Each entry has `config` (path string, relative to the repo root / CWD)
    and `weight`. We load the referenced config, compute its canonical dict,
    and replace `config` with the canonical form so the parent fingerprint
    covers the actual base-model content (not just the path string, which
    could be stale).
    """
    out = []
    for ref in base_models:
        ref_path = Path(ref["config"])
        if not ref_path.is_absolute():
            ref_path = ref_path.resolve()
        base_cfg = ExperimentConfig.from_file(str(ref_path))
        base_canon = _canonicalize_config(base_cfg, config_path=ref_path)
        entry = {
            "base_canonical": base_canon,
            "weight": ref.get("weight", 1.0),
        }
        out.append(entry)
    # Sort by base fingerprint so YAML order doesn't affect parent hash
    out.sort(key=lambda e: _hash_dict(e["base_canonical"]))
    return out


def _hash_dict(d: dict) -> str:
    return hashlib.sha256(
        json.dumps(d, sort_keys=True, default=str).encode()
    ).hexdigest()


def _canonicalize_config(
    config: ExperimentConfig,
    config_path: Path | None = None,
) -> dict:
    """Build the canonical dict that gets hashed.

    Drops fields that don't affect training output. Normalizes feature specs
    and sorts dict keys deterministically.
    """
    dump = config.model_dump()

    canon: dict = {}
    canon["target"] = dump.get("target")

    # data
    data = dump.get("data") or {}
    canon["data"] = {
        "date_range": data.get("date_range"),
        "filters": _deep_sort(data.get("filters") or {}),
        "train_filters": _deep_sort(data.get("train_filters") or {}),
        "eval_filters": _deep_sort(data.get("eval_filters") or {}),
    }

    # features
    canon["features"] = _canonicalize_features(dump.get("features"))

    # model
    model = dump.get("model") or {}
    canon_model: dict = {"type": model.get("type")}
    model_type = model.get("type")
    params = model.get("params") or {}
    if model_type == "ensemble":
        base_models = params.get("base_models") or []
        canon_model["params"] = {
            "strategy": params.get("strategy", "average"),
            "base_models": _canonicalize_ensemble_base_models(base_models),
            "meta_features": sorted(params.get("meta_features") or []),
            "meta_model_params": _canonicalize_params(
                params.get("meta_model_params")
            ),
        }
    else:
        canon_model["params"] = _canonicalize_params(params)
    canon["model"] = canon_model

    # validation
    canon["validation"] = _deep_sort(dump.get("validation") or {})

    # sample_weight, calibration
    canon["sample_weight"] = _deep_sort(dump.get("sample_weight"))
    canon["calibration"] = _deep_sort(dump.get("calibration"))

    # metrics.objective drives the run (tuning / early stopping / pruner). primary
    # and secondary are descriptive and do NOT affect the fingerprint.
    metrics = dump.get("metrics") or {}
    canon["metrics_objective"] = metrics.get("objective")

    return canon


def canonicalize_config(
    config: ExperimentConfig,
    config_path: Path | None = None,
) -> dict:
    """Public entry: return the canonical dict that hashes to the fingerprint."""
    return _canonicalize_config(config, config_path=config_path)


def compute_fingerprint(
    config: ExperimentConfig,
    config_path: Path | None = None,
) -> str:
    """SHA-256 of the canonical config, truncated to 12 hex chars."""
    canon = _canonicalize_config(config, config_path=config_path)
    return _hash_dict(canon)[:FINGERPRINT_LEN]


# (key, default) for ServeModelConfig fields added after the first IID
# fingerprints were written. See the loop in `_canonicalize_iid_config`.
#
# ANY new serve_model field belongs here or in the explicit block above. A field
# in neither is invisible to the fingerprint, so two configs differing only in
# it hash the same, land in the same `projection_evaluations/<fp>/`, and the
# second silently overwrites the first's pmf, ledger and trained artifact.
_IID_SERVE_MODEL_OPTIONAL_KEYS: tuple[tuple[str, object], ...] = (
    ("surface_circuit_offset", {}),
    ("posterior_draws", 200),
    ("posterior_seed", 0),
    # type == "two_level" only. Each is a per-component feature set, and two
    # two-level configs differing in ANY of them are different models — so all
    # of them have to reach the hash or the second run silently overwrites the
    # first's artifacts. Feature-list order is meaningful (it fixes the model's
    # column order), so `_deep_sort` must not reach these; the loop below sorts
    # only dicts, which is why they are safe here as lists.
    ("first_in_match_features", []),
    ("first_in_point_features", []),
    ("first_in_params", {}),
    ("win_first_match_features", []),
    ("win_first_point_features", []),
    ("win_second_match_features", []),
    ("win_second_point_features", []),
)


def _canonicalize_iid_config(
    config: "IIDProjectionConfig",
    config_path: Path | None = None,
) -> dict:
    """Canonical dict for an IID projection config (`serve_model:` shape).

    Parallel to `_canonicalize_config`, not an extension of it: the two schemas
    share almost no fields, and the classification canonical form must stay
    byte-identical or every existing fingerprint dir is orphaned.

    `metrics` IS included, covering both its reporting fields (which metric
    families and which total/spread lines get emitted, so two configs differing
    only there produce different `projection.json`) and `objective`, the tune
    target. Including the objective mirrors the classification canonical form's
    `metrics_objective` — it doesn't change the trained model on either side once
    early stopping is off, but the two families should key runs the same way.
    """
    del config_path  # signature parity with _canonicalize_config; unused today
    dump = config.model_dump()

    canon: dict = {"schema": "iid_projection"}

    data = dump.get("data") or {}
    canon["data"] = {
        "date_range": data.get("date_range"),
        "filters": _deep_sort(data.get("filters") or {}),
        "train_filters": _deep_sort(data.get("train_filters") or {}),
        "eval_filters": _deep_sort(data.get("eval_filters") or {}),
    }

    canon["features"] = _canonicalize_features(dump.get("features"))

    sm = dump.get("serve_model") or {}
    canon["serve_model"] = {
        "type": sm.get("type"),
        "model_type": sm.get("model_type"),
        "window": sm.get("window"),
        "clip_min": sm.get("clip_min"),
        "clip_max": sm.get("clip_max"),
        "gap_shrink": sm.get("gap_shrink"),
        "feature_columns": sorted(
            _normalize_feature_spec(s) for s in (sm.get("feature_columns") or [])
        ),
        "match_level_columns": sorted(
            _normalize_feature_spec(s) for s in (sm.get("match_level_columns") or [])
        ),
        # Order is meaningful downstream (it fixes the model's column order), so
        # these two are NOT sorted — a reordered feature list is a different model.
        "match_level_features": [
            _normalize_feature_spec(s) for s in (sm.get("match_level_features") or [])
        ],
        "point_level_features": list(sm.get("point_level_features") or []),
        "params": _canonicalize_params(sm.get("params")),
        "regressor": _deep_sort(sm.get("regressor")),
    }

    # Fields added to ServeModelConfig after the first fingerprints were
    # written. They enter the canonical form ONLY when set away from their
    # default, because adding a key unconditionally changes every existing hash
    # and orphans every dir already on disk — while omitting them entirely lets
    # two configs that differ ONLY in one of these collide on a fingerprint and
    # silently overwrite each other's artifacts, which is what happened when
    # `surface_circuit_offset` landed.
    #
    # A default-valued field cannot change the model, so leaving it out when it
    # is default is safe; a non-default value always changes the model, so
    # including it then is necessary.
    for key, default in _IID_SERVE_MODEL_OPTIONAL_KEYS:
        value = sm.get(key)
        if value is not None and value != default:
            canon["serve_model"][key] = (
                _deep_sort(value) if isinstance(value, dict) else value
            )

    canon["validation"] = _deep_sort(dump.get("validation") or {})
    canon["metrics"] = _deep_sort(dump.get("metrics") or {})

    return canon


def compute_iid_fingerprint(
    config: "IIDProjectionConfig",
    config_path: Path | None = None,
) -> str:
    """SHA-256 of the canonical IID config, truncated to 12 hex chars."""
    return _hash_dict(_canonicalize_iid_config(config, config_path=config_path))[
        :FINGERPRINT_LEN
    ]


# Projection evaluations live under their OWN root, deliberately.
# `mvp.model.evaluation.wipe_stale_evaluations` clears everything in
# model_evaluations/ older than the current ISO week, because classification
# comparability is scoped to the weekly frozen-matches snapshot. IID projection
# runs have no weekly-snapshot semantics and cost far more per run, so they must
# not sit under that broom.
MODEL_EVAL_ROOT = "model_evaluations"
PROJECTION_EVAL_ROOT = "projection_evaluations"


def fingerprint_dir(fp: str, root: str = MODEL_EVAL_ROOT) -> Path:
    return get_data_root() / root / fp


def iid_fingerprint_dir(fp: str) -> Path:
    return fingerprint_dir(fp, root=PROJECTION_EVAL_ROOT)


def write_config_snapshot(
    config: ExperimentConfig,
    fp: str,
    config_path: Path | None = None,
) -> Path:
    """Write the canonical config snapshot to <fp_dir>/config.yaml.

    If the snapshot already exists with different content, raise — this would
    indicate a fingerprint collision (or a bug in canonicalization). Use
    `os.replace` semantics for atomic write.
    """
    return _write_snapshot(
        _canonicalize_config(config, config_path=config_path), fp, MODEL_EVAL_ROOT,
    )


def write_iid_config_snapshot(
    config: "IIDProjectionConfig",
    fp: str,
    config_path: Path | None = None,
) -> Path:
    """`write_config_snapshot` for IID projection configs."""
    return _write_snapshot(
        _canonicalize_iid_config(config, config_path=config_path),
        fp,
        PROJECTION_EVAL_ROOT,
    )


def _write_snapshot(canon: dict, fp: str, root: str) -> Path:
    fp_dir = fingerprint_dir(fp, root=root)
    fp_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = fp_dir / "config.yaml"
    new_text = yaml.safe_dump(canon, sort_keys=True, default_flow_style=False)
    if snapshot_path.exists():
        existing = snapshot_path.read_text(encoding="utf-8")
        if existing != new_text:
            raise RuntimeError(
                f"Fingerprint collision at {fp}: existing config.yaml differs "
                f"from new canonical form. Bug in canonicalization or "
                f"collision (probability ~2^-48 — unlikely)."
            )
        return snapshot_path
    tmp = snapshot_path.with_suffix(".yaml.tmp")
    tmp.write_text(new_text, encoding="utf-8")
    import os

    os.replace(tmp, snapshot_path)
    return snapshot_path


def append_source(
    fp: str,
    source_name: str,
    run_id: str | None,
    root: str = MODEL_EVAL_ROOT,
) -> Path:
    """Append `<source_name>\\t<run_id>\\t<isoformat_ts>` to source.txt.

    Deduped by (source_name, run_id) — re-runs of the same (name, run_id)
    pair are no-ops. Lines are tab-separated; isoformat timestamp is added
    at append time.
    """
    import datetime as _dt

    fp_dir = fingerprint_dir(fp, root=root)
    fp_dir.mkdir(parents=True, exist_ok=True)
    source_path = fp_dir / "source.txt"
    run_id_clean = run_id or "-"

    # Dedup check
    if source_path.exists():
        for line in source_path.read_text(encoding="utf-8").splitlines():
            parts = line.split("\t")
            if len(parts) >= 2 and parts[0] == source_name and parts[1] == run_id_clean:
                return source_path

    ts = _dt.datetime.now().isoformat(timespec="seconds")
    new_line = f"{source_name}\t{run_id_clean}\t{ts}\n"
    with open(source_path, "a", encoding="utf-8") as f:
        f.write(new_line)
    return source_path
