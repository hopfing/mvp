"""Shared cal_tiers helpers: tier classification + sidecar lookup.

Used by the betting backtest to label per-pick rows and by the gsheets
writer to surface tier labels in the live bets sheet. The diagnostics
sidecar JSON shape it parses comes from `Diagnostics.compute_all` (see
`mvp.model.diagnostics`) and is emitted next to each production artifact
by `mvp.model.predictor.ProductionPredictor._train_single`.
"""

import json
from pathlib import Path

# Calibration tier thresholds (mirror scripts/review_models.py classifier).
# cal is expressed as a signed proportion (e.g. 0.02 == +2pp).
_TIER_UNDERC_MIN = 0.02
_TIER_OPTIMAL_MIN = 0.0
_TIER_BORDER_MIN = -0.005
_TIER_RISKY_MIN = -0.01


def classify_cal_tier(cal: float | None) -> str | None:
    """Bucket a signed calibration value into UnderC/Optimal/Border/Risky/Danger."""
    if cal is None:
        return None
    if cal >= _TIER_UNDERC_MIN:
        return "UnderC"
    if cal >= _TIER_OPTIMAL_MIN:
        return "Optimal"
    if cal >= _TIER_BORDER_MIN:
        return "Border"
    if cal >= _TIER_RISKY_MIN:
        return "Risky"
    return "Danger"


def extract_circuit_round_lookup(diag: dict) -> dict[tuple[str, str], float]:
    """Pull (circuit, round) -> signed_calibration from a diagnostics-shaped dict."""
    lookup: dict[tuple[str, str], float] = {}
    by_circuit = diag.get("segments", {}).get("by_circuit", {})
    for circuit, circuit_data in by_circuit.items():
        rounds = (circuit_data or {}).get("round", {}) or {}
        for round_name, round_data in rounds.items():
            if not round_data:
                continue
            cal = round_data.get("signed_calibration")
            if cal is None:
                continue
            lookup[(circuit, round_name)] = float(cal)
    return lookup


def load_cal_tiers_from_path(path: Path) -> dict[tuple[str, str], float]:
    """Load and parse a cal_tiers sidecar JSON. Returns {} if the file is missing."""
    if not path.exists():
        return {}
    with open(path) as f:
        diag = json.load(f)
    return extract_circuit_round_lookup(diag)
