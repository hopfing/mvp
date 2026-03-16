"""Shared imputation module for feature NaN handling.

Features declare their imputation strategy via ``@feature(impute=...)`` in the
registry.  This module reads those declarations and applies imputation
consistently across training and prediction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np

from mvp.model.engine import parse_feature_spec
from mvp.model.registry import FeatureRegistry


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ImputeSpec:
    """Per-feature imputation specification."""

    col_index: int
    strategy: Literal["median", "constant"]
    constant: float | None = None


@dataclass
class ImputeState:
    """Fitted imputation state from a training fold."""

    specs: list[ImputeSpec]
    circuit_medians: dict[str, np.ndarray]  # circuit label -> per-feature medians
    global_medians: np.ndarray  # fallback for sparse circuit buckets
    circuit_labels: list[str]  # known circuit values


# ---------------------------------------------------------------------------
# build_impute_specs
# ---------------------------------------------------------------------------

def build_impute_specs(
    feature_specs: list[str],
    registry: FeatureRegistry,
) -> list[ImputeSpec]:
    """Build imputation specs from feature spec strings and the registry.

    For each feature spec, parses it to extract the base name, looks up
    the ``impute`` field on the registry entry, and returns an
    :class:`ImputeSpec` with the correct strategy.

    Parameters
    ----------
    feature_specs:
        Feature spec strings, e.g. ``["player_win_pct(days=30)"]``.
    registry:
        A :class:`FeatureRegistry` containing definitions for each feature.

    Returns
    -------
    list[ImputeSpec]
        One spec per input feature, with ``col_index`` matching position.
    """
    specs: list[ImputeSpec] = []
    for idx, spec_str in enumerate(feature_specs):
        _prefix, base_name, _full_name, _params = parse_feature_spec(spec_str)
        feature_def = registry.get(base_name)
        impute_val = feature_def.impute

        if impute_val == "median":
            specs.append(ImputeSpec(col_index=idx, strategy="median"))
        else:
            specs.append(
                ImputeSpec(
                    col_index=idx, strategy="constant", constant=float(impute_val)
                )
            )
    return specs
