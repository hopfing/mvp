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


# ---------------------------------------------------------------------------
# fit_imputation
# ---------------------------------------------------------------------------

def fit_imputation(
    X_train: np.ndarray,
    circuit_train: np.ndarray,
    specs: list[ImputeSpec],
    min_circuit_samples: int = 30,
) -> ImputeState:
    """Fit imputation parameters from training data.

    Computes global and per-circuit medians for median-strategy features.
    Circuit buckets with fewer than *min_circuit_samples* non-NaN values
    fall back to the global median.

    Parameters
    ----------
    X_train:
        Training feature matrix, shape ``(n_samples, n_features)``.
    circuit_train:
        Circuit label for each row (e.g. ``"TOUR"``, ``"CHAL"``).
    specs:
        Imputation specs (from :func:`build_impute_specs`).
    min_circuit_samples:
        Minimum non-NaN values per circuit to trust the circuit median.

    Returns
    -------
    ImputeState
        Fitted state ready for :func:`apply_imputation`.
    """
    n_cols = X_train.shape[1]

    # Global medians (NaN-safe); replace any remaining NaN with 0.0
    global_medians = np.nanmedian(X_train, axis=0)
    nan_mask = np.isnan(global_medians)
    global_medians[nan_mask] = 0.0

    # Identify which columns need circuit medians (median strategy only)
    median_col_indices = {s.col_index for s in specs if s.strategy == "median"}

    # Per-circuit medians
    unique_circuits = np.unique(circuit_train)
    circuit_labels = list(unique_circuits)
    circuit_medians: dict[str, np.ndarray] = {}

    for circ in unique_circuits:
        circ_mask = circuit_train == circ
        X_circ = X_train[circ_mask]
        circ_med = np.full(n_cols, np.nan)

        for col_idx in range(n_cols):
            if col_idx not in median_col_indices:
                # Not a median-strategy column — just copy global
                circ_med[col_idx] = global_medians[col_idx]
                continue

            col_vals = X_circ[:, col_idx]
            non_nan_count = int(np.sum(~np.isnan(col_vals)))

            if non_nan_count >= min_circuit_samples:
                circ_med[col_idx] = np.nanmedian(col_vals)
                # If all values in the circuit bucket are NaN (shouldn't
                # happen given count check, but be safe)
                if np.isnan(circ_med[col_idx]):
                    circ_med[col_idx] = global_medians[col_idx]
            else:
                circ_med[col_idx] = global_medians[col_idx]

        circuit_medians[circ] = circ_med

    return ImputeState(
        specs=specs,
        circuit_medians=circuit_medians,
        global_medians=global_medians,
        circuit_labels=circuit_labels,
    )


# ---------------------------------------------------------------------------
# apply_imputation
# ---------------------------------------------------------------------------

def apply_imputation(
    X: np.ndarray,
    circuit: np.ndarray,
    state: ImputeState,
) -> np.ndarray:
    """Apply fitted imputation to a feature matrix.

    Returns a **copy** of *X* with NaN values replaced according to the
    imputation state.

    Parameters
    ----------
    X:
        Feature matrix, shape ``(n_samples, n_features)``.
    circuit:
        Circuit label for each row.
    state:
        Fitted state from :func:`fit_imputation`.

    Returns
    -------
    np.ndarray
        Copy of *X* with NaN values imputed.
    """
    result = X.copy()

    for spec in state.specs:
        col = spec.col_index

        if spec.strategy == "constant":
            nan_mask = np.isnan(result[:, col])
            result[nan_mask, col] = spec.constant
        else:
            # Median strategy: use circuit-specific medians
            nan_mask = np.isnan(result[:, col])
            if not np.any(nan_mask):
                continue

            for row_idx in np.where(nan_mask)[0]:
                circ_label = circuit[row_idx]
                if circ_label in state.circuit_medians:
                    result[row_idx, col] = state.circuit_medians[circ_label][col]
                else:
                    result[row_idx, col] = state.global_medians[col]

    return result


# ---------------------------------------------------------------------------
# subset_impute_state
# ---------------------------------------------------------------------------

def subset_impute_state(
    state: ImputeState,
    col_indices: np.ndarray,
) -> ImputeState:
    """Create an ImputeState remapped for a column subset.

    When the scorer selects a subset of columns from X_wide, the
    ``col_index`` values in the original ImputeState no longer match.
    This function remaps indices and slices median arrays so that
    ``apply_imputation`` works on the narrow matrix.

    Parameters
    ----------
    state:
        Original ImputeState fitted on the full feature set.
    col_indices:
        Column indices into the full feature set (e.g. ``[5, 12, 47]``).

    Returns
    -------
    ImputeState
        New state where ``col_index`` values map to positions in the subset.
    """
    idx_map = {int(old): new for new, old in enumerate(col_indices)}
    new_specs = [
        ImputeSpec(
            col_index=idx_map[spec.col_index],
            strategy=spec.strategy,
            constant=spec.constant,
        )
        for spec in state.specs
        if spec.col_index in idx_map
    ]
    new_circuit_medians = {
        label: medians[col_indices]
        for label, medians in state.circuit_medians.items()
    }
    return ImputeState(
        specs=new_specs,
        circuit_medians=new_circuit_medians,
        global_medians=state.global_medians[col_indices],
        circuit_labels=state.circuit_labels,
    )
