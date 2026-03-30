"""Shared imputation module for feature NaN handling.

Features declare their imputation strategy via ``@feature(impute=...)`` in the
registry.  This module reads those declarations and applies imputation
consistently across training and prediction.

Two-phase imputation:

1. **Phase A** — impute base features using median (circuit-stratified) or
   constant strategies, exactly as before.
2. **Phase B** — recompute derived features (diffs, matchups) from the now-
   imputed base columns.  This recovers partial information that would
   otherwise be lost when one side has data and the other doesn't.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import numpy as np

from mvp.model.engine import build_column_name, get_feature_columns, parse_feature_spec
from mvp.model.registry import FeatureRegistry

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RecomputeInfo:
    """Indices of the player/opp base columns used to recompute a derived feature."""

    player_base_idx: int  # column index in augmented matrix
    opp_base_idx: int     # column index in augmented matrix


@dataclass
class ImputeSpec:
    """Per-feature imputation specification."""

    col_index: int
    strategy: Literal["median", "constant", "recompute"]
    constant: float | None = None
    recompute: RecomputeInfo | None = None


@dataclass
class ImputeState:
    """Fitted imputation state from a training fold."""

    specs: list[ImputeSpec]
    circuit_medians: dict[str, np.ndarray]  # circuit label -> per-feature medians
    global_medians: np.ndarray  # fallback for sparse circuit buckets
    circuit_labels: list[str]  # known circuit values


@dataclass
class ImputeBuildResult:
    """Result of :func:`build_imputation` — specs plus auxiliary column info."""

    specs: list[ImputeSpec]
    aux_base_col_names: list[str] = field(default_factory=list)
    n_model_features: int = 0


# ---------------------------------------------------------------------------
# build_impute_specs  (kept for backward compat)
# ---------------------------------------------------------------------------

def build_impute_specs(
    feature_specs: list[str],
    registry: FeatureRegistry,
) -> list[ImputeSpec]:
    """Build imputation specs from feature spec strings and the registry.

    Legacy interface — returns flat specs without recompute support.
    Prefer :func:`build_imputation` for new code.
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
# build_imputation
# ---------------------------------------------------------------------------

def build_imputation(
    feature_specs: list[str],
    registry: FeatureRegistry,
) -> ImputeBuildResult:
    """Build imputation specs with recompute support for derived features.

    For each feature spec, determines the imputation strategy:

    * **median / constant** — same as :func:`build_impute_specs`.
    * **recompute** — the feature is a diff/matchup whose value can be
      reconstructed from imputed base columns (``player_base - opp_base``).

    Auto-detection criteria for recompute:
        ``depends_on`` non-empty AND ``mirror=False`` AND ``impute`` is numeric.

    Returns an :class:`ImputeBuildResult` containing specs for the augmented
    matrix (model features + auxiliary base columns) and the list of extra
    column names the caller must extract alongside the model features.
    """
    n_model = len(feature_specs)
    model_col_names = get_feature_columns(feature_specs)
    col_name_to_idx: dict[str, int] = {
        name: i for i, name in enumerate(model_col_names)
    }

    aux_col_names: list[str] = []
    model_specs: list[ImputeSpec] = []
    aux_specs: list[ImputeSpec] = []

    for idx, spec_str in enumerate(feature_specs):
        _prefix, base_name, _full_name, params = parse_feature_spec(spec_str)
        feature_def = registry.get(base_name)
        impute_val = feature_def.impute

        # Check if this is a recomputable derived feature
        is_recomputable = (
            feature_def.depends_on
            and not feature_def.mirror
            and impute_val != "median"
        )

        if is_recomputable:
            deps = feature_def.depends_on
            if len(deps) == 1:
                player_dep, opp_dep = deps[0], deps[0]
            elif len(deps) == 2:
                player_dep, opp_dep = deps[0], deps[1]
            else:
                # >2 deps — can't auto-resolve, fall back to constant
                model_specs.append(
                    ImputeSpec(
                        col_index=idx,
                        strategy="constant",
                        constant=float(impute_val),
                    )
                )
                continue

            player_col = build_column_name(f"player_{player_dep}", params)
            opp_col = build_column_name(f"opp_{opp_dep}", params)

            # Resolve or add player base column
            if player_col not in col_name_to_idx:
                aux_idx = n_model + len(aux_col_names)
                col_name_to_idx[player_col] = aux_idx
                aux_col_names.append(player_col)
                base_def = registry.get(player_dep)
                if base_def.impute == "median":
                    aux_specs.append(
                        ImputeSpec(col_index=aux_idx, strategy="median")
                    )
                else:
                    aux_specs.append(
                        ImputeSpec(
                            col_index=aux_idx,
                            strategy="constant",
                            constant=float(base_def.impute),
                        )
                    )

            # Resolve or add opp base column
            if opp_col not in col_name_to_idx:
                aux_idx = n_model + len(aux_col_names)
                col_name_to_idx[opp_col] = aux_idx
                aux_col_names.append(opp_col)
                base_def = registry.get(opp_dep)
                if base_def.impute == "median":
                    aux_specs.append(
                        ImputeSpec(col_index=aux_idx, strategy="median")
                    )
                else:
                    aux_specs.append(
                        ImputeSpec(
                            col_index=aux_idx,
                            strategy="constant",
                            constant=float(base_def.impute),
                        )
                    )

            model_specs.append(
                ImputeSpec(
                    col_index=idx,
                    strategy="recompute",
                    recompute=RecomputeInfo(
                        player_base_idx=col_name_to_idx[player_col],
                        opp_base_idx=col_name_to_idx[opp_col],
                    ),
                )
            )
        elif impute_val == "median":
            model_specs.append(ImputeSpec(col_index=idx, strategy="median"))
        else:
            model_specs.append(
                ImputeSpec(
                    col_index=idx,
                    strategy="constant",
                    constant=float(impute_val),
                )
            )

    return ImputeBuildResult(
        specs=model_specs + aux_specs,
        aux_base_col_names=aux_col_names,
        n_model_features=n_model,
    )


# ---------------------------------------------------------------------------
# augmented_col_indices
# ---------------------------------------------------------------------------

def augmented_col_indices(
    model_col_indices: np.ndarray,
    specs: list[ImputeSpec],
) -> tuple[np.ndarray, int]:
    """Compute augmented column indices: model features + required aux bases.

    For each recompute spec whose derived column is in *model_col_indices*,
    adds the player/opp base indices if they aren't already present.

    Returns ``(augmented_indices, n_model)`` where model features come first
    so that stripping is simply ``X[:, :n_model]``.
    """
    model_set = set(model_col_indices.tolist())
    aux_needed: list[int] = []
    aux_seen: set[int] = set()

    for spec in specs:
        if (
            spec.col_index in model_set
            and spec.strategy == "recompute"
            and spec.recompute is not None
        ):
            for base_idx in (
                spec.recompute.player_base_idx,
                spec.recompute.opp_base_idx,
            ):
                if base_idx not in model_set and base_idx not in aux_seen:
                    aux_needed.append(base_idx)
                    aux_seen.add(base_idx)

    if not aux_needed:
        return model_col_indices, len(model_col_indices)

    return (
        np.concatenate([model_col_indices, np.array(aux_needed)]),
        len(model_col_indices),
    )


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
    """Apply fitted imputation to a feature matrix (two-phase).

    **Phase A** fills NaN in base features using median/constant strategies.
    **Phase B** unconditionally recomputes derived features from the imputed
    base columns (``player_base - opp_base``).

    Returns a **copy** of *X* with NaN values replaced.
    """
    result = X.copy()

    # Phase A: constant and median imputation
    constant_specs = [s for s in state.specs if s.strategy == "constant"]
    median_cols = [s.col_index for s in state.specs if s.strategy == "median"]

    # Constant imputation (vectorized, circuit-independent)
    for spec in constant_specs:
        col = spec.col_index
        nan_mask = np.isnan(result[:, col])
        if nan_mask.any():
            result[nan_mask, col] = spec.constant

    # Median imputation (vectorized per circuit)
    if median_cols:
        unique_circuits = set(circuit)
        for circ_label in unique_circuits:
            circ_row_mask = circuit == circ_label
            medians = state.circuit_medians.get(circ_label, state.global_medians)
            for col in median_cols:
                nan_and_circ = circ_row_mask & np.isnan(result[:, col])
                if nan_and_circ.any():
                    result[nan_and_circ, col] = medians[col]

    # Phase B: recompute derived features from imputed bases
    for spec in state.specs:
        if spec.strategy == "recompute" and spec.recompute is not None:
            result[:, spec.col_index] = (
                result[:, spec.recompute.player_base_idx]
                - result[:, spec.recompute.opp_base_idx]
            )

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

    For recompute specs, both base column indices must be present in
    *col_indices* for the spec to be included.

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
    new_specs: list[ImputeSpec] = []

    for spec in state.specs:
        if spec.col_index not in idx_map:
            continue

        if spec.strategy == "recompute" and spec.recompute is not None:
            p_idx = spec.recompute.player_base_idx
            o_idx = spec.recompute.opp_base_idx
            if p_idx not in idx_map or o_idx not in idx_map:
                continue
            new_specs.append(
                ImputeSpec(
                    col_index=idx_map[spec.col_index],
                    strategy="recompute",
                    recompute=RecomputeInfo(
                        player_base_idx=idx_map[p_idx],
                        opp_base_idx=idx_map[o_idx],
                    ),
                )
            )
        else:
            new_specs.append(
                ImputeSpec(
                    col_index=idx_map[spec.col_index],
                    strategy=spec.strategy,
                    constant=spec.constant,
                )
            )

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
