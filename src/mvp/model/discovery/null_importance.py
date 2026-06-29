"""Null-importance (target-permutation) feature pre-filter.

Trains the configured tree model on the real target and on ``n_runs`` shuffled
copies of it, then keeps only features whose real gain importance significantly
exceeds their own shuffled-target null distribution (a per-feature permutation
test). Used to shrink the candidate pool before stability selection so the
B-resample forward-selection loop isn't spent rediscovering that pure-noise
features are noise.

Gain-based, so it requires a tree model (xgboost / random_forest). Fits are on
the full precomputed frame — this is a relevance screen against random-target
noise, not a generalization estimate, so a single full-data fit per run is the
intended design rather than per-fold scoring.
"""

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from mvp.model.discovery.config import DiscoveryConfig, NullImportanceConfig
from mvp.model.discovery.fast_selection import (
    NAN_TOLERANT_MODEL_TYPES,
    FastForwardSelector,
)
from mvp.model.discovery.importance import gain_importance
from mvp.model.engine import get_feature_columns
from mvp.model.models import get_model

logger = logging.getLogger(__name__)

# Importance methods exposing gain (feature_importances_); null-importance needs it.
_GAIN_MODEL_TYPES = frozenset({"xgboost", "lightgbm", "random_forest"})


def _screen_fingerprint(
    config: DiscoveryConfig,
    ni_config: NullImportanceConfig,
    all_features: list[str],
) -> str:
    """Content hash of everything that affects the screen's p-values.

    Deliberately EXCLUDES ``alpha`` — alpha only partitions kept/dropped from
    the cached p-values, it doesn't change the fits — so re-thresholding with a
    different alpha reuses the cache instead of re-fitting. Includes ``n_runs``
    (it sets the p-value resolution) and the seed (it sets the shuffles).
    """
    payload = {
        "pool": sorted(all_features),
        "model_type": config.model.type,
        "model_params": config.model.params or {},
        "date_range": {
            "start": str(config.data.date_range.start),
            "end": str(config.data.date_range.end),
        },
        "filters": config.data.filters or {},
        "target": config.target,
        "sample_weight": (
            config.sample_weight.model_dump() if config.sample_weight else None
        ),
        "n_runs": ni_config.n_runs,
        "seed": ni_config.random_seed,
    }
    blob = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


def _cache_path(cache_dir: Path | str, fingerprint: str) -> Path:
    return Path(cache_dir) / f"null_importance_{fingerprint}.json"


def _load_screen_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Null-importance cache at %s unreadable (%s); recomputing.", path, e)
        return None


def _save_screen_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    import os

    os.replace(tmp, path)


@dataclass
class NullImportanceResult:
    """Result of a null-importance screen.

    All dicts are keyed by feature spec (index-aligned to the input pool).
    """

    real_importance: dict[str, float]
    null_mean: dict[str, float]
    p_value: dict[str, float]  # fraction of null runs with importance >= real (+1 smoothing)
    kept_features: list[str]
    dropped_features: list[str]
    n_runs: int
    alpha: float


def _prepare_matrix(
    fast: FastForwardSelector, col_indices: np.ndarray, nan_tolerant: bool
) -> np.ndarray:
    """Slice the candidate columns out of X_wide, median-filling if needed.

    XGBoost consumes NaN natively; random_forest does not, so for it we fill
    each column's NaNs with that column's global median (constant across runs,
    so real and shuffled fits see the same imputation).
    """
    X = fast.X_wide[:, col_indices]
    if nan_tolerant:
        return X
    X = X.copy()
    med = np.nanmedian(X, axis=0)
    med = np.where(np.isnan(med), 0.0, med)
    nan_rows, nan_cols = np.where(np.isnan(X))
    X[nan_rows, nan_cols] = np.take(med, nan_cols)
    return X


def _compute_screen(
    fast: FastForwardSelector,
    all_features: list[str],
    config: NullImportanceConfig,
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """Run the real + shuffled-target fits; return per-spec (real, null_mean, p).

    This is the expensive part (n_runs + 1 full-width fits) that the cache in
    run_null_importance exists to avoid re-running.
    """
    model_type = fast.config.model.type
    col_names = get_feature_columns(all_features)  # index-aligned to all_features
    try:
        col_indices = np.array([fast.col_to_idx[c] for c in col_names])
    except KeyError as e:
        raise ValueError(f"null-importance column lookup failed for {e}")

    # Must match the forward FS scorer's NaN handling (fast_selection.py) so the
    # null-importance screen evaluates the same model the scorer does — consult
    # the shared set, not a hardcoded "xgboost".
    nan_tolerant = model_type in NAN_TOLERANT_MODEL_TYPES
    X = _prepare_matrix(fast, col_indices, nan_tolerant)
    y = fast.y
    params = fast.config.model.params or {}
    sw = fast.sample_weights
    fit_kwargs = {"sample_weight": sw} if sw is not None else {}

    logger.info(
        "Null-importance: %d features × %d rows; fitting real model + %d "
        "shuffled-target models (sequential)...",
        len(col_names), X.shape[0], config.n_runs,
    )
    t0 = time.perf_counter()
    real_model = get_model(model_type, params, feature_names=col_names)
    real_model.fit(X, y, **fit_kwargs)
    real_imp = gain_importance(real_model, col_names)
    real_elapsed = time.perf_counter() - t0
    logger.info(
        "  real fit done in %.1fs — rough estimate %.0fs for the %d null fits",
        real_elapsed, real_elapsed * config.n_runs, config.n_runs,
    )

    rng = np.random.default_rng(config.random_seed)
    null_ge = {c: 0 for c in col_names}  # # null runs with importance >= real
    null_sum = {c: 0.0 for c in col_names}
    null_t0 = time.perf_counter()
    for k in range(config.n_runs):
        y_shuf = rng.permutation(y)
        m = get_model(model_type, params, feature_names=col_names)
        m.fit(X, y_shuf, **fit_kwargs)
        ni = gain_importance(m, col_names)
        for c in col_names:
            null_sum[c] += ni[c]
            if ni[c] >= real_imp[c]:
                null_ge[c] += 1
        done = k + 1
        elapsed = time.perf_counter() - null_t0
        avg = elapsed / done
        eta = avg * (config.n_runs - done)
        logger.info(
            "  null fit %d/%d (avg %.1fs/fit, ETA %.0fs)",
            done, config.n_runs, avg, eta,
        )

    real_importance: dict[str, float] = {}
    null_mean: dict[str, float] = {}
    p_value: dict[str, float] = {}
    for spec, c in zip(all_features, col_names):
        real_importance[spec] = real_imp[c]
        null_mean[spec] = null_sum[c] / config.n_runs
        p_value[spec] = (null_ge[c] + 1) / (config.n_runs + 1)
    return real_importance, null_mean, p_value


def run_null_importance(
    fast: FastForwardSelector,
    *,
    all_features: list[str],
    config: NullImportanceConfig,
    cache_dir: Path | str | None = None,
) -> NullImportanceResult:
    """Screen the candidate pool against shuffled-target gain importance.

    If ``cache_dir`` is given, the expensive fits are cached by a fingerprint of
    the screen inputs (pool, model, data, n_runs, seed — NOT alpha). A matching
    fingerprint reuses the cached p-values and skips all fitting; only the
    alpha thresholding is re-applied. This is what makes a resumed or re-tuned
    run skip the 40-60 min screen.

    Args:
        fast: a FastForwardSelector with precompute() already run on the full frame.
        all_features: candidate feature specs to screen.
        config: null-importance configuration.
        cache_dir: directory for the screen cache. None disables caching.

    Returns:
        NullImportanceResult with kept/dropped specs and per-spec diagnostics.
    """
    model_type = fast.config.model.type
    if model_type not in _GAIN_MODEL_TYPES:
        raise ValueError(
            "null-importance is gain-based and requires a tree model "
            f"(xgboost / lightgbm / random_forest); got {model_type!r}. Disable "
            "null_importance for non-tree models."
        )

    fp = _screen_fingerprint(fast.config, config, all_features)
    cache_path = _cache_path(cache_dir, fp) if cache_dir is not None else None

    cached = _load_screen_cache(cache_path) if cache_path is not None else None
    if cached is not None:
        logger.info(
            "Null-importance: reusing cached screen %s — skipping %d fits "
            "(~the full upfront cost).",
            fp, config.n_runs,
        )
        real_importance = cached["real_importance"]
        null_mean = cached["null_mean"]
        p_value = cached["p_value"]
    else:
        real_importance, null_mean, p_value = _compute_screen(
            fast, all_features, config
        )
        if cache_path is not None:
            _save_screen_cache(
                cache_path,
                {
                    "fingerprint": fp,
                    "n_runs": config.n_runs,
                    "real_importance": real_importance,
                    "null_mean": null_mean,
                    "p_value": p_value,
                },
            )
            logger.info("Null-importance: cached screen to %s", cache_path)

    kept = [s for s in all_features if p_value[s] <= config.alpha]
    dropped = [s for s in all_features if p_value[s] > config.alpha]
    logger.info(
        "Null-importance: kept %d / %d features (alpha=%.3f, %d runs)",
        len(kept), len(all_features), config.alpha, config.n_runs,
    )
    return NullImportanceResult(
        real_importance=real_importance,
        null_mean=null_mean,
        p_value=p_value,
        kept_features=kept,
        dropped_features=dropped,
        n_runs=config.n_runs,
        alpha=config.alpha,
    )
