"""Sample weighting utilities for model training."""

import numpy as np

from mvp.model.config import SampleWeightConfig


def compute_sample_weights(
    dates: np.ndarray,
    config: SampleWeightConfig,
) -> np.ndarray:
    """Compute per-sample weights from match dates.

    Args:
        dates: Array of date-like values (datetime64, date objects, etc.).
        config: Weighting configuration.

    Returns:
        Array of weights, same length as dates.
    """
    if config.type == "recency":
        return _recency_weights(dates, config.half_life_days)
    raise ValueError(f"Unknown sample weight type: {config.type}")


def _recency_weights(dates: np.ndarray, half_life_days: int) -> np.ndarray:
    """Exponential recency decay: weight = exp(-ln(2) * age_days / half_life).

    Reference date is the maximum date in the array (most recent match).
    """
    dates_dt = np.array(dates, dtype="datetime64[D]")
    ref_date = dates_dt.max()
    age_days = (ref_date - dates_dt).astype(np.float64)
    return np.exp(-np.log(2) * age_days / half_life_days)
