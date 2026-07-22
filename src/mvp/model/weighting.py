"""Sample weighting utilities for model training."""

import numpy as np
import polars as pl

from mvp.model.config import SampleWeightConfig


def sample_weights_from_frame(
    df: pl.DataFrame, config: SampleWeightConfig
) -> np.ndarray:
    """Compute per-row training weights from a training frame.

    Reads the columns each mode needs directly from ``df`` —
    ``effective_match_date`` for ``recency``, the columns referenced by the
    ``group`` rules — so callers don't repeat the column-selection logic. Thin
    wrapper over :func:`compute_sample_weights`.
    """
    dates = df["effective_match_date"].to_numpy()
    attributes = None
    if config.type == "group":
        attributes = {}
        for col in config.referenced_columns():
            if col not in df.columns:
                raise ValueError(
                    f"sample_weight column {col!r} is not in the training "
                    "frame; ensure it is among the loaded columns for this path."
                )
            attributes[col] = df[col].to_numpy()
    return compute_sample_weights(dates, config, attributes=attributes)


def compute_sample_weights(
    dates: np.ndarray,
    config: SampleWeightConfig,
    attributes: dict[str, np.ndarray] | None = None,
) -> np.ndarray:
    """Compute per-sample weights.

    Args:
        dates: Array of date-like values (datetime64, date objects, etc.), one
            per row. Used by the ``recency`` mode.
        config: Weighting configuration.
        attributes: Per-row values keyed by column name, for the columns the
            ``group`` rules reference. Required for the ``group`` mode; ignored
            otherwise.

    Returns:
        Array of weights, same length as dates.
    """
    if config.type == "recency":
        return _recency_weights(dates, config.half_life_days)
    if config.type == "group":
        if attributes is None:
            raise ValueError(
                "group sample weighting requires per-row column values; call "
                "sample_weights_from_frame or pass attributes="
            )
        return _group_weights(attributes, config.rules, config.default_weight)
    raise ValueError(f"Unknown sample weight type: {config.type}")


def _recency_weights(dates: np.ndarray, half_life_days: int) -> np.ndarray:
    """Exponential recency decay: weight = exp(-ln(2) * age_days / half_life).

    Reference date is the maximum date in the array (most recent match).
    """
    dates_dt = np.array(dates, dtype="datetime64[D]")
    ref_date = dates_dt.max()
    age_days = (ref_date - dates_dt).astype(np.float64)
    return np.exp(-np.log(2) * age_days / half_life_days)


def _group_weights(
    attributes: dict[str, np.ndarray],
    rules: list,
    default_weight: float,
) -> np.ndarray:
    """Per-row weight from an ordered list of match rules.

    Every row starts at ``default_weight``. For each rule (in order), rows that
    match all of its ``where`` conditions and haven't been claimed by an earlier
    rule are set to the rule's weight — so the first matching rule wins.
    """
    if not attributes:
        raise ValueError("group weighting requires at least one column")
    n = len(next(iter(attributes.values())))
    out = np.full(n, float(default_weight), dtype=np.float64)
    assigned = np.zeros(n, dtype=bool)
    for rule in rules:
        mask = np.ones(n, dtype=bool)
        for col, val in rule.where.items():
            mask &= np.asarray(attributes[col]) == val
        newly = mask & ~assigned
        out[newly] = float(rule.weight)
        assigned |= mask
    return out
