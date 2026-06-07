"""Shared match-completeness predicate.

Walkovers are flagged in TWO near-disjoint fields: ``reason == "W/O"`` and
``result_type == "walkover"`` (the latter is populated mostly where ``reason``
is null). Historically only ``reason`` was checked, so result_type-flagged
walkovers (~8.9k rows, ~2.8k of them singles) leaked into training/prediction.
Centralizing the predicate so the filters across runner / predictor /
fast_selection can't drift apart again.
"""

import polars as pl

# Columns the predicate reads. Callers must load these for it to be effective;
# the predicate degrades gracefully (ignores) any that are absent.
COMPLETENESS_COLUMNS = ("reason", "result_type")


def is_incomplete_match(columns, exclude_incomplete: bool = False) -> pl.Expr:
    """Boolean expr — True for rows to EXCLUDE as incomplete matches.

    Walkovers (``reason == "W/O"`` OR ``result_type == "walkover"``) are
    excluded always. RET / DEF / UNP are excluded only when
    ``exclude_incomplete`` (MTL or the explicit config flag), preserving prior
    retirement behavior. Built only from columns present in ``columns``, so it
    is safe on frames missing either field.
    """
    cols = set(columns)
    excl = pl.lit(False)
    if "reason" in cols:
        reasons = ["W/O"]
        if exclude_incomplete:
            reasons += ["RET", "DEF", "UNP"]
        excl = excl | pl.col("reason").fill_null("").is_in(reasons)
    if "result_type" in cols:
        excl = excl | (pl.col("result_type") == "walkover").fill_null(False)
    return excl
