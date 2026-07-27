"""Cleaned match-duration expressions shared by the workload features.

``duration_seconds`` is not usable straight from the feed:

  * ~12% of tour+chal singles rows carry a junk sub-20-minute value. These are
    rows whose score never parsed — 29,100 of the 31,788 are flagged
    ``result_type='completed'`` yet have a null ``sets_played``. Gated on
    ``sets_played`` being present rather than on any retirement flag.
  * A ~0.4% tail carries wall-clock instead of court time, from rain-suspended
    matches that resumed on a later day; the extreme is 1,861 minutes. Caught by
    winsorizing minutes-per-point, which identifies a suspension regardless of
    ``best_of`` or match format. A ``best_of`` ceiling was considered and
    rejected: it cannot separate a genuine five-set marathon from a suspension.

Retirements keep their true (short) duration in volume sums — the player
genuinely spent less time on court — but are masked out of the rate features,
where warm-up and changeover overhead dominates a truncated match and would
drag the grind rate down for reasons unrelated to how the points were played.
"""

import polars as pl

# Whole-match point count. ``pts_total_pts_played`` is service + return points,
# i.e. every point in the match, and is identical for both players — summing
# the player and opponent columns double-counts.
MATCH_POINTS = pl.col("pts_total_pts_played").cast(pl.Float64)

# Corpus median minutes-per-point is 0.678 on cleaned tour+chal singles, with
# 1st/99th percentiles of 0.506/0.924. These bounds sit far outside that range
# so only physically impossible values are pulled in (0.38% of rows).
MPP_LO, MPP_HI = 0.2, 2.0

# Fixed scaling constant for the excess-minutes feature, set once from the
# corpus median above. It is a hardcoded literal, never refit per split, so it
# introduces no train/test coupling — one median of a bounded ratio, applied
# identically to every row, in a feature that is affine in it.
# Derived by: scripts/probe_points_semantics.py (tour+chal singles, cleaned rows).
EXPECTED_MINUTES_PER_POINT = 0.678


def is_retirement() -> pl.Expr:
    """True when either player retired, so the clock stopped early."""
    return (pl.col("reason").fill_null("") == "RET") | (
        pl.col("result_type").fill_null("") == "retirement"
    )


def match_minutes() -> pl.Expr:
    """On-court minutes, cleaned. Null where the value cannot be trusted.

    Used for volume sums, where a retirement's real short duration is the
    correct contribution to workload.
    """
    minutes = pl.col("duration_seconds").cast(pl.Float64) / 60.0
    pts = MATCH_POINTS
    scored = pl.col("sets_played").is_not_null() & (minutes > 0)
    lo, hi = MPP_LO * pts, MPP_HI * pts
    winsorized = (
        pl.when(pts.is_null() | (pts <= 0))
        .then(minutes)
        .when(minutes < lo)
        .then(lo)
        .when(minutes > hi)
        .then(hi)
        .otherwise(minutes)
    )
    return pl.when(scored).then(winsorized).otherwise(None)


def match_minutes_for_rates() -> pl.Expr:
    """``match_minutes`` with retirements additionally masked to null.

    Use for any minutes-per-X rate: a 14-minute retirement is dominated by
    fixed overhead and is not evidence of a fast-tempo match.
    """
    return pl.when(is_retirement()).then(None).otherwise(match_minutes())
