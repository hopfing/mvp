"""Tennis scoring math, written once.

The point-to-game closed form had drifted into three copies — a numpy version in
the projection chain, a scalar version in the model's feature module, and a
polars expression a hundred lines below that scalar one. Three copies of an
algebraic identity is three chances for them to stop agreeing, and the failure
would be quiet: the model's `iid_hold_prob` feature and the projection engine's
hold probability would simply mean slightly different things, with nothing
raising.

The formula lives here in a single generic form. `float`, `numpy.ndarray`,
`polars.Expr` and `polars.Series` all implement the same arithmetic operators,
so one expression serves every caller without a backend switch or a dispatch
table — the callers keep their own signatures and delegate the algebra.
"""

from __future__ import annotations

from typing import TypeVar

__all__ = ["hold_probability"]

# Anything supporting **, *, +, - and / against Python scalars. Deliberately not
# constrained further: the point of this module is that the algebra is identical
# whether it lands on a scalar, an array or a deferred polars expression.
Numeric = TypeVar("Numeric")


def hold_probability(p: Numeric) -> Numeric:
    """P(hold serve) given P(win a point on serve) = `p`.

    The closed-form sum over the game scoring tree:

        win to 0   :      p^4
        win to 15  :  4 * p^4 * q
        win to 30  : 10 * p^4 * q^2
        reach deuce: 20 * p^3 * q^3, then win it: p^2 / (p^2 + q^2)

    The first three collect into `p^4 (1 + 4q + 10q^2)`; the deuce branch is an
    infinite series over deuce/advantage cycles that sums in closed form because
    each cycle returns to the same state.

    Returns whatever type it was given — array in, array out; expression in,
    expression out.

    Two properties worth knowing rather than rediscovering:

    - The denominator `p^2 + q^2` has minimum 0.5 at p=0.5 and never reaches
      zero on [0, 1], so this needs no domain guard. It is also exact at the
      endpoints (0 -> 0, 1 -> 1) rather than merely close, so a caller clamping
      to [0, 1] is not correcting anything.

    - It is NOT convex over the operating range. The curve inflects at exactly
      p=0.5 and is concave above it, and serve-win probabilities in tennis sit
      near [0.55, 0.74] — entirely on the concave side. Anything averaging this
      over a distribution of `p` therefore pulls the result DOWN relative to
      evaluating at the mean, and a test asserting the opposite is asserting the
      wrong sign.
    """
    q = 1 - p
    pre_deuce = p ** 4 * (1 + 4 * q + 10 * q ** 2)
    deuce = 20 * p ** 5 * q ** 3 / (p ** 2 + q ** 2)
    return pre_deuce + deuce
