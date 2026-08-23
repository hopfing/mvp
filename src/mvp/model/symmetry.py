"""Orientation symmetry for pairwise match predictions.

Every match is scored twice — once per player orientation — and XGBoost is not
antisymmetric under that swap, so the two predictions do not sum to 1 (observed
pair sums range 0.83-1.26). Any function decomposes uniquely as ``f_odd +
f_even``; the Bayes margin here is exactly odd, because the target itself is
antisymmetric under the swap. The even component therefore carries no signal in
expectation, and it is pure noise on the shipped number.

Averaging in MARGIN (logit) space extracts the odd component exactly::

    L_sym(A) = (L_A - L_B) / 2

Margin space is the right space because the even component enters additively
there: a tree splitting on a symmetric feature contributes the identical leaf
value to both orientation rows, and the offset's intercept is likewise a shared
additive constant. Both cancel in the difference, at any magnitude. Probability
space averaging only cancels them to first order.

The result is antisymmetric by construction, so a match's two rows sum to 1
exactly.
"""

from __future__ import annotations

import numpy as np

# Clip before logit so a 0.0/1.0 prediction can't produce +/-inf. Matches the
# clipping compute_metrics uses on the probability scale.
_EPS = 1e-15


class PairingError(ValueError):
    """The frame's orientation pairs are malformed -- a DATA problem.

    Distinct from the plain ValueError raised on a length mismatch, which is a
    PROGRAMMING error: it means the probability vector and the uid vector came
    from different row orders, so the alignment contract broke and any pairing
    done anyway would be silently wrong.

    The split exists so serving can degrade past data problems (a malformed
    match should cost the averaging, not the whole betting cycle) without also
    swallowing an alignment bug, which must surface.
    """


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=np.float64), _EPS, 1.0 - _EPS)
    return np.log(p / (1.0 - p))


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-z))


def _reject_null_uids(match_uid: np.ndarray) -> None:
    """Refuse null match_uid up front, with a message that names the problem.

    Left alone, a null reaches ``np.argsort`` on an object array and raises
    ``TypeError: '<' not supported between instances of 'str' and 'NoneType'``
    from inside numpy, mid-study, pointing nowhere near the cause. There is no
    silent-mispairing path -- it always crashes -- so this is purely about
    crashing legibly.
    """
    if match_uid.dtype == object:
        n_null = int(sum(v is None for v in match_uid))
    elif match_uid.dtype.kind == "f":
        n_null = int(np.isnan(match_uid).sum())
    else:
        return
    if n_null:
        raise PairingError(
            f"match_uid has {n_null} null value(s) of {match_uid.size} rows; "
            "orientation pairs cannot be formed without it"
        )


def _pair_index(
    match_uid: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Row indices of each match's two orientations, plus the unpaired rows.

    Returns (i, j, unpaired) where i[k] and j[k] are the two rows of the k-th
    paired match. Raises if any match carries more than two rows -- at this
    grain (one row per player per singles match) a third row is upstream
    duplication that would double-weight the match, and absorbing it silently
    is worse than stopping.
    """
    _reject_null_uids(match_uid)
    # Stability is not load-bearing: every operation downstream is invariant
    # under swapping a pair's i/j roles. It costs nothing and keeps the index
    # reproducible, so it stays.
    order = np.argsort(match_uid, kind="stable")
    uid_sorted = match_uid[order]
    starts = np.flatnonzero(
        np.concatenate(([True], uid_sorted[1:] != uid_sorted[:-1]))
    )
    counts = np.diff(np.concatenate((starts, [len(uid_sorted)])))

    if (counts > 2).any():
        bad = uid_sorted[starts[counts > 2]][:5]
        raise PairingError(
            f"{int((counts > 2).sum())} match(es) have >2 prediction rows "
            f"(e.g. {[str(b) for b in bad]}); expected one row per player "
            "orientation"
        )

    pair_starts = starts[counts == 2]
    solo_starts = starts[counts == 1]
    return order[pair_starts], order[pair_starts + 1], order[solo_starts]


def validate_pairing(
    match_uid: np.ndarray,
    y_true: np.ndarray,
    player_id: np.ndarray | None = None,
    opp_id: np.ndarray | None = None,
    match_date: np.ndarray | None = None,
) -> tuple[int, int]:
    """Check the orientation pairs are what the `won` target requires.

    Returns (n_pairs, n_unpaired). Raises on violation.

    This is the check with content. That a symmetrized pair sums to 1 proves
    nothing -- the construction forces it for ANY pairing, including mispaired
    rows or colliding uids. What proves the pairing is real is that the two
    rows disagree on the label: for `won`, exactly one side of a match won it.

    The same assertion is what catches a `deciding_set` frame reaching this
    code path, since that target is EVEN under the orientation swap -- both
    rows carry the identical label -- and odd-projecting it would destroy the
    prediction rather than fail.
    """
    i, j, solo = _pair_index(np.asarray(match_uid))
    y = np.asarray(y_true)

    bad = np.flatnonzero(y[i] + y[j] != 1)
    if bad.size:
        both = int((y[i][bad] == 1).sum())
        raise PairingError(
            f"{bad.size} of {i.size} orientation pairs do not carry "
            f"complementary labels ({both} have both rows labelled 1). Either "
            "the rows are mispaired, match_uid collides across matches, or "
            "this is not a `won`-target frame -- an even target such as "
            "deciding_set must not be odd-projected."
        )

    if player_id is not None and opp_id is not None:
        pid, oid = np.asarray(player_id), np.asarray(opp_id)
        mism = int(((pid[i] != oid[j]) | (pid[j] != oid[i])).sum())
        if mism:
            raise PairingError(
                f"{mism} of {i.size} orientation pairs do not swap "
                "player_id/opp_id; the rows sharing a match_uid are not the "
                "two sides of one match"
            )

    if match_date is not None:
        d = np.asarray(match_date)
        n_split = int((d[i] != d[j]).sum())
        if n_split:
            raise PairingError(
                f"{n_split} of {i.size} orientation pairs span different "
                "dates; the rows sharing a match_uid are two different "
                "matches (a rematch of the same two players is the realistic "
                "way a uid collision survives the label and identity checks)"
            )

    return int(i.size), int(solo.size)


def pair_index(match_uid: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Public form of the pairing index, for callers that hoist it.

    Pair indices depend only on a frame's row order, which does not change
    across trials in a study. Computing them once per fold alongside
    `validate_pairing` and reusing them keeps every failure mode out of the
    per-trial hot path, which then only runs `symmetrize_indexed`.
    """
    return _pair_index(np.asarray(match_uid))


def symmetrize_indexed(
    y_prob: np.ndarray, i: np.ndarray, j: np.ndarray
) -> np.ndarray:
    """Odd-project using a precomputed pair index. O(n), no scan, no raises.

    CONTRACT: `i`/`j` must come from `pair_index` on the SAME frame, in the
    same row order, that produced `y_prob`. Nothing here can detect a
    misaligned index -- it will silently pair the wrong predictions. See the
    warning in `symmetrize`.
    """
    out = np.asarray(y_prob, dtype=np.float64).copy()
    if i.size == 0:
        return out
    half = 0.5 * (_logit(out[i]) - _logit(out[j]))
    out[i] = _sigmoid(half)
    out[j] = _sigmoid(-half)
    return out


def symmetrize(
    y_prob: np.ndarray, match_uid: np.ndarray
) -> tuple[np.ndarray, int, int]:
    """Odd-project per-orientation probabilities.

    Returns (probs, n_pairs, n_unpaired). Same shape and row order in and out,
    so callers can substitute the result for their existing per-row array.

    FOR `won`-TARGET FRAMES ONLY. This forces a match's two rows to sum to 1.
    An EVEN target -- `deciding_set`, where both rows carry the identical
    match-level label and the two probabilities should be EQUAL -- is destroyed
    by this transform, silently and without error. Nothing in this function can
    detect that; `validate_pairing` is the gate, and callers must run it.

    Pairing itself is NOT validated here. That a symmetrized pair sums to 1 is
    a property of this construction, true for any pairing including a wrong
    one, so it is not evidence of anything. Use `validate_pairing`.

    Rows whose match has only one orientation present are returned UNCHANGED --
    there is nothing to average against, and dropping them would make the
    evaluated population depend on the data.
    """
    y_prob = np.asarray(y_prob, dtype=np.float64)
    match_uid = np.asarray(match_uid)
    if y_prob.shape[0] != match_uid.shape[0]:
        # Deliberately NOT PairingError: this is the alignment contract
        # breaking, not malformed data, and must never be degraded past.
        raise ValueError(
            f"y_prob has {y_prob.shape[0]} rows but match_uid has "
            f"{match_uid.shape[0]}"
        )
    i, j, solo = _pair_index(match_uid)
    return symmetrize_indexed(y_prob, i, j), int(i.size), int(solo.size)
