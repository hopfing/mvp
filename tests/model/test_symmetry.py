import numpy as np
import pytest

from mvp.model.symmetry import symmetrize


def test_pairs_sum_to_one_exactly():
    p = np.array([0.62, 0.41, 0.90, 0.05])
    uid = np.array(["m1", "m1", "m2", "m2"])
    out, n, _ = symmetrize(p, uid)
    assert n == 2
    assert out[0] + out[1] == pytest.approx(1.0, abs=1e-12)
    assert out[2] + out[3] == pytest.approx(1.0, abs=1e-12)


def test_already_complementary_is_a_fixed_point():
    """A model that is already antisymmetric must not be perturbed."""
    p = np.array([0.73, 0.27, 0.5, 0.5])
    uid = np.array(["m1", "m1", "m2", "m2"])
    out, _, _ = symmetrize(p, uid)
    assert out == pytest.approx(p, abs=1e-12)


def test_cancels_a_shared_margin_offset_exactly():
    """The even component is additive in margin space, so any shared shift --
    a symmetric-feature leaf value, Platt's intercept, the offset constant --
    must cancel regardless of magnitude."""
    logit = lambda x: np.log(x / (1 - x))
    sig = lambda z: 1 / (1 + np.exp(-z))
    true_margin = 0.8
    for delta in (0.0, 0.35, -1.2, 4.0):
        p = np.array([sig(true_margin + delta), sig(-true_margin + delta)])
        out, _, _ = symmetrize(p, np.array(["m", "m"]))
        assert logit(out[0]) == pytest.approx(true_margin, abs=1e-9)


def test_row_order_and_shape_preserved():
    p = np.array([0.62, 0.9, 0.41, 0.05])
    uid = np.array(["m1", "m2", "m1", "m2"])  # interleaved, not adjacent
    out, n, _ = symmetrize(p, uid)
    assert n == 2 and out.shape == p.shape
    assert out[0] + out[2] == pytest.approx(1.0, abs=1e-12)
    assert out[1] + out[3] == pytest.approx(1.0, abs=1e-12)


def test_unpaired_rows_pass_through_unchanged():
    p = np.array([0.62, 0.41, 0.77])
    uid = np.array(["m1", "m1", "solo"])
    out, n, _ = symmetrize(p, uid)
    assert n == 1
    assert out[2] == 0.77


def test_extreme_probabilities_do_not_produce_nan():
    p = np.array([1.0, 0.0, 0.0, 0.0])
    uid = np.array(["m1", "m1", "m2", "m2"])
    out, _, _ = symmetrize(p, uid)
    assert np.isfinite(out).all()
    assert out[2] + out[3] == pytest.approx(1.0, abs=1e-12)


def test_more_than_two_rows_per_match_raises():
    p = np.array([0.6, 0.4, 0.5])
    uid = np.array(["m1", "m1", "m1"])
    with pytest.raises(ValueError, match=">2 prediction rows"):
        symmetrize(p, uid)


def test_length_mismatch_raises():
    with pytest.raises(ValueError, match="rows"):
        symmetrize(np.array([0.5, 0.5]), np.array(["m1"]))


# --- validate_pairing -------------------------------------------------------

from mvp.model.symmetry import validate_pairing


def test_validate_accepts_a_well_formed_won_frame():
    uid = np.array(["m1", "m1", "m2", "m2", "solo"])
    y = np.array([1, 0, 0, 1, 1])
    pid = np.array(["a", "b", "c", "d", "e"])
    oid = np.array(["b", "a", "d", "c", "f"])
    n_pairs, n_solo = validate_pairing(uid, y, pid, oid)
    assert (n_pairs, n_solo) == (2, 1)


def test_validate_rejects_a_deciding_set_frame():
    """The even target carries the SAME label on both rows. Odd-projecting it
    would destroy the prediction silently, so the pairing check must stop it."""
    uid = np.array(["m1", "m1", "m2", "m2"])
    y = np.array([1, 1, 0, 0])  # match-level label, identical per side
    with pytest.raises(ValueError, match="deciding_set"):
        validate_pairing(uid, y)


def test_validate_rejects_colliding_uids():
    """Two different matches sharing a uid pair rows that aren't opponents."""
    uid = np.array(["dup", "dup"])
    y = np.array([1, 1])
    with pytest.raises(ValueError, match="complementary labels"):
        validate_pairing(uid, y)


def test_validate_rejects_rows_that_are_not_opponents():
    uid = np.array(["m1", "m1"])
    y = np.array([1, 0])          # labels look fine
    pid = np.array(["a", "c"])    # but c is not a's opponent
    oid = np.array(["b", "a"])
    with pytest.raises(ValueError, match="swap"):
        validate_pairing(uid, y, pid, oid)


def test_validate_identity_check_is_optional():
    uid = np.array(["m1", "m1"])
    y = np.array([1, 0])
    assert validate_pairing(uid, y) == (1, 0)


def test_validate_reports_unpaired_count():
    uid = np.array(["m1", "m1", "s1", "s2"])
    y = np.array([1, 0, 1, 0])
    assert validate_pairing(uid, y) == (1, 2)


def test_validate_propagates_the_over_pairing_guard():
    with pytest.raises(ValueError, match=">2 prediction rows"):
        validate_pairing(np.array(["m", "m", "m"]), np.array([1, 0, 1]))


# --- properties and edge cases ---------------------------------------------

from mvp.model.symmetry import pair_index, symmetrize_indexed


def test_permutation_invariance():
    """The property that catches indexing bugs fixed-order tests cannot:
    symmetrizing a shuffled frame must equal shuffling the symmetrized one."""
    rng = np.random.default_rng(0)
    n_matches = 40
    uid = np.repeat([f"m{k}" for k in range(n_matches)], 2)
    p = rng.uniform(0.02, 0.98, size=uid.size)
    base, _, _ = symmetrize(p, uid)
    for _ in range(20):
        perm = rng.permutation(uid.size)
        out, _, _ = symmetrize(p[perm], uid[perm])
        assert out == pytest.approx(base[perm], abs=1e-12)


def test_role_swap_within_pair_is_invariant():
    p = np.array([0.62, 0.41])
    a, _, _ = symmetrize(p, np.array(["m", "m"]))
    b, _, _ = symmetrize(p[::-1], np.array(["m", "m"]))
    assert a == pytest.approx(b[::-1], abs=1e-12)


def test_empty_frame():
    out, n, solo = symmetrize(np.array([]), np.array([], dtype=object))
    assert out.size == 0 and n == 0 and solo == 0


def test_null_match_uid_is_rejected_legibly():
    """Left to numpy this is a TypeError from inside argsort, mid-study,
    naming neither the column nor the cause."""
    uid = np.array(["m1", None], dtype=object)
    with pytest.raises(ValueError, match="null value"):
        symmetrize(np.array([0.6, 0.4]), uid)


def test_duplicated_same_orientation_is_caught_by_validate_not_symmetrize():
    """symmetrize alone forces a duplicated row to exactly 0.5 with no error --
    the logit difference is zero. Only validate_pairing catches it, which is
    why validation must see every frame symmetrize touches."""
    uid = np.array(["m1", "m1"])
    p = np.array([0.73, 0.73])
    out, _, _ = symmetrize(p, uid)
    assert out == pytest.approx([0.5, 0.5], abs=1e-12)
    with pytest.raises(ValueError, match="complementary labels"):
        validate_pairing(uid, np.array([1, 1]))


def test_indexed_path_matches_the_scanning_path():
    uid = np.array(["m1", "m2", "m1", "m2", "solo"])
    p = np.array([0.62, 0.9, 0.41, 0.05, 0.5])
    i, j, solo = pair_index(uid)
    assert solo.size == 1
    expect, _, _ = symmetrize(p, uid)
    assert symmetrize_indexed(p, i, j) == pytest.approx(expect, abs=1e-15)


def test_validate_rejects_a_pair_spanning_two_dates():
    uid = np.array(["m1", "m1"])
    y = np.array([1, 0])
    pid, oid = np.array(["a", "b"]), np.array(["b", "a"])
    dates = np.array(["2026-01-01", "2026-06-01"])
    with pytest.raises(ValueError, match="different"):
        validate_pairing(uid, y, pid, oid, dates)
