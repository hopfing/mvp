"""Compiled per-match arithmetic of the multi-stream serve/return skill filter.

The tracker (`filter.py`) owns the state as per-stream arrays indexed by a
player table; every function here takes those arrays plus one match's inputs
and does exactly what the reference Python implementation did, in the same
order of floating-point operations, so the shipped stream's values are
bit-identical to the single-stream filter and the parity tests keep holding.

Layout (S streams, C player slots):
  overall axes        sm, sv, rm, rv                float64 (S, C)
  surface residuals   ssm, ssv, rsm, rsv            float64 (S, 3, C)
  indoor residuals    ism, isv, irm, irv            float64 (S, C)
  clocks              last_s, last_r, last_is, last_ir  int32 (S, C), -1 = never
                      last_ss, last_rs              int32 (S, 3, C)
  counters            n_s, n_r                      int32 (S, C)
  seeded              uint8 (S, C): the stream's cold-start seed committed

Work buffer W: float64 (2, NF, S), side 0 = the row player, side 1 = the
opponent; fields below. It is filled by `predict_side` (the state drifted to
the match date, seeds re-read for uncommitted streams), completed by
`matchup`, and read by the emitters and the update.

numba is optional at import: without it the same source runs as plain
Python (correct, slow), which is what `NUMBA_DISABLE_JIT=1` gives too.
"""

from __future__ import annotations

import math

try:  # pragma: no cover - exercised implicitly by the whole test suite
    from numba import njit
except ImportError:  # pragma: no cover
    def njit(*args, **kwargs):  # type: ignore[misc]
        if args and callable(args[0]) and len(args) == 1 and not kwargs:
            return args[0]

        def deco(f):
            return f
        return deco

# W fields
F_SM, F_SV, F_RM, F_RV = 0, 1, 2, 3
F_SSM, F_SSV, F_RSM, F_RSV = 4, 5, 6, 7
F_ISM, F_ISV, F_IRM, F_IRV = 8, 9, 10, 11
F_ETA, F_V = 12, 13
F_SEED_S, F_SEED_R = 14, 15
NF = 16

OVERALL_VAR_FLOOR = 1e-6
SURFACE_VAR_FLOOR = 1e-7
NAN = float("nan")


@njit(cache=True)
def _sigmoid(x):
    if x >= 0.0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


@njit(cache=True)
def init_player(i, elo8, W_unused,
                sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                last_s, last_r, last_ss, last_rs, last_is, last_ir,
                n_s, n_r, seeded, v0, q_surf, q_indoor,
                seed_src_s, seed_src_r, seed_scale_s, seed_scale_r):
    """A fresh state for player slot `i`, seeded from this row's Elo capture
    (`elo8`: the eight 1500-centred components, NaN when absent)."""
    S = sm.shape[0]
    for j in range(S):
        sm[j, i] = _seed(elo8, seed_src_s[j], seed_scale_s[j])
        rm[j, i] = _seed(elo8, seed_src_r[j], seed_scale_r[j])
        sv[j, i] = v0[j]
        rv[j, i] = v0[j]
        for s in range(3):
            ssm[j, s, i] = 0.0
            ssv[j, s, i] = q_surf[j]
            rsm[j, s, i] = 0.0
            rsv[j, s, i] = q_surf[j]
            last_ss[j, s, i] = -1
            last_rs[j, s, i] = -1
        ism[j, i] = 0.0
        isv[j, i] = q_indoor[j]
        irm[j, i] = 0.0
        irv[j, i] = q_indoor[j]
        last_s[j, i] = -1
        last_r[j, i] = -1
        last_is[j, i] = -1
        last_ir[j, i] = -1
        n_s[j, i] = 0
        n_r[j, i] = 0
        seeded[j, i] = 0


@njit(cache=True)
def _seed(elo8, src, scale):
    """`scale * (component - 1500) / 100`, or 0.0 with no usable source."""
    if src < 0:
        return 0.0
    v = elo8[src]
    if v != v:
        return 0.0
    return scale * (v - 1500.0) / 100.0


@njit(cache=True)
def predict_side(side, i, s, day, is_indoor, elo8, reseed, W,
                 sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                 last_s, last_r, last_ss, last_rs, last_is, last_ir, seeded,
                 q_s, q_r, q_surf, q_indoor, cap_days, phi,
                 has_surf, has_ind, seed_src_s, seed_src_r,
                 seed_scale_s, seed_scale_r, seedable):
    """The state of slot `i` drifted to `day` for surface `s`, into W[side].

    Mirrors the reference `_predict`: overall variances grow with the capped
    elapsed days since that axis's last observation; this surface's residual
    axes take one AR(1) step if ever observed; the indoor axes drift only on
    an indoor match; an uncommitted seedable stream re-reads its seed from
    this row's Elo (when `reseed`, i.e. the row is in domain)."""
    S = sm.shape[0]
    phi2 = phi * phi
    for j in range(S):
        W[side, F_SM, j] = sm[j, i]
        W[side, F_RM, j] = rm[j, i]
        v = sv[j, i]
        ls = last_s[j, i]
        if day >= 0 and ls >= 0:
            v = sv[j, i] + q_s[j] * min(day - ls, cap_days)
        W[side, F_SV, j] = v
        v = rv[j, i]
        lr = last_r[j, i]
        if day >= 0 and lr >= 0:
            v = rv[j, i] + q_r[j] * min(day - lr, cap_days)
        W[side, F_RV, j] = v
        if has_surf[j]:
            m = ssm[j, s, i]
            v = ssv[j, s, i]
            if last_ss[j, s, i] >= 0:
                m = phi * m
                v = phi2 * v + q_surf[j]
            W[side, F_SSM, j] = m
            W[side, F_SSV, j] = v
            m = rsm[j, s, i]
            v = rsv[j, s, i]
            if last_rs[j, s, i] >= 0:
                m = phi * m
                v = phi2 * v + q_surf[j]
            W[side, F_RSM, j] = m
            W[side, F_RSV, j] = v
        else:
            W[side, F_SSM, j] = NAN
            W[side, F_SSV, j] = NAN
            W[side, F_RSM, j] = NAN
            W[side, F_RSV, j] = NAN
        m = ism[j, i]
        v = isv[j, i]
        m2 = irm[j, i]
        v2 = irv[j, i]
        if is_indoor and has_ind[j]:
            if last_is[j, i] >= 0:
                m = phi * m
                v = phi2 * v + q_indoor[j]
            if last_ir[j, i] >= 0:
                m2 = phi * m2
                v2 = phi2 * v2 + q_indoor[j]
        W[side, F_ISM, j] = m
        W[side, F_ISV, j] = v
        W[side, F_IRM, j] = m2
        W[side, F_IRV, j] = v2
        W[side, F_SEED_S, j] = 0.0
        W[side, F_SEED_R, j] = 0.0
    if reseed:
        for k in range(seedable.shape[0]):
            j = seedable[k]
            if seeded[j, i] == 0:
                ss = _seed(elo8, seed_src_s[j], seed_scale_s[j])
                sr = _seed(elo8, seed_src_r[j], seed_scale_r[j])
                W[side, F_SM, j] = ss
                W[side, F_RM, j] = sr
                W[side, F_SEED_S, j] = ss
                W[side, F_SEED_R, j] = sr
    else:
        # Out of domain there is no re-read and no commit: a stream that has
        # never committed has no posterior mean to emit, only the seed of
        # whatever row created the slot, so its means read as null — what
        # the shipped filter emitted for a player it had no state for.
        for j in range(S):
            if seeded[j, i] == 0:
                W[side, F_SM, j] = NAN
                W[side, F_RM, j] = NAN


@njit(cache=True)
def matchup(W, srv, ret, cell, is_indoor, mu, has_ret, has_surf, has_ind):
    """Per-stream (eta, variance) for side `srv` serving against side `ret`,
    written into W[srv, F_ETA/F_V]. Stream 0's expression is the shipped one
    in the shipped order; the indoor terms are appended after it."""
    S = mu.shape[0]
    for j in range(S):
        e = mu[j, cell] + W[srv, F_SM, j]
        vv = W[srv, F_SV, j]
        if has_surf[j]:
            e = e + W[srv, F_SSM, j]
            vv = vv + W[srv, F_SSV, j]
        if has_ret[j]:
            e = e - W[ret, F_RM, j]
            vv = vv + W[ret, F_RV, j]
            if has_surf[j]:
                e = e - W[ret, F_RSM, j]
                vv = vv + W[ret, F_RSV, j]
        if is_indoor and has_ind[j]:
            e = e + W[srv, F_ISM, j]
            vv = vv + W[srv, F_ISV, j]
            if has_ret[j]:
                e = e - W[ret, F_IRM, j]
                vv = vv + W[ret, F_IRV, j]
        W[srv, F_ETA, j] = e
        W[srv, F_V, j] = vv


@njit(cache=True)
def emit_new(W, side, i, day, have_eta, last_s, n_s, tau2,
             has_ret, has_surf, has_ind, out, row_off):
    """Streams 1..S-1 as the flat float vector of `new_value_names`, written
    into `out[row_off:row_off+n_new]` (a slab column slice). NaN is null.
    The branch sequence IS the name sequence; keep them edited together."""
    S = n_s.shape[0]
    k = row_off
    for j in range(1, S):
        out[k] = W[side, F_SM, j]
        k += 1
        out[k] = math.sqrt(W[side, F_SV, j])
        k += 1
        if has_surf[j]:
            out[k] = W[side, F_SSM, j]
            k += 1
            out[k] = math.sqrt(W[side, F_SSV, j])
            k += 1
        if has_ind[j]:
            out[k] = W[side, F_ISM, j]
            k += 1
            out[k] = math.sqrt(W[side, F_ISV, j])
            k += 1
        if has_ret[j]:
            out[k] = W[side, F_RM, j]
            k += 1
            out[k] = math.sqrt(W[side, F_RV, j])
            k += 1
            if has_surf[j]:
                out[k] = W[side, F_RSM, j]
                k += 1
                out[k] = math.sqrt(W[side, F_RSV, j])
                k += 1
            if has_ind[j]:
                out[k] = W[side, F_IRM, j]
                k += 1
                out[k] = math.sqrt(W[side, F_IRV, j])
                k += 1
        out[k] = n_s[j, i]
        k += 1
        ls = last_s[j, i]
        if day >= 0 and ls >= 0:
            out[k] = day - ls
        else:
            out[k] = NAN
        k += 1
        if have_eta:
            out[k] = W[side, F_ETA, j]
            k += 1
            out[k] = math.sqrt(W[side, F_V, j] + tau2[j])
            k += 1
        else:
            out[k] = NAN
            k += 1
            out[k] = NAN
            k += 1
    return k - row_off


@njit(cache=True)
def emit_shipped(W, side, i, day, have_eta, last_s, n_s, tau0, out16):
    """The shipped stream's sixteen values, in BSR_VALUE_NAMES order, into
    `out16` (float64; NaN where the dict carried None)."""
    out16[0] = W[side, F_SM, 0]
    out16[1] = math.sqrt(W[side, F_SV, 0])
    out16[2] = W[side, F_RM, 0]
    out16[3] = math.sqrt(W[side, F_RV, 0])
    out16[4] = W[side, F_SSM, 0]
    out16[5] = math.sqrt(W[side, F_SSV, 0])
    out16[6] = W[side, F_RSM, 0]
    out16[7] = math.sqrt(W[side, F_RSV, 0])
    out16[8] = n_s[0, i]
    ls = last_s[0, i]
    if day >= 0 and ls >= 0:
        out16[9] = day - ls
    else:
        out16[9] = NAN
    if have_eta:
        out16[10] = W[side, F_ETA, 0]
        out16[11] = math.sqrt(W[side, F_V, 0] + tau0)
    else:
        out16[10] = NAN
        out16[11] = NAN
    out16[12] = W[side, F_ISM, 0]
    out16[13] = math.sqrt(W[side, F_ISV, 0])
    out16[14] = W[side, F_IRM, 0]
    out16[15] = math.sqrt(W[side, F_IRV, 0])


@njit(cache=True)
def observed(k, n, mask):
    """mask[j] = 1 when (k[j], n[j]) is a valid observation: n > 0 and
    0 <= k <= n. Returns the count."""
    c = 0
    for j in range(k.shape[0]):
        kj = k[j]
        nj = n[j]
        if nj > 0 and kj >= 0 and kj <= nj:
            mask[j] = 1
            c += 1
        else:
            mask[j] = 0
    return c


@njit(cache=True)
def apply_match(ia, ib, s, day, is_indoor, W,
                k_a, n_a, k_b, n_b, mask_a, mask_b,
                sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                last_s, last_r, last_ss, last_rs, last_is, last_ir,
                n_s, n_r, seeded, tau2, newton, has_ret, has_surf, has_ind):
    """Commit the seeds, the drift and the clocks for the axes the two
    observations touch, then the Newton/Laplace updates, exactly as the
    reference `apply`/`_commit`/`_update`. W holds both sides' predictions
    (the pre-match state) and their eta/v; `mask_*` the valid observations."""
    S = sm.shape[0]
    # Seeds of every stream this match observes, for BOTH players and both
    # axes, before any update moves them.
    for j in range(S):
        if mask_a[j] == 1 or mask_b[j] == 1:
            if seeded[j, ia] == 0:
                sm[j, ia] = W[0, F_SM, j]
                rm[j, ia] = W[0, F_RM, j]
                seeded[j, ia] = 1
            if seeded[j, ib] == 0:
                sm[j, ib] = W[1, F_SM, j]
                rm[j, ib] = W[1, F_RM, j]
                seeded[j, ib] = 1
    # a serves, b returns
    if _any(mask_a):
        _commit(ia, ib, 0, 1, s, day, is_indoor, W, mask_a,
                sv, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                last_s, last_r, last_ss, last_rs, last_is, last_ir, n_s, n_r,
                has_ret, has_surf, has_ind)
        for j in range(S):
            if mask_a[j] == 1:
                _update(ia, ib, j, s, is_indoor,
                        W[0, F_ETA, j], W[0, F_V, j], k_a[j], n_a[j],
                        sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                        tau2[j], newton, has_ret[j], has_surf[j], has_ind[j])
    # b serves, a returns
    if _any(mask_b):
        _commit(ib, ia, 1, 0, s, day, is_indoor, W, mask_b,
                sv, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                last_s, last_r, last_ss, last_rs, last_is, last_ir, n_s, n_r,
                has_ret, has_surf, has_ind)
        for j in range(S):
            if mask_b[j] == 1:
                _update(ib, ia, j, s, is_indoor,
                        W[1, F_ETA, j], W[1, F_V, j], k_b[j], n_b[j],
                        sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                        tau2[j], newton, has_ret[j], has_surf[j], has_ind[j])


@njit(cache=True)
def _any(mask):
    for j in range(mask.shape[0]):
        if mask[j] == 1:
            return True
    return False


@njit(cache=True)
def _commit(isrv, iret, wsrv, wret, s, day, is_indoor, W, mask,
            sv, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
            last_s, last_r, last_ss, last_rs, last_is, last_ir, n_s, n_r,
            has_ret, has_surf, has_ind):
    S = sv.shape[0]
    for j in range(S):
        if mask[j] != 1:
            continue
        sv[j, isrv] = W[wsrv, F_SV, j]
        last_s[j, isrv] = day
        n_s[j, isrv] += 1
        if has_surf[j]:
            ssm[j, s, isrv] = W[wsrv, F_SSM, j]
            ssv[j, s, isrv] = W[wsrv, F_SSV, j]
            last_ss[j, s, isrv] = day
        if is_indoor and has_ind[j]:
            ism[j, isrv] = W[wsrv, F_ISM, j]
            isv[j, isrv] = W[wsrv, F_ISV, j]
            last_is[j, isrv] = day
        if has_ret[j]:
            rv[j, iret] = W[wret, F_RV, j]
            last_r[j, iret] = day
            n_r[j, iret] += 1
            if has_surf[j]:
                rsm[j, s, iret] = W[wret, F_RSM, j]
                rsv[j, s, iret] = W[wret, F_RSV, j]
                last_rs[j, s, iret] = day
            if is_indoor and has_ind[j]:
                irm[j, iret] = W[wret, F_IRM, j]
                irv[j, iret] = W[wret, F_IRV, j]
                last_ir[j, iret] = day


@njit(cache=True)
def _update(isrv, iret, j, s, is_indoor, eta, v, y, n,
            sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
            tau2, newton, has_ret, has_surf, has_ind):
    """Newton/Laplace update on eta ~ N(eta, v + tau2) against Binomial(n,
    sigmoid(eta)); the shift and the precision gain are split across the
    components by their share of the prior variance. The shipped order:
    server overall, server surface, returner overall, returner surface, then
    the indoor axes (stream 0 has none, so it is bit-identical)."""
    vt = v + tau2
    m = eta
    for _ in range(newton):
        p = _sigmoid(m)
        g = (y - n * p) - (m - eta) / vt
        h = n * p * (1.0 - p) + 1.0 / vt
        m += g / h
    p = _sigmoid(m)
    v_post = 1.0 / (n * p * (1.0 - p) + 1.0 / vt)
    delta = m - eta
    shrink = vt - v_post
    w = sv[j, isrv] / vt
    sm[j, isrv] += w * delta
    sv[j, isrv] -= w * w * shrink
    if has_surf:
        w = ssv[j, s, isrv] / vt
        ssm[j, s, isrv] += w * delta
        ssv[j, s, isrv] -= w * w * shrink
    if has_ret:
        w = rv[j, iret] / vt
        rm[j, iret] -= w * delta
        rv[j, iret] -= w * w * shrink
        if has_surf:
            w = rsv[j, s, iret] / vt
            rsm[j, s, iret] -= w * delta
            rsv[j, s, iret] -= w * w * shrink
    if is_indoor and has_ind:
        w = isv[j, isrv] / vt
        ism[j, isrv] += w * delta
        isv[j, isrv] -= w * w * shrink
        if isv[j, isrv] < SURFACE_VAR_FLOOR:
            isv[j, isrv] = SURFACE_VAR_FLOOR
        if has_ret:
            w = irv[j, iret] / vt
            irm[j, iret] -= w * delta
            irv[j, iret] -= w * w * shrink
            if irv[j, iret] < SURFACE_VAR_FLOOR:
                irv[j, iret] = SURFACE_VAR_FLOOR
    if sv[j, isrv] < OVERALL_VAR_FLOOR:
        sv[j, isrv] = OVERALL_VAR_FLOOR
    if has_surf and ssv[j, s, isrv] < SURFACE_VAR_FLOOR:
        ssv[j, s, isrv] = SURFACE_VAR_FLOOR
    if has_ret:
        if rv[j, iret] < OVERALL_VAR_FLOOR:
            rv[j, iret] = OVERALL_VAR_FLOOR
        if has_surf and rsv[j, s, iret] < SURFACE_VAR_FLOOR:
            rsv[j, s, iret] = SURFACE_VAR_FLOOR


@njit(cache=True)
def capture_all(ia, ib, s, cell, day, is_indoor, in_domain,
                elo8, k, n, mask, W, out16, vec,
                sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                last_s, last_r, last_ss, last_rs, last_is, last_ir, n_s, seeded,
                q_s, q_r, q_surf, q_indoor, cap_days, phi, tau2, mu,
                has_ret, has_surf, has_ind,
                seed_src_s, seed_src_r, seed_scale_s, seed_scale_r, seedable):
    """One match's whole capture in a single compiled call: observation
    masks, both sides' predictions, both matchups, and both sides' shipped
    and new-stream emissions. One call instead of eight, because the
    per-call argument unboxing dominated the per-match cost. Returns the
    number of valid observations."""
    n_obs = 0
    if in_domain:
        n_obs = observed(k[0], n[0], mask[0]) + observed(k[1], n[1], mask[1])
    else:
        mask[0, :] = 0
        mask[1, :] = 0
    have_a = ia >= 0
    have_b = ib >= 0
    if have_a:
        predict_side(0, ia, s, day, is_indoor, elo8[0], in_domain, W,
                     sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                     last_s, last_r, last_ss, last_rs, last_is, last_ir, seeded,
                     q_s, q_r, q_surf, q_indoor, cap_days, phi,
                     has_surf, has_ind, seed_src_s, seed_src_r,
                     seed_scale_s, seed_scale_r, seedable)
    if have_b:
        predict_side(1, ib, s, day, is_indoor, elo8[1], in_domain, W,
                     sm, sv, rm, rv, ssm, ssv, rsm, rsv, ism, isv, irm, irv,
                     last_s, last_r, last_ss, last_rs, last_is, last_ir, seeded,
                     q_s, q_r, q_surf, q_indoor, cap_days, phi,
                     has_surf, has_ind, seed_src_s, seed_src_r,
                     seed_scale_s, seed_scale_r, seedable)
    have_eta = in_domain and have_a and have_b
    if have_eta:
        matchup(W, 0, 1, cell, is_indoor, mu, has_ret, has_surf, has_ind)
        matchup(W, 1, 0, cell, is_indoor, mu, has_ret, has_surf, has_ind)
    if have_a:
        emit_shipped(W, 0, ia, day, have_eta, last_s, n_s, tau2[0], out16[0])
        emit_new(W, 0, ia, day, have_eta, last_s, n_s, tau2,
                 has_ret, has_surf, has_ind, vec[0], 0)
    else:
        out16[0, :] = NAN
        vec[0, :] = NAN
    if have_b:
        emit_shipped(W, 1, ib, day, have_eta, last_s, n_s, tau2[0], out16[1])
        emit_new(W, 1, ib, day, have_eta, last_s, n_s, tau2,
                 has_ret, has_surf, has_ind, vec[1], 0)
    else:
        out16[1, :] = NAN
        vec[1, :] = NAN
    return n_obs
