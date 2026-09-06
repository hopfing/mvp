"""The serve/return skill filter as a ratings-pass tracker.

Model (plan 2026-09-06-bayesian-serve-return-skill): per player, a serve
skill and a return skill on the logit scale that random-walk between
observations, plus one shrunk residual axis per surface for each; per match,
each side's serve points won out of points served is a binomial observation
of

    eta = mu(surface, circuit, indoor) + s_server + s_server^surface
          - r_returner - r_returner^surface

with a per-observation Gaussian random effect tau^2 on eta (match-day form).
The posterior is Gaussian per axis (assumed-density filtering): a Laplace /
Newton update on eta, whose information is then distributed to the four
components by their share of the prior variance.

Seams mirror `MovTracker` (elo/mov.py): the ratings driver captures BEFORE
it updates, both rows of a match read one cached capture, and the update runs
once per match. Two things are deliberate and load-bearing:

- **Predictive state on every row.** `capture_match` computes each player's
  state drifted to the match date and emits from that, whether or not the
  match carries serve counts. A pending live match has no counts, and if the
  drift were applied only when an observation arrives, the live columns would
  understate the skill sd by 20-30% at the cap and carry an undecayed surface
  mean relative to what the same row shows once it is settled and trained on.
  The drift is committed to stored state only by `apply`, and only for the
  axes an observation actually touches, so parity with a filter that only ever
  sees observed rows is exact.
- **Both observations captured before either is applied.** Today the two
  observations of a match touch disjoint slots (server's serve axes and
  returner's return axes), so batched equals sequential exactly. Once any
  covariance between a player's serve and return exists, only the batched
  form stays correct.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from mvp.atptour.bsr.constants import (
    CIRCUIT_INDEX,
    DEFAULT_BSR_CONFIG,
    SURFACE_INDEX,
    BsrConfig,
)

BSR_VALUE_NAMES: tuple[str, ...] = (
    "bsr_serve_mu", "bsr_serve_sd", "bsr_return_mu", "bsr_return_sd",
    "bsr_serve_surface_mu", "bsr_serve_surface_sd",
    "bsr_return_surface_mu", "bsr_return_surface_sd",
    "bsr_n_serve_obs", "bsr_days_since_serve_obs",
    "bsr_pserve_logit", "bsr_pserve_logit_sd",
)

_OVERALL_VAR_FLOOR = 1e-6
_SURFACE_VAR_FLOOR = 1e-7
_NULLS: dict[str, Any] = {name: None for name in BSR_VALUE_NAMES}


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and x == x


class _PlayerState:
    """One player's posterior: overall serve/return axes and per-surface
    residual axes, each a mean and a variance; the clocks and counters the
    drift and the emitted columns read."""

    __slots__ = (
        "sm", "sv", "rm", "rv", "ssm", "ssv", "rsm", "rsv",
        "last_s", "last_r", "last_ss", "last_rs", "n_s", "n_r",
    )

    def __init__(self, sm: float, rm: float, v0: float, q_surf: float) -> None:
        self.sm = sm
        self.sv = v0
        self.rm = rm
        self.rv = v0
        self.ssm = [0.0, 0.0, 0.0]
        self.ssv = [q_surf, q_surf, q_surf]
        self.rsm = [0.0, 0.0, 0.0]
        self.rsv = [q_surf, q_surf, q_surf]
        self.last_s: date | None = None
        self.last_r: date | None = None
        self.last_ss: list[date | None] = [None, None, None]
        self.last_rs: list[date | None] = [None, None, None]
        self.n_s = 0
        self.n_r = 0


@dataclass
class _Pred:
    """A player's state drifted to a match date, for this match's surface."""

    sm: float
    sv: float
    rm: float
    rv: float
    ssm: float
    ssv: float
    rsm: float
    rsv: float
    last_s: date | None
    n_s: int


@dataclass
class BsrCapture:
    """What the driver stashes per match: the two emitted value dicts and the
    pending update `apply` needs."""

    player: dict[str, Any]
    opp: dict[str, Any]
    pending: tuple | None


class BsrTracker:
    """Per-player filter state threaded through compute_all_ratings.

    ONE tracker per compute_all_ratings call, like MovTracker: reusing it
    across calls would double-process every match without error.
    """

    def __init__(self, config: BsrConfig | None = None) -> None:
        self.cfg = config or DEFAULT_BSR_CONFIG
        self._state: dict[str, _PlayerState] = {}

    # ---- seams -------------------------------------------------------------

    def output_columns(self) -> list[str]:
        return [
            f"{side}_{name}"
            for name in BSR_VALUE_NAMES
            for side in ("player", "opp")
        ]

    @staticmethod
    def append_output(
        output: dict[str, list],
        player_vals: dict[str, Any],
        opp_vals: dict[str, Any],
    ) -> None:
        for name in BSR_VALUE_NAMES:
            output[f"player_{name}"].append(player_vals[name])
            output[f"opp_{name}"].append(opp_vals[name])

    def capture_match(
        self,
        player_id: str,
        opp_id: str,
        surface: str | None,
        circuit: str | None,
        indoor: Any,
        match_date: Any,
        y_p: Any,
        n_p: Any,
        y_o: Any,
        n_o: Any,
        seed_serve_p: Any,
        seed_return_p: Any,
        seed_serve_o: Any,
        seed_return_o: Any,
    ) -> BsrCapture:
        """PRE-match values for both players, computed from state drifted to
        `match_date`; nothing is stored until `apply`.

        `y_p`/`n_p` are the player's serve points won/played (observation 1,
        player serves); `y_o`/`n_o` the opponent's (observation 2). Seeds are
        the pre-match base serve/return Elo of each player, used only when a
        player's state is created at this match.
        """
        cfg = self.cfg
        # The aggregate carries effective_match_date as a datetime; the
        # filter's clocks are day-grained, and date < datetime comparisons
        # raise, so normalize once here.
        if isinstance(match_date, datetime):
            d: date | None = match_date.date()
        elif isinstance(match_date, date):
            d = match_date
        else:
            d = None
        s = SURFACE_INDEX.get(surface, 0)
        circ = CIRCUIT_INDEX.get(circuit)
        in_domain = circ is not None and d is not None and d >= cfg.start_date
        obs1 = (float(y_p), float(n_p)) if in_domain and _valid_obs(y_p, n_p) else None
        obs2 = (float(y_o), float(n_o)) if in_domain and _valid_obs(y_o, n_o) else None

        st_a = self._state.get(player_id)
        st_b = self._state.get(opp_id)
        new: dict[str, _PlayerState] = {}
        if in_domain:
            # A player without state is seeded from this row's pre-match Elo
            # on every in-domain row, so a debutant's pending row emits the
            # same seed values its settled row will. The seed is STORED only
            # by `apply`, and only when this match carries an observation —
            # which is when the probe creates state — so committed
            # trajectories are unchanged and parity holds.
            if st_a is None:
                st_a = new[player_id] = self._new_state(seed_serve_p, seed_return_p)
            if st_b is None:
                st_b = new[opp_id] = self._new_state(seed_serve_o, seed_return_o)

        pa = self._predict(st_a, s, d)
        pb = self._predict(st_b, s, d)
        # Player-level state is emitted wherever it exists (an ITF row of a
        # challenger regular still carries his posterior); the matchup pair
        # needs a fixed effect for the row's cell, which only in-domain rows
        # have, so it is null outside the domain rather than computed with a
        # borrowed circuit's mu.
        mu = cfg.mu_cells[s * 4 + (circ or 0) * 2 + (1 if indoor else 0)]
        eta_a = v_a = eta_b = v_b = None
        if in_domain and pa is not None and pb is not None:
            eta_a = mu + pa.sm + pa.ssm - pb.rm - pb.rsm
            v_a = pa.sv + pa.ssv + pb.rv + pb.rsv
            eta_b = mu + pb.sm + pb.ssm - pa.rm - pa.rsm
            v_b = pb.sv + pb.ssv + pa.rv + pa.rsv

        vals_a = self._values(pa, eta_a, v_a, d)
        vals_b = self._values(pb, eta_b, v_b, d)
        pending = None
        if obs1 is not None or obs2 is not None:
            pending = (
                player_id, opp_id, s, d, obs1, obs2,
                pa, pb, eta_a, v_a, eta_b, v_b, new,
            )
        return BsrCapture(player=vals_a, opp=vals_b, pending=pending)

    def apply(self, cap: BsrCapture) -> None:
        """Commit the drift for the axes an observation touches and run the
        update. Both observations are taken from the capture's pre-match
        state; today they touch disjoint slots, so this equals applying them
        one after the other."""
        if cap.pending is None:
            return
        (a, b, s, d, obs1, obs2, pa, pb, eta_a, v_a, eta_b, v_b, new) = cap.pending
        self._state.update(new)
        st_a = self._state[a]
        st_b = self._state[b]
        if obs1 is not None:
            # a serves, b returns
            st_a.sv = pa.sv
            st_a.ssm[s] = pa.ssm
            st_a.ssv[s] = pa.ssv
            st_b.rv = pb.rv
            st_b.rsm[s] = pb.rsm
            st_b.rsv[s] = pb.rsv
            self._update(st_a, st_b, s, eta_a, v_a, *obs1)
            st_a.last_s = d
            st_b.last_r = d
            st_a.last_ss[s] = d
            st_b.last_rs[s] = d
            st_a.n_s += 1
            st_b.n_r += 1
        if obs2 is not None:
            # b serves, a returns
            st_b.sv = pb.sv
            st_b.ssm[s] = pb.ssm
            st_b.ssv[s] = pb.ssv
            st_a.rv = pa.rv
            st_a.rsm[s] = pa.rsm
            st_a.rsv[s] = pa.rsv
            self._update(st_b, st_a, s, eta_b, v_b, *obs2)
            st_b.last_s = d
            st_a.last_r = d
            st_b.last_ss[s] = d
            st_a.last_rs[s] = d
            st_b.n_s += 1
            st_a.n_r += 1

    # ---- internals ---------------------------------------------------------

    def _new_state(self, serve_elo: Any, return_elo: Any) -> _PlayerState:
        cfg = self.cfg
        sm = cfg.seed_es * (float(serve_elo) - 1500.0) / 100.0 if _finite(serve_elo) else 0.0
        rm = cfg.seed_er * (float(return_elo) - 1500.0) / 100.0 if _finite(return_elo) else 0.0
        return _PlayerState(sm, rm, cfg.v0, cfg.q_surf)

    def _predict(self, st: _PlayerState | None, s: int, d: date | None) -> _Pred | None:
        """The state drifted to `d`: overall variances grow with elapsed days
        since that axis's last observation (capped); this surface's residual
        axes take one AR(1) step if they have ever been observed."""
        if st is None:
            return None
        cfg = self.cfg
        sv = st.sv
        rv = st.rv
        if d is not None:
            if st.last_s is not None:
                sv = sv + cfg.q_s * min((d - st.last_s).days, cfg.cap_days)
            if st.last_r is not None:
                rv = rv + cfg.q_r * min((d - st.last_r).days, cfg.cap_days)
        ssm, ssv = st.ssm[s], st.ssv[s]
        if st.last_ss[s] is not None:
            ssm = cfg.phi_surf * ssm
            ssv = cfg.phi_surf * cfg.phi_surf * ssv + cfg.q_surf
        rsm, rsv = st.rsm[s], st.rsv[s]
        if st.last_rs[s] is not None:
            rsm = cfg.phi_surf * rsm
            rsv = cfg.phi_surf * cfg.phi_surf * rsv + cfg.q_surf
        return _Pred(st.sm, sv, st.rm, rv, ssm, ssv, rsm, rsv, st.last_s, st.n_s)

    def _values(
        self,
        p: _Pred | None,
        eta: float | None,
        v: float | None,
        d: date | None,
    ) -> dict[str, Any]:
        if p is None:
            return dict(_NULLS)
        days = (d - p.last_s).days if (d is not None and p.last_s is not None) else None
        return {
            "bsr_serve_mu": p.sm,
            "bsr_serve_sd": math.sqrt(p.sv),
            "bsr_return_mu": p.rm,
            "bsr_return_sd": math.sqrt(p.rv),
            "bsr_serve_surface_mu": p.ssm,
            "bsr_serve_surface_sd": math.sqrt(p.ssv),
            "bsr_return_surface_mu": p.rsm,
            "bsr_return_surface_sd": math.sqrt(p.rsv),
            "bsr_n_serve_obs": p.n_s,
            "bsr_days_since_serve_obs": days,
            "bsr_pserve_logit": eta,
            "bsr_pserve_logit_sd": (
                math.sqrt(v + self.cfg.tau2) if v is not None else None
            ),
        }

    def _update(
        self,
        srv: _PlayerState,
        ret: _PlayerState,
        s: int,
        eta: float,
        v: float,
        y: float,
        n: float,
    ) -> None:
        """Newton/Laplace update on eta ~ N(eta, v + tau2) against
        Binomial(n, sigmoid(eta)); the shift and the precision gain are split
        across the four components by their share of the prior variance."""
        cfg = self.cfg
        vt = v + cfg.tau2
        m = eta
        for _ in range(cfg.newton):
            p = _sigmoid(m)
            g = (y - n * p) - (m - eta) / vt
            h = n * p * (1.0 - p) + 1.0 / vt
            m += g / h
        p = _sigmoid(m)
        v_post = 1.0 / (n * p * (1.0 - p) + 1.0 / vt)
        delta = m - eta
        shrink = vt - v_post
        w = srv.sv / vt
        srv.sm += w * delta
        srv.sv -= w * w * shrink
        w = srv.ssv[s] / vt
        srv.ssm[s] += w * delta
        srv.ssv[s] -= w * w * shrink
        w = ret.rv / vt
        ret.rm -= w * delta
        ret.rv -= w * w * shrink
        w = ret.rsv[s] / vt
        ret.rsm[s] -= w * delta
        ret.rsv[s] -= w * w * shrink
        if srv.sv < _OVERALL_VAR_FLOOR:
            srv.sv = _OVERALL_VAR_FLOOR
        if ret.rv < _OVERALL_VAR_FLOOR:
            ret.rv = _OVERALL_VAR_FLOOR
        if srv.ssv[s] < _SURFACE_VAR_FLOOR:
            srv.ssv[s] = _SURFACE_VAR_FLOOR
        if ret.rsv[s] < _SURFACE_VAR_FLOOR:
            ret.rsv[s] = _SURFACE_VAR_FLOOR


def _valid_obs(y: Any, n: Any) -> bool:
    if n is None or y is None:
        return False
    try:
        nf = float(n)
        yf = float(y)
    except (TypeError, ValueError):
        return False
    return nf == nf and yf == yf and nf > 0 and 0.0 <= yf <= nf
