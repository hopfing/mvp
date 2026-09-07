"""The serve/return skill filter as a ratings-pass tracker.

Model (plan 2026-09-06-bayesian-serve-return-skill, extended by the
21-stream build): per player and per observation STREAM, a server-side skill
and — where the other player acts — a returner-side skill on the logit scale
that random-walk between observations, plus one shrunk residual axis per
surface and one for indoor venues on the streams that carry them; per match,
each side's count pair (k, n) for a stream is a binomial observation of

    eta = mu(surface, circuit, indoor) + s_server + s_server^surface
          + s_server^indoor - r_returner - r_returner^surface
          - r_returner^indoor

with a per-observation Gaussian random effect tau^2 on eta (match-day form).
The posterior is Gaussian per axis (assumed-density filtering): a Laplace /
Newton update on eta, whose information is then distributed to the components
by their share of the prior variance. Streams are conditionally independent
given the counts, so the update is the single-stream update applied per
stream; a stream whose n is 0 or missing is skipped for that observation only,
and every other stream still updates.

The stream table, its fixed order and what each stream observes live in
`constants.py`. Stream 0 is the shipped pooled `serve` stream: its seven
shipped knobs, its mu cells and the twelve `bsr_*` column names it emits are
unchanged, and it has GAINED an indoor residual axis it emits no column for,
so its twelve values move on indoor rows and only there. The probe-parity and
shipped-parity tests hold it to the shipped filter with that axis switched
off, which is the config under which the two are the same model.

Seams mirror `MovTracker` (elo/mov.py): the ratings driver captures BEFORE
it updates, both rows of a match read one cached capture, and the update runs
once per match. Four things are deliberate and load-bearing:

- **Predictive state on every row.** `capture_match` computes each player's
  state drifted to the match date and emits from that, whether or not the
  match carries counts. A pending live match has no counts, and if the
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
- **Two emission paths.** The shipped twelve value names keep the per-row
  dict of Python floats, because the parquet carries them as Float64/Int64
  and the parity test holds them to 1e-9. The 178 names the other 20 streams
  add would cost ~12 GB as per-column Python lists on the live frame, so they
  are written as ONE float32 column slice per row into a slab the tracker
  preallocates, replayed on the second row by copying that column with the
  two sides swapped, and handed to polars at the end as zero-copy series
  built with `nan_to_null=True` — NaN is the null marker, and without that
  flag `null_count()` would read an all-NaN column as fully populated and
  both `check_columns.py` and the parity guard would go blind.
- **Cold-start seeds freeze per stream, not per player.** A player's state
  now comes into being at the first observation of ANY stream, and on the
  live aggregate 744 in-domain rows carry a tiebreak the pooled stream cannot
  see. If the seed froze with the state, the pooled axis of those players
  would start from a different row's Elo than it does today and its whole
  trajectory would move. So each stream keeps a `seeded` flag: until it
  commits, its prior mean is re-read from the current row's pre-match Elo,
  which is exactly what the one-stream filter did for a player it had no
  state for. Outside the tuning domain there is no re-read and no commit,
  which is the one place this filter and the shipped one differ: a player
  another stream has given state to now emits it on an ITF row where the
  shipped filter emitted null. No non-null value changes for that reason;
  the indoor axis above is the only thing that moves one.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import numpy as np
import polars as pl

from mvp.atptour.bsr import kernel
from mvp.atptour.bsr.constants import (
    CIRCUIT_INDEX,
    DEFAULT_BSR_CONFIG,
    SURFACE_INDEX,
    BsrConfig,
    StreamConfig,
)

# The shipped stream's twelve names, unchanged: the running serve feature
# selection and the three serve_base_* configs reference them by name.
BSR_VALUE_NAMES: tuple[str, ...] = (
    "bsr_serve_mu", "bsr_serve_sd", "bsr_return_mu", "bsr_return_sd",
    "bsr_serve_surface_mu", "bsr_serve_surface_sd",
    "bsr_return_surface_mu", "bsr_return_surface_sd",
    "bsr_n_serve_obs", "bsr_days_since_serve_obs",
    "bsr_pserve_logit", "bsr_pserve_logit_sd",
    # The pooled stream's indoor residual axes (both sides), added with the
    # per-stream indoor axis; emitted on every row like the new streams'.
    "bsr_serve_indoor_mu", "bsr_serve_indoor_sd",
    "bsr_return_indoor_mu", "bsr_return_indoor_sd",
)

_OVERALL_VAR_FLOOR = 1e-6
_SURFACE_VAR_FLOOR = 1e-7
_NULLS: dict[str, Any] = {name: None for name in BSR_VALUE_NAMES}
_NAN = float("nan")


def new_value_names(streams: tuple[StreamConfig, ...]) -> tuple[str, ...]:
    """Value names for streams 1..n, in slab order.

    Per stream, in this exact order: the server-axis pair, the server's
    surface and indoor residual pairs where the stream carries them, the
    returner-axis pair and the returner's surface and indoor residual pairs
    on the same condition, then the observation count, the days-since clock
    and the matchup logit pair.

    Must stay in lockstep with the emission loop in `_new_values`; the two are
    written as the same sequence of branches so a stream flag cannot change one
    without the other.
    """
    names: list[str] = []
    for st in streams[1:]:
        b = f"bsr_{st.name}"
        names += [f"{b}_mu", f"{b}_sd"]
        if st.has_surface:
            names += [f"{b}_surface_mu", f"{b}_surface_sd"]
        if st.has_indoor:
            names += [f"{b}_indoor_mu", f"{b}_indoor_sd"]
        if st.has_returner:
            names += [f"{b}_r_mu", f"{b}_r_sd"]
            if st.has_surface:
                names += [f"{b}_r_surface_mu", f"{b}_r_surface_sd"]
            if st.has_indoor:
                names += [f"{b}_r_indoor_mu", f"{b}_r_indoor_sd"]
        names += [
            f"{b}_n_obs", f"{b}_days_since", f"{b}_logit", f"{b}_logit_sd",
        ]
    return tuple(names)


BSR_NEW_VALUE_NAMES: tuple[str, ...] = new_value_names(DEFAULT_BSR_CONFIG.streams)


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and x == x


class _PlayerState:
    """One player's posterior across every stream.

    Parallel lists indexed by stream: the plan's layout, and the reason the
    per-stream loops below never look a stream up by name. The surface and
    indoor residual lists are None on the streams that do not carry those
    axes, so a missing axis is a TypeError in development rather than a
    silently-updated slot.
    """

    __slots__ = (
        "sm", "sv", "rm", "rv", "ssm", "ssv", "rsm", "rsv",
        "ism", "isv", "irm", "irv",
        "last_s", "last_r", "last_ss", "last_rs", "last_is", "last_ir",
        "n_s", "n_r", "seeded",
    )

    def __init__(self, seeds_s: list[float], seeds_r: list[float],
                 streams: tuple[StreamConfig, ...]) -> None:
        n = len(streams)
        self.sm = list(seeds_s)
        self.sv = [st.v0 for st in streams]
        self.rm = list(seeds_r)
        self.rv = [st.v0 for st in streams]
        self.ssm: list[list[float] | None] = [
            [0.0, 0.0, 0.0] if st.has_surface else None for st in streams
        ]
        self.ssv: list[list[float] | None] = [
            [st.q_surf] * 3 if st.has_surface else None for st in streams
        ]
        self.rsm: list[list[float] | None] = [
            [0.0, 0.0, 0.0] if st.has_surface else None for st in streams
        ]
        self.rsv: list[list[float] | None] = [
            [st.q_surf] * 3 if st.has_surface else None for st in streams
        ]
        self.ism = [0.0] * n
        self.isv = [st.q_indoor for st in streams]
        self.irm = [0.0] * n
        self.irv = [st.q_indoor for st in streams]
        self.last_s: list[date | None] = [None] * n
        self.last_r: list[date | None] = [None] * n
        self.last_ss: list[list[date | None] | None] = [
            [None, None, None] if st.has_surface else None for st in streams
        ]
        self.last_rs: list[list[date | None] | None] = [
            [None, None, None] if st.has_surface else None for st in streams
        ]
        self.last_is: list[date | None] = [None] * n
        self.last_ir: list[date | None] = [None] * n
        self.n_s = [0] * n
        self.n_r = [0] * n
        # Per stream: has this stream's cold-start seed been committed? A
        # player's state now comes into being at the first observation of ANY
        # stream, so without this a stream that has not observed the player yet
        # would be frozen at whatever row happened to create the state. Until a
        # stream commits, its prior mean is re-read from the current row's Elo,
        # exactly as the one-stream filter re-seeded a stateless player on
        # every row.
        self.seeded = [False] * n


class _Pred:
    """A player's state drifted to a match date, for this match's surface and
    venue. Parallel lists indexed by stream, like `_PlayerState`.

    `sm` and `rm` alias the state's own lists — the overall means do not
    drift, only their variances do — so nothing is copied for them.
    """

    __slots__ = ("sm", "sv", "rm", "rv", "ssm", "ssv", "rsm", "rsv",
                 "ism", "isv", "irm", "irv", "last_s", "n_s")

    def __init__(self, sm, sv, rm, rv, ssm, ssv, rsm, rsv,
                 ism, isv, irm, irv, last_s, n_s) -> None:
        self.sm = sm
        self.sv = sv
        self.rm = rm
        self.rv = rv
        self.ssm = ssm
        self.ssv = ssv
        self.rsm = rsm
        self.rsv = rsv
        self.ism = ism
        self.isv = isv
        self.irm = irm
        self.irv = irv
        # Aliases of the state's own lists; read only at capture time, which
        # is before `apply` touches them.
        self.last_s = last_s
        self.n_s = n_s


@dataclass
class BsrCapture:
    """What the driver stashes per match: the two emitted shipped-value dicts,
    the two new-stream value vectors, the slab row the first row wrote, the
    row player's id (so the second row knows which side to copy where) and the
    pending update `apply` needs."""

    player: dict[str, Any]
    opp: dict[str, Any]
    player_new: np.ndarray
    opp_new: np.ndarray
    row: int | None
    player_id: str
    pending: tuple | None


class BsrTracker:
    """Per-player filter state threaded through compute_all_ratings.

    ONE tracker per compute_all_ratings call, like MovTracker: reusing it
    across calls would double-process every match without error.
    """

    def __init__(self, config: BsrConfig | None = None) -> None:
        self.cfg = config or DEFAULT_BSR_CONFIG
        self.streams = self.cfg.streams
        self.n_streams = S = len(self.streams)
        self.new_value_names = new_value_names(self.streams)
        self._n_new = len(self.new_value_names)
        st = self.streams
        # Per-stream constants as arrays the compiled kernel reads.
        self._has_ret = np.array([x.has_returner for x in st], dtype=np.uint8)
        self._has_surf = np.array([x.has_surface for x in st], dtype=np.uint8)
        self._has_ind = np.array([x.has_indoor for x in st], dtype=np.uint8)
        self._q_s = np.array([x.q_s for x in st], dtype=np.float64)
        self._q_r = np.array([x.q_r for x in st], dtype=np.float64)
        self._q_surf = np.array([x.q_surf for x in st], dtype=np.float64)
        self._q_indoor = np.array([x.q_indoor for x in st], dtype=np.float64)
        self._v0 = np.array([x.v0 for x in st], dtype=np.float64)
        self._tau2 = np.array([x.tau2 for x in st], dtype=np.float64)
        self._mu = np.array([list(x.mu_cells) for x in st], dtype=np.float64)
        for x in st:
            for src in (x.seed_src_s, x.seed_src_r):
                if src is not None and src not in _ELO8:
                    raise ValueError(f"stream {x.name}: unknown seed source {src!r}")
        self._seed_src_s = np.array(
            [_ELO8.index(x.seed_src_s) if x.seed_src_s is not None else -1 for x in st],
            dtype=np.int64)
        self._seed_src_r = np.array(
            [_ELO8.index(x.seed_src_r) if x.seed_src_r is not None else -1 for x in st],
            dtype=np.int64)
        self._seed_scale_s = np.array([x.seed_s for x in st], dtype=np.float64)
        self._seed_scale_r = np.array([x.seed_r for x in st], dtype=np.float64)
        # Streams whose seed can be anything but zero: only these need the
        # per-row re-seed of an uncommitted axis.
        self._seedable = np.array(
            [j for j in range(S)
             if (self._seed_src_s[j] >= 0 and self._seed_scale_s[j] != 0.0)
             or (self._seed_src_r[j] >= 0 and self._seed_scale_r[j] != 0.0)],
            dtype=np.int64)
        self._cap_days = float(self.cfg.cap_days)
        self._phi = float(self.cfg.phi_surf)
        self._newton = int(self.cfg.newton)
        # Player table and state arrays (see kernel.py for the layout).
        self._index: dict[str, int] = {}
        self._n_used = 0
        self._alloc(1024)
        # Work buffers reused every row.
        self._W = np.zeros((2, kernel.NF, S), dtype=np.float64)
        self._elo8 = np.full((2, len(_ELO8)), _NAN, dtype=np.float64)
        self._k = np.full((2, S), -1, dtype=np.int64)
        self._n = np.full((2, S), -1, dtype=np.int64)
        self._mask = np.zeros((2, S), dtype=np.uint8)
        self._out16 = np.zeros((2, len(BSR_VALUE_NAMES)), dtype=np.float64)
        self._vec = np.zeros((2, self._n_new), dtype=np.float64)
        self._out: np.ndarray | None = None
        self._out_index: np.ndarray | None = None
        # Set by a caller that will scatter the slab into a larger frame
        # (the aggregator): the slab is allocated at THIS height, so the last
        # frame row need not be a pass row for the sizes to agree.
        self.frame_height: int | None = None
        # Emission self-check: the compiled emitter must write exactly the
        # names `new_value_names` enumerates for these flags, or the slab's
        # columns would be silently misaligned.
        _probe = np.zeros(self._n_new + 64, dtype=np.float64)
        written = kernel.emit_new(
            self._W, 0, 0, -1, False, self._last_s, self._n_s, self._tau2,
            self._has_ret, self._has_surf, self._has_ind, _probe, 0,
        )
        if written != self._n_new:
            raise RuntimeError(
                f"emit_new writes {written} values for {self._n_new} names"
            )

    # ---- state storage -----------------------------------------------------

    def _alloc(self, cap: int) -> None:
        S = self.n_streams

        def f(*shape):
            return np.zeros(shape, dtype=np.float64)

        def i(*shape):
            return np.full(shape, -1, dtype=np.int32)

        self._sm, self._sv = f(S, cap), f(S, cap)
        self._rm, self._rv = f(S, cap), f(S, cap)
        self._ssm, self._ssv = f(S, 3, cap), f(S, 3, cap)
        self._rsm, self._rsv = f(S, 3, cap), f(S, 3, cap)
        self._ism, self._isv = f(S, cap), f(S, cap)
        self._irm, self._irv = f(S, cap), f(S, cap)
        self._last_s, self._last_r = i(S, cap), i(S, cap)
        self._last_ss, self._last_rs = i(S, 3, cap), i(S, 3, cap)
        self._last_is, self._last_ir = i(S, cap), i(S, cap)
        self._n_s = np.zeros((S, cap), dtype=np.int32)
        self._n_r = np.zeros((S, cap), dtype=np.int32)
        self._seeded = np.zeros((S, cap), dtype=np.uint8)
        self._cap = cap

    def _grow(self) -> None:
        old = {k: getattr(self, k) for k in _STATE_ARRAYS}
        self._alloc(self._cap * 2)
        for name, arr in old.items():
            getattr(self, name)[..., : arr.shape[-1]] = arr

    def _kernel_state(self) -> tuple:
        return (
            self._sm, self._sv, self._rm, self._rv,
            self._ssm, self._ssv, self._rsm, self._rsv,
            self._ism, self._isv, self._irm, self._irv,
            self._last_s, self._last_r, self._last_ss, self._last_rs,
            self._last_is, self._last_ir,
        )

    def _new_slot(self, side: int) -> int:
        """A fresh, seeded slot for a player first seen on this row. It is
        NOT registered in the player table; `apply` registers it, so a
        capture that is never applied leaves no state behind (the reference
        stored a new state only at `apply`)."""
        if self._n_used >= self._cap:
            self._grow()
        i = self._n_used
        self._n_used += 1
        kernel.init_player(
            i, self._elo8[side], self._W,
            *self._kernel_state(), self._n_s, self._n_r, self._seeded,
            self._v0, self._q_surf, self._q_indoor,
            self._seed_src_s, self._seed_src_r,
            self._seed_scale_s, self._seed_scale_r,
        )
        return i

    @property
    def _state(self) -> _StateMap:
        """Read-only per-player view of the arrays, for tests and diagnostics."""
        return _StateMap(self)

    # ---- seams -------------------------------------------------------------

    def begin(self, n_rows: int, index: np.ndarray | None = None) -> None:
        """Preallocate the new-stream output slab: (2 * n_new, n_out) float32,
        rows 0..n_new-1 the player side and n_new..2n_new-1 the opponent side.

        With `index` (the frame position of each pass row in a larger frame)
        the slab is allocated at that frame's length and each pass row is
        written at its scattered position directly, so no second full-length
        copy is ever needed. NaN is the null marker (see the module docstring).
        """
        if index is not None:
            self._out_index = np.asarray(index, dtype=np.int64)
            n_out = int(self._out_index.max()) + 1 if len(self._out_index) else 0
            n_out = max(n_out, n_rows, self.frame_height or 0)
        else:
            self._out_index = None
            n_out = n_rows
        self._out = np.full((2 * self._n_new, n_out), _NAN, dtype=np.float32)

    @property
    def slab_is_scattered(self) -> bool:
        """True when `begin` was given a full-frame row index: the slab is
        that frame's length and only `scatter_series` may attach it."""
        return self._out_index is not None

    def output_columns(self) -> list[str]:
        """The shipped stream's names, both sides. The new streams' columns do
        NOT go through the driver's per-column lists — see `new_series`."""
        return [
            f"{side}_{name}"
            for name in BSR_VALUE_NAMES
            for side in ("player", "opp")
        ]

    def new_output_columns(self) -> list[str]:
        return (
            [f"player_{n}" for n in self.new_value_names]
            + [f"opp_{n}" for n in self.new_value_names]
        )

    def new_series(self) -> list[pl.Series]:
        """The new-stream columns as polars series, zero-copy over the slab.

        `nan_to_null=True` is not optional: the slab marks a missing value with
        NaN, and a Float32 column full of NaN reports a null count of zero.
        """
        if self._out is None:
            return []
        names = self.new_output_columns()
        out = self._out
        self._out = None
        return [
            pl.Series(name, out[j], nan_to_null=True)
            for j, name in enumerate(names)
        ]

    def scatter_series(self, index: np.ndarray, n_out: int) -> list[pl.Series]:
        """The new-stream columns scattered to `n_out` rows at `index`.

        When `begin` was given the index the slab already IS the scattered
        frame and this is `new_series`; otherwise the rows are gathered into a
        full-length slab (a second copy alive for one assignment).
        """
        if self._out is None:
            return []
        if self._out_index is not None:
            if self._out.shape[1] < n_out:
                # The caller's frame is longer than max(index) + 1 (its last
                # rows are not pass rows); pad the slab's columns with NaN.
                pad = np.full((2 * self._n_new, n_out - self._out.shape[1]),
                              _NAN, dtype=np.float32)
                self._out = np.concatenate([self._out, pad], axis=1)
            return self.new_series()
        dest = np.full((2 * self._n_new, n_out), _NAN, dtype=np.float32)
        dest[:, index] = self._out
        self._out = None
        names = self.new_output_columns()
        return [
            pl.Series(name, dest[j], nan_to_null=True)
            for j, name in enumerate(names)
        ]

    @staticmethod
    def append_output(
        output: dict[str, Any],
        player_vals: dict[str, Any],
        opp_vals: dict[str, Any],
    ) -> None:
        for name in BSR_VALUE_NAMES:
            output[f"player_{name}"].append(player_vals[name])
            output[f"opp_{name}"].append(opp_vals[name])

    def _slab_col(self, row: int) -> int:
        return int(self._out_index[row]) if self._out_index is not None else row

    def replay_row(self, row: int, cap: BsrCapture, player_id: str) -> None:
        """Write the cached capture into the slab for the match's second row.

        The second row is the same match seen from the other side, so the two
        halves of the column swap. Nothing is recomputed.
        """
        out = self._out
        if out is None or cap.row is None:
            return
        h = self._n_new
        src = self._slab_col(cap.row)
        dst = self._slab_col(row)
        if player_id == cap.player_id:
            out[:, dst] = out[:, src]
        else:
            out[:h, dst] = out[h:, src]
            out[h:, dst] = out[:h, src]

    def _fill_elo8(self, side: int, elo: dict[str, Any] | None) -> None:
        row = self._elo8[side]
        if elo is None:
            row[:] = _NAN
            return
        get = elo.get
        for k, key in enumerate(_ELO8):
            v = get(key)
            row[k] = float(v) if _finite(v) else _NAN

    def _fill_counts(self, side: int, k: Any, n: Any) -> None:
        """Per-stream counts for one side into the work arrays. The driver
        hands numpy rows (-1 already marking missing); tests hand lists that
        may carry None."""
        kk = self._k[side]
        nn = self._n[side]
        if isinstance(k, np.ndarray) and isinstance(n, np.ndarray):
            kk[:] = k
            nn[:] = n
            return
        for j in range(self.n_streams):
            kj = k[j]
            nj = n[j]
            kk[j] = -1 if kj is None else int(kj)
            nn[j] = -1 if nj is None else int(nj)

    def capture_match(
        self,
        player_id: str,
        opp_id: str,
        surface: str | None,
        circuit: str | None,
        indoor: Any,
        match_date: Any,
        k_p: list,
        n_p: list,
        k_o: list,
        n_o: list,
        elo_player: dict[str, Any],
        elo_opp: dict[str, Any],
        row: int | None = None,
    ) -> BsrCapture:
        """PRE-match values for both players, computed from state drifted to
        `match_date`; nothing is stored until `apply`.

        `k_p`/`n_p` are the player's per-stream counts when the player serves
        (observation 1); `k_o`/`n_o` the opponent's (observation 2). Both are
        indexed by stream in `constants.STREAMS` order, with -1 (or any n <= 0,
        or k outside [0, n]) marking "this stream has nothing to say about this
        match" — that stream alone is skipped.

        `elo_player` / `elo_opp` are the pre-match Elo capture dicts; each
        stream names the component that seeds it.
        """
        cfg = self.cfg
        if isinstance(match_date, datetime):
            d: date | None = match_date.date()
        elif isinstance(match_date, date):
            d = match_date
        else:
            d = None
        day = d.toordinal() if d is not None else -1
        s = SURFACE_INDEX.get(surface, 0)
        circ = CIRCUIT_INDEX.get(circuit)
        is_indoor = bool(indoor)
        in_domain = circ is not None and d is not None and d >= cfg.start_date
        cell = s * 4 + (circ or 0) * 2 + (1 if is_indoor else 0)

        self._fill_elo8(0, elo_player)
        self._fill_elo8(1, elo_opp)
        if in_domain:
            self._fill_counts(0, k_p, n_p)
            self._fill_counts(1, k_o, n_o)

        ia = self._index.get(player_id, -1)
        ib = self._index.get(opp_id, -1)
        new: dict[str, int] = {}
        used_before = self._n_used
        if in_domain:
            if ia < 0:
                ia = new[player_id] = self._new_slot(0)
            if ib < 0:
                ib = new[opp_id] = self._new_slot(1)

        W = self._W
        mask = self._mask
        vec = self._vec
        out16 = self._out16
        n_obs = kernel.capture_all(
            ia, ib, s, cell, day, is_indoor, in_domain,
            self._elo8, self._k, self._n, mask, W, out16, vec,
            *self._kernel_state(), self._n_s, self._seeded,
            self._q_s, self._q_r, self._q_surf, self._q_indoor,
            self._cap_days, self._phi, self._tau2, self._mu,
            self._has_ret, self._has_surf, self._has_ind,
            self._seed_src_s, self._seed_src_r,
            self._seed_scale_s, self._seed_scale_r, self._seedable,
        )
        vals_a = _dict16(out16[0])
        vals_b = _dict16(out16[1])
        out = self._out
        if out is not None and row is not None:
            h = self._n_new
            col = self._slab_col(row)
            out[:h, col] = vec[0]
            out[h:, col] = vec[1]
        pending = None
        if n_obs > 0:
            # Indices 4 and 5 are the two observations (None when that side
            # observed nothing), as the reference tuple laid them out.
            obs1 = obs2 = None
            if mask[0].any():
                obs1 = (self._k[0].copy(), self._n[0].copy(), mask[0].copy())
            if mask[1].any():
                obs2 = (self._k[1].copy(), self._n[1].copy(), mask[1].copy())
            pending = (ia, ib, s, day, obs1, obs2, is_indoor, W.copy(), new)
        elif new:
            # No observation: the fresh slots are discarded, as the reference
            # never stored a state it did not apply.
            self._n_used = used_before
        return BsrCapture(
            player=vals_a, opp=vals_b, player_new=vec[0].copy(), opp_new=vec[1].copy(),
            row=row, player_id=player_id, pending=pending,
        )

    def apply(self, cap: BsrCapture) -> None:
        """Commit the drift for the axes an observation touches and run the
        update, per stream, from the capture's pre-match state. Both
        observations of a match are taken from that state; today they touch
        disjoint slots, so this equals applying them one after the other."""
        if cap.pending is None:
            return
        ia, ib, s, day, obs1, obs2, is_indoor, W, new = cap.pending
        self._index.update(new)
        S = self.n_streams
        zk = np.full(S, -1, dtype=np.int64)
        zm = np.zeros(S, dtype=np.uint8)
        k_a, n_a, m_a = obs1 if obs1 is not None else (zk, zk, zm)
        k_b, n_b, m_b = obs2 if obs2 is not None else (zk, zk, zm)
        kernel.apply_match(
            ia, ib, s, day, is_indoor, W, k_a, n_a, k_b, n_b, m_a, m_b,
            *self._kernel_state(), self._n_s, self._n_r, self._seeded,
            self._tau2, self._newton, self._has_ret, self._has_surf, self._has_ind,
        )


_STATE_ARRAYS = (
    "_sm", "_sv", "_rm", "_rv", "_ssm", "_ssv", "_rsm", "_rsv",
    "_ism", "_isv", "_irm", "_irv",
    "_last_s", "_last_r", "_last_ss", "_last_rs", "_last_is", "_last_ir",
    "_n_s", "_n_r", "_seeded",
)

# The Elo capture components a stream may seed from, in the order the kernel
# indexes them. Every one is DEFAULT_ELO(1500)-centred.
_ELO8 = (
    "serve_elo", "return_elo", "second_serve_reliability", "first_serve_power",
    "ace_resistance", "serve_clutch", "return_clutch", "tb_clutch",
)

_INT16 = frozenset({8, 9})  # n_serve_obs, days_since_serve_obs


def _dict16(row: np.ndarray) -> dict[str, Any]:
    """The shipped value names from the kernel's float row: None for NaN,
    Python ints for the two counters, floats otherwise."""
    vals = row.tolist()
    out = dict(zip(BSR_VALUE_NAMES, vals))
    for k in _INT16:
        v = vals[k]
        out[BSR_VALUE_NAMES[k]] = None if v != v else int(v)
    for k, v in enumerate(vals):
        if v != v and k not in _INT16:
            out[BSR_VALUE_NAMES[k]] = None
    return out


def _ordinal_to_date(o: int) -> date | None:
    return date.fromordinal(int(o)) if o >= 0 else None


class _StateView:
    """One player's state as the reference `_PlayerState` exposed it: lists
    indexed by stream (surface lists are 3-lists, None on streams without the
    axis), dates for the clocks, ints for the counters, bools for the seeded
    flags. Read-only snapshots, for tests and diagnostics."""

    def __init__(self, t: BsrTracker, i: int) -> None:
        self._t = t
        self._i = i

    def _col(self, name: str) -> list:
        return getattr(self._t, name)[..., self._i].tolist()

    def _surf(self, name: str) -> list:
        has = self._t._has_surf
        return [row if has[j] else None for j, row in enumerate(self._col(name))]

    def _dates(self, name: str) -> list:
        return [_ordinal_to_date(x) for x in self._col(name)]

    def _surf_dates(self, name: str) -> list:
        has = self._t._has_surf
        return [
            [_ordinal_to_date(x) for x in row] if has[j] else None
            for j, row in enumerate(self._col(name))
        ]

    sm = property(lambda self: self._col("_sm"))
    sv = property(lambda self: self._col("_sv"))
    rm = property(lambda self: self._col("_rm"))
    rv = property(lambda self: self._col("_rv"))
    ssm = property(lambda self: self._surf("_ssm"))
    ssv = property(lambda self: self._surf("_ssv"))
    rsm = property(lambda self: self._surf("_rsm"))
    rsv = property(lambda self: self._surf("_rsv"))
    ism = property(lambda self: self._col("_ism"))
    isv = property(lambda self: self._col("_isv"))
    irm = property(lambda self: self._col("_irm"))
    irv = property(lambda self: self._col("_irv"))
    n_s = property(lambda self: self._col("_n_s"))
    n_r = property(lambda self: self._col("_n_r"))
    seeded = property(lambda self: [bool(x) for x in self._col("_seeded")])
    last_s = property(lambda self: self._dates("_last_s"))
    last_r = property(lambda self: self._dates("_last_r"))
    last_is = property(lambda self: self._dates("_last_is"))
    last_ir = property(lambda self: self._dates("_last_ir"))
    last_ss = property(lambda self: self._surf_dates("_last_ss"))
    last_rs = property(lambda self: self._surf_dates("_last_rs"))


class _StateMap:
    """Mapping view over the tracker's player table."""

    def __init__(self, t: BsrTracker) -> None:
        self._t = t

    def __len__(self) -> int:
        return len(self._t._index)

    def __contains__(self, pid: object) -> bool:
        return pid in self._t._index

    def __iter__(self):
        return iter(self._t._index)

    def keys(self):
        return self._t._index.keys()

    def __getitem__(self, pid: str) -> _StateView:
        return _StateView(self._t, self._t._index[pid])

    def get(self, pid: str, default: Any = None) -> Any:
        i = self._t._index.get(pid)
        return default if i is None else _StateView(self._t, i)

    def items(self):
        return [(pid, _StateView(self._t, i)) for pid, i in self._t._index.items()]

    def values(self):
        return [_StateView(self._t, i) for i in self._t._index.values()]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, dict):
            return not other and not self._t._index
        return NotImplemented
