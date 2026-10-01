"""The newcomer terms of the serve/return skill filter.

A player with no earlier singles match carrying serve statistics is a newcomer:
a stream that first commits for them adds an offset to their prior means, and
before each later observation on an axis their mean gains a decaying drift
(`StreamConfig.nc_*`, `BsrConfig.newcomer_tau_d`). These tests pin the
neutral case to the previous filter bit for bit, the two terms' placement, the
newcomer definition (which reaches outside the domain), and the arithmetic
against an independent transcription of the research kernel
(`scripts/style_ratings/style_study.py::_kernel_nc`).
"""

import math
import random
import subprocess
import sys
import types
from dataclasses import fields, replace
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mvp.atptour.bsr.constants import (
    N_STREAMS,
    STREAM_INDEX,
    STREAMS,
    BsrConfig,
    StreamConfig,
    bsr_input_columns,
    mirror_col,
)
from mvp.atptour.bsr.filter import BsrTracker
from mvp.atptour.ratings.compute import compute_all_ratings
from tests.atptour.bsr._configs import neutral_newcomer

# The commit before the newcomer terms: the filter they must reproduce when
# every term is zero.
BASELINE_REV = "5836ce7"
REPO = Path(__file__).resolve().parents[3]
SERVE = STREAM_INDEX["serve"]
D0 = date(2020, 1, 10)


def _with_serve_terms(cfg: BsrConfig | None = None, **terms) -> BsrConfig:
    """Newcomer terms on the `serve` stream only, every other stream neutral."""
    cfg = neutral_newcomer(cfg or BsrConfig())
    s0 = replace(cfg.streams[0], **terms)
    return replace(cfg, streams=(s0,) + tuple(cfg.streams[1:]))


def _counts(serve: tuple | None) -> tuple[list[int], list[int]]:
    k = [-1] * N_STREAMS
    n = [-1] * N_STREAMS
    if serve is not None:
        k[SERVE], n[SERVE] = serve
    return k, n


def _elo(serve: float, ret: float) -> dict[str, float]:
    return {
        "serve_elo": serve, "return_elo": ret,
        "first_serve_power": 1500.0, "second_serve_reliability": 1500.0,
        "ace_resistance": 1500.0, "serve_clutch": 1500.0,
        "return_clutch": 1500.0, "tb_clutch": 1500.0,
    }


def _cap(t, a, b, sa, sb, d=D0, circuit="tour", surface="Hard",
         elo_a=(1600.0, 1550.0), elo_b=(1500.0, 1500.0), row=None):
    """One match on the serve stream: `sa`/`sb` are (k, n) for a and b
    serving, or None."""
    kp, np_ = _counts(sa)
    ko, no = _counts(sb)
    return t.capture_match(a, b, surface, circuit, False, d, kp, np_, ko, no,
                           _elo(*elo_a), _elo(*elo_b), row)


# ---- neutral terms reproduce the previous filter exactly ----------------------


def _git_show(rev: str, path: str) -> str:
    return subprocess.run(
        ["git", "-C", str(REPO), "show", f"{rev}:{path}"],
        capture_output=True, text=True, check=True,
    ).stdout


def _load_baseline():
    """constants, kernel and filter as of BASELINE_REV, as live modules. The
    kernel compiles uncached (an exec'd source has no file to cache against)."""
    try:
        srcs = {
            m: _git_show(BASELINE_REV, f"src/mvp/atptour/bsr/{m}.py")
            for m in ("constants", "kernel", "filter")
        }
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        pytest.skip(f"cannot read {BASELINE_REV} from git: {exc}")
    mods = {}
    for m in ("constants", "kernel", "filter"):
        src = srcs[m].replace("cache=True", "cache=False")
        src = src.replace("from mvp.atptour.bsr import kernel",
                          "import bsr_prev_kernel as kernel")
        src = src.replace("mvp.atptour.bsr.constants", "bsr_prev_constants")
        mod = types.ModuleType(f"bsr_prev_{m}")
        # In sys.modules before exec: @dataclass resolves annotations through it.
        sys.modules[f"bsr_prev_{m}"] = mod
        exec(compile(src, f"<bsr_prev_{m}>", "exec"), mod.__dict__)
        mods[m] = mod
    return mods


def _frame(n_matches: int = 90, seed: int = 3) -> pl.DataFrame:
    """Two rows per match, every stream's inputs fed (often invalidly, which
    the filter skips), some matches before the domain, some on ITF, some
    without counts."""
    rng = random.Random(seed)
    players = [f"P{i}" for i in range(10)]
    player_cols = sorted({c for st in STREAMS for c in st.input_columns()})
    rows = []
    d = date(2014, 9, 1)
    for m in range(n_matches):
        a, b = rng.sample(players, 2)
        d = d + timedelta(days=rng.randint(0, 25))
        circuit = rng.choice(["tour", "chal", "chal", "itf"])
        none = rng.random() < 0.15
        va, vb = (
            {c: None if none or rng.random() < 0.1 else rng.randint(0, 40)
             for c in player_cols}
            for _ in range(2)
        )
        for c in ("pts_service_pts_played",):
            if va[c] is not None:
                va[c] += 40
            if vb[c] is not None:
                vb[c] += 40
        won = rng.random() < 0.5
        common = dict(
            match_uid=f"m{m}", surface=rng.choice(["Hard", "Clay", "Grass"]),
            round="R32", round_order=7, tournament_start_date=d - timedelta(days=2),
            tournament_level="250", effective_match_date=d,
            indoor=rng.random() < 0.2, circuit=circuit, player_rank=None, opp_rank=None,
        )
        for p, o, vp, vo, w in ((a, b, va, vb, won), (b, a, vb, va, not won)):
            r = dict(common, player_id=p, opp_id=o, won=w)
            for c in player_cols:
                r[c] = vp[c]
                r[mirror_col(c)] = vo[c]
            rows.append(r)
    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.col("player_rank").cast(pl.Int64), pl.col("opp_rank").cast(pl.Int64),
    )
    return df.with_columns([
        pl.lit(None, dtype=pl.Int32).alias(c)
        for c in bsr_input_columns() if c not in df.columns
    ])


def test_neutral_terms_match_previous_filter():
    """Every new term at zero: the filter is the baseline revision's, to the
    bit, on every emitted column and every state array, with the baseline's
    own constants."""
    prev = _load_baseline()
    prev_cfg = prev["constants"].BsrConfig()
    names = {f.name for f in fields(StreamConfig)}
    streams = tuple(
        StreamConfig(**{
            f.name: getattr(ps, f.name) for f in fields(ps) if f.name in names
        })
        for ps in prev_cfg.streams
    )
    scal = {k: getattr(prev_cfg, k) for k in
            ("q_s", "q_r", "cap_days", "q_surf", "phi_surf", "v0", "seed_es",
             "seed_er", "tau2", "newton", "start_date", "mu_cells")}
    cfg = BsrConfig(streams=streams, **scal)
    assert not any(s.nc_c_s or s.nc_c_r or s.nc_dr_s or s.nc_dr_r for s in cfg.streams)
    df = _frame()
    t_old = prev["filter"].BsrTracker(prev_cfg)
    t_new = BsrTracker(cfg)
    out_old = compute_all_ratings(df.clone(), bsr_tracker=t_old)
    out_new = compute_all_ratings(df.clone(), bsr_tracker=t_new)
    cols = [c for c in out_new.columns if "_bsr_" in c]
    assert cols and set(cols) == {c for c in out_old.columns if "_bsr_" in c}
    assert_frame_equal(out_old.select(cols), out_new.select(cols), check_exact=True)
    assert t_old._index == t_new._index
    n = t_new._n_used
    for name in prev["filter"]._STATE_ARRAYS:
        a, b = getattr(t_old, name)[..., :n], getattr(t_new, name)[..., :n]
        assert np.array_equal(a, b, equal_nan=True), name


# ---- placement of the two terms ----------------------------------------------


def test_offset_lands_once_at_first_commit():
    cfg = _with_serve_terms(nc_c_s=0.5, nc_c_r=-0.25)
    t = BsrTracker(cfg)
    seed_s = cfg.streams[0].seed_s * (1600.0 - 1500.0) / 100.0
    seed_r = cfg.streams[0].seed_r * (1550.0 - 1500.0) / 100.0
    cap = _cap(t, "A", "B", (50, 80), (40, 80))
    assert cap.player["bsr_serve_mu"] == seed_s + 0.5
    assert cap.player["bsr_return_mu"] == seed_r + -0.25
    assert cap.opp["bsr_serve_mu"] == 0.0 + 0.5  # B's Elo is 1500: a zero seed
    t.apply(cap)
    a = t._state["A"]
    assert a.newc[SERVE] is True and a.seeded[SERVE] is True
    # A served: the server axis moved from seed + offset; the return axis was
    # observed through B serving, so it moved too. A later match adds nothing.
    sm, rm = a.sm[SERVE], a.rm[SERVE]
    cap2 = _cap(t, "A", "B", None, None, d=D0 + timedelta(days=30))
    assert cap2.player["bsr_serve_mu"] == sm
    assert cap2.player["bsr_return_mu"] == rm


def test_offset_is_stored_but_not_reapplied_without_observation():
    """A newcomer whose stream commits stores seed + offset on an unobserved
    axis exactly (the returner axis of a server-only match)."""
    cfg = _with_serve_terms(nc_c_s=0.5, nc_c_r=-0.25)
    t = BsrTracker(cfg)
    seed_r = cfg.streams[0].seed_r * (1550.0 - 1500.0) / 100.0
    t.apply(_cap(t, "A", "B", (50, 80), None))  # only A serves
    assert t._state["A"].rm[SERVE] == seed_r + -0.25  # committed, untouched
    assert t._state["B"].sm[SERVE] == 0.0 + 0.5


def test_drift_predicted_then_committed_only_on_observed_axis():
    cfg = _with_serve_terms(nc_dr_s=0.2, nc_dr_r=0.1)
    tau = cfg.newcomer_tau_d
    t = BsrTracker(cfg)
    seed_s = cfg.streams[0].seed_s * (1600.0 - 1500.0) / 100.0
    seed_r = cfg.streams[0].seed_r * (1550.0 - 1500.0) / 100.0
    cap = _cap(t, "A", "B", (50, 80), None)  # only A serves
    assert cap.player["bsr_serve_mu"] == seed_s + 0.2
    assert cap.player["bsr_return_mu"] == seed_r + 0.1
    assert cap.opp["bsr_serve_mu"] == 0.0 + 0.2
    t.apply(cap)
    a, b = t._state["A"], t._state["B"]
    # observed axes: A's serve and B's return carry the drift plus the update
    assert a.sm[SERVE] != seed_s and a.n_s[SERVE] == 1
    assert b.rm[SERVE] != 0.0 and b.n_r[SERVE] == 1
    # unobserved axes: committed without the drift
    assert a.rm[SERVE] == seed_r
    assert b.sm[SERVE] == 0.0
    # a later prediction adds the drift for the observations made so far, and
    # a capture that is never applied leaves the state alone
    sm = a.sm[SERVE]
    cap2 = _cap(t, "A", "B", None, None, d=D0 + timedelta(days=30))
    assert math.isclose(
        cap2.player["bsr_serve_mu"], sm + 0.2 * math.exp(-1 / tau), rel_tol=1e-14
    )
    assert cap2.player["bsr_return_mu"] == seed_r + 0.1 * math.exp(-0 / tau)
    assert t._state["A"].sm[SERVE] == sm


def test_newcomer_flag_counts_out_of_domain_stats():
    cfg = _with_serve_terms(nc_c_s=0.5)
    t = BsrTracker(cfg)
    t.commit_log = []
    # X has a 2014 (pre-domain) tour match with serve statistics; Y's only
    # earlier match is an ITF row without any.
    t.apply(_cap(t, "X", "Q", (40, 70), (30, 70), d=date(2014, 6, 1)))
    t.apply(_cap(t, "Y", "R", None, None, d=date(2016, 1, 5), circuit="itf"))
    assert t._prior_stats == {"X": 1, "Q": 1}
    seed_x = cfg.streams[0].seed_s * (1600.0 - 1500.0) / 100.0
    cap = _cap(t, "X", "Y", (50, 80), (40, 80), d=date(2016, 2, 1))
    assert cap.player["bsr_serve_mu"] == seed_x  # X: not a newcomer
    assert cap.opp["bsr_serve_mu"] == 0.0 + 0.5  # Y: a newcomer
    t.apply(cap)
    flags = {(pid, j): f for pid, j, f in t.commit_log}
    assert flags[("X", SERVE)] is False and flags[("Y", SERVE)] is True
    assert t._state["X"].newc[SERVE] is False and t._state["Y"].newc[SERVE] is True
    assert t._prior_stats == {"X": 2, "Q": 1, "Y": 1}


def test_pending_live_row_does_not_count():
    t = BsrTracker(_with_serve_terms(nc_c_s=0.5))
    t.apply(_cap(t, "A", "B", None, None))
    assert t._prior_stats == {}
    # an ace count alone is serve statistics too
    kp, np_ = _counts(None)
    kp[STREAM_INDEX["ace"]] = 0
    ko, no = _counts(None)
    t.apply(t.capture_match("A", "B", "Hard", "itf", False, D0, kp, np_, ko, no,
                            _elo(1500.0, 1500.0), _elo(1500.0, 1500.0)))
    assert t._prior_stats == {"A": 1}


def test_needs_a_stat_stream():
    s0 = replace(neutral_newcomer().streams[STREAM_INDEX["w1"]], nc_c_s=0.1)
    with pytest.raises(ValueError, match="'serve' or 'ace'"):
        BsrTracker(BsrConfig(streams=(s0,)))


# ---- arithmetic against an independent transcription of the research kernel ---


def _sig(x: float) -> float:
    """The kernel's sigmoid, branch for branch."""
    if x >= 0.0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


class _Ref:
    """`_kernel_nc` for one stream with a returner and surface axes, no indoor
    axis, v0_mult 1, no extra process noise, no first-match term: the offset at
    a player's first sight in the stream, the drift added to the mean before
    each observation on that axis, then the Newton/Laplace update."""

    def __init__(self, st: StreamConfig, cfg: BsrConfig):
        self.st, self.cfg = st, cfg
        self.p: dict[str, dict] = {}

    def _init(self, pid, elo, new):
        st = self.st
        sm = st.seed_s * (elo[0] - 1500.0) / 100.0
        rm = st.seed_r * (elo[1] - 1500.0) / 100.0
        if new:
            sm += st.nc_c_s
            rm += st.nc_c_r
        self.p[pid] = dict(
            sm=sm, sv=st.v0, rm=rm, rv=st.v0, ssm=[0.0] * 3, ssv=[st.q_surf] * 3,
            rsm=[0.0] * 3, rsv=[st.q_surf] * 3, last_s=-1, last_r=-1,
            last_ss=[-1] * 3, last_rs=[-1] * 3, cs=0, cr=0, new=new,
        )

    def obs(self, a, b, y, n, s, day, cell, elo_a, elo_b, new_a, new_b) -> float:
        st, cfg = self.st, self.cfg
        if a not in self.p:
            self._init(a, elo_a, new_a)
        if b not in self.p:
            self._init(b, elo_b, new_b)
        A, B = self.p[a], self.p[b]
        if A["last_s"] >= 0:
            A["sv"] += st.q_s * min(day - A["last_s"], cfg.cap_days)
        if B["last_r"] >= 0:
            B["rv"] += st.q_r * min(day - B["last_r"], cfg.cap_days)
        if A["new"]:
            A["sm"] += st.nc_dr_s * math.exp(-A["cs"] / cfg.newcomer_tau_d)
        if B["new"]:
            B["rm"] += st.nc_dr_r * math.exp(-B["cr"] / cfg.newcomer_tau_d)
        phi = cfg.phi_surf
        if A["last_ss"][s] >= 0:
            A["ssm"][s] *= phi
            A["ssv"][s] = phi * phi * A["ssv"][s] + st.q_surf
        if B["last_rs"][s] >= 0:
            B["rsm"][s] *= phi
            B["rsv"][s] = phi * phi * B["rsv"][s] + st.q_surf
        eta = st.mu_cells[cell] + A["sm"]
        vv = A["sv"]
        eta += A["ssm"][s]
        vv += A["ssv"][s]
        eta -= B["rm"]
        vv += B["rv"]
        eta -= B["rsm"][s]
        vv += B["rsv"][s]
        vt = vv + st.tau2
        m = eta
        for _ in range(cfg.newton):
            p = _sig(m)
            g = (y - n * p) - (m - eta) / vt
            h = n * p * (1.0 - p) + 1.0 / vt
            m += g / h
        p = _sig(m)
        v_post = 1.0 / (n * p * (1.0 - p) + 1.0 / vt)
        delta, shrink = m - eta, vt - v_post
        w = A["sv"] / vt
        A["sm"] += w * delta
        A["sv"] -= w * w * shrink
        w = A["ssv"][s] / vt
        A["ssm"][s] += w * delta
        A["ssv"][s] -= w * w * shrink
        w = B["rv"] / vt
        B["rm"] -= w * delta
        B["rv"] -= w * w * shrink
        w = B["rsv"][s] / vt
        B["rsm"][s] -= w * delta
        B["rsv"][s] -= w * w * shrink
        A["sv"] = max(A["sv"], 1e-6)
        B["rv"] = max(B["rv"], 1e-6)
        A["ssv"][s] = max(A["ssv"][s], 1e-7)
        B["rsv"][s] = max(B["rsv"][s], 1e-7)
        A["last_s"], A["last_ss"][s], A["cs"] = day, day, A["cs"] + 1
        B["last_r"], B["last_rs"][s], B["cr"] = day, day, B["cr"] + 1
        return eta


def test_reference_parity():
    """Pre-match eta of every serve observation, tracker against the
    transcription, over a sequence with newcomers, veterans (serve statistics
    before the domain), ITF rows, count-less rows and one-sided matches."""
    cfg = _with_serve_terms(nc_c_s=0.3, nc_c_r=-0.2, nc_dr_s=0.15, nc_dr_r=0.1)
    cfg = replace(cfg, streams=(replace(cfg.streams[0], has_indoor=False),)
                  + tuple(cfg.streams[1:]))
    st = cfg.streams[0]
    ref = _Ref(st, cfg)
    t = BsrTracker(cfg)
    rng = random.Random(17)
    players = [f"P{i}" for i in range(1, 11)]
    prior: dict[str, int] = {}
    checked = 0
    # P1 and P2 have serve statistics before the domain, P3 and P4 on an ITF
    # row; P5 and P6 meet once in domain without counts (no statistics). The
    # rest of the field starts as newcomers.
    script = [
        ("P1", "P2", (40, 70), (35, 70), date(2014, 5, 1), "tour"),
        ("P3", "P4", (30, 60), (28, 60), date(2015, 1, 10), "itf"),
        ("P5", "P6", None, None, date(2015, 1, 20), "chal"),
    ]
    d = date(2015, 2, 1)
    for k in range(90):
        if k < len(script):
            a, b, sa, sb, dd, circuit = script[k]
        else:
            a, b = rng.sample(players, 2)
            d = d + timedelta(days=rng.randint(0, 6))
            dd = d
            circuit = rng.choice(["tour", "chal", "chal", "itf"])
            r = rng.random()
            sa = None if r < 0.1 else (rng.randint(20, 60), 80)
            sb = None if r < 0.25 else (rng.randint(20, 60), 80)
        d_row = dd
        surface = rng.choice(["Hard", "Clay", "Grass"])
        elo_a = (1400.0 + 200.0 * rng.random(), 1400.0 + 200.0 * rng.random())
        elo_b = (1400.0 + 200.0 * rng.random(), 1400.0 + 200.0 * rng.random())
        d = max(d, d_row)
        new_a, new_b = prior.get(a, 0) == 0, prior.get(b, 0) == 0
        cap = _cap(t, a, b, sa, sb, d=d_row, circuit=circuit, surface=surface,
                   elo_a=elo_a, elo_b=elo_b)
        in_domain = circuit != "itf" and d_row >= cfg.start_date
        if in_domain:
            s = {"Hard": 0, "Clay": 1, "Grass": 2}[surface]
            cell = s * 4 + (0 if circuit == "tour" else 1) * 2
            day = d_row.toordinal()
            if sa is not None:
                eta = ref.obs(a, b, sa[0], sa[1], s, day, cell,
                              elo_a, elo_b, new_a, new_b)
                assert abs(cap.player["bsr_pserve_logit"] - eta) < 1e-12, (d, a)
                checked += 1
            if sb is not None:
                eta = ref.obs(b, a, sb[0], sb[1], s, day, cell,
                              elo_b, elo_a, new_b, new_a)
                assert abs(cap.opp["bsr_pserve_logit"] - eta) < 1e-12, (d, b)
                checked += 1
        t.apply(cap)
        for pid, sx in ((a, sa), (b, sb)):
            if sx is not None:
                prior[pid] = prior.get(pid, 0) + 1
    assert checked > 80
    newcomers = [pid for pid, P in ref.p.items() if P["new"]]
    assert len(newcomers) >= 5 and {"P1", "P2", "P3", "P4"}.isdisjoint(newcomers)
    # the drift is exercised well past the first observation on both axes
    assert max(ref.p[pid]["cs"] for pid in newcomers) >= 5
    assert max(ref.p[pid]["cr"] for pid in newcomers) >= 5
    for pid, P in ref.p.items():
        s = t._state[pid]
        for k in ("sm", "rm", "sv", "rv"):
            assert abs(getattr(s, k)[SERVE] - P[k]) < 1e-12, (pid, k)
        assert s.newc[SERVE] is P["new"], pid
