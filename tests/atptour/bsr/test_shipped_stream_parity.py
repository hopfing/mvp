"""The 21-stream tracker reproduces the one-stream tracker it grew out of.

The probe-parity test pins the pooled `serve` stream against an independent
implementation (`scripts/bsr/probe_bsr.py`) at 1e-9. This pins it against the
code that actually shipped, at exact equality: the filter as of 26e4bbf is
read out of git, loaded beside the current one, and both are driven over the
same matches. Every one of the twelve `bsr_*` values must come back `==`, not
`approx` — those columns are in a running feature selection and in three
`serve_base_*` configs, and a change in the last bit is still a change.

The one deliberate exception is the indoor residual axis the `serve` stream
gained: the shipped filter has none, so on an indoor row the twelve values
legitimately move. Every test here therefore runs under
`_serve_outdoor_config()`, which switches that axis back off and leaves the
seven shipped knobs and the mu cells exactly as they are. That is the config
under which the two are the same model, and it is what makes "identical" a
meaningful claim rather than a vacuous one.

Pinned to the shipping commit rather than HEAD on purpose: HEAD will carry the
multi-stream filter as soon as this lands, and comparing it with itself would
pass forever without testing anything.
"""

import importlib.util
import subprocess
import sys
import types
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from mvp.atptour.bsr.constants import N_STREAMS, STREAM_INDEX, BsrConfig
from mvp.atptour.bsr.filter import BSR_VALUE_NAMES, BsrTracker

SHIPPED_REV = "26e4bbf"
REPO = Path(__file__).resolve().parents[3]


def _serve_outdoor_config() -> BsrConfig:
    """The default config with the `serve` stream's indoor axis switched off.

    Built here rather than changing the default: the axis is shipped ON, and
    the point of this file is to compare the two filters where they are the
    same model. Nothing else about the stream is touched.
    """
    from dataclasses import replace

    from mvp.atptour.bsr.constants import STREAMS

    return BsrConfig(
        streams=(replace(STREAMS[0], has_indoor=False),) + STREAMS[1:]
    )


def _git_show(rev: str, path: str) -> str:
    return subprocess.run(
        ["git", "-C", str(REPO), "show", f"{rev}:{path}"],
        capture_output=True, text=True, check=True,
    ).stdout


def _load_shipped():
    """The filter as of `SHIPPED_REV`, as a live module pair."""
    try:
        const_src = _git_show(SHIPPED_REV, "src/mvp/atptour/bsr/constants.py")
        filter_src = _git_show(SHIPPED_REV, "src/mvp/atptour/bsr/filter.py")
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        pytest.skip(f"cannot read {SHIPPED_REV} from git: {exc}")
    const_mod = types.ModuleType("bsr_shipped_constants")
    sys.modules["bsr_shipped_constants"] = const_mod
    exec(compile(const_src, "<bsr_shipped_constants>", "exec"), const_mod.__dict__)
    filter_mod = types.ModuleType("bsr_shipped_filter")
    # In sys.modules BEFORE exec: @dataclass resolves annotations through
    # sys.modules[cls.__module__], and a module that is not there yet makes it
    # raise inside the decorator rather than at import.
    sys.modules["bsr_shipped_filter"] = filter_mod
    exec(
        compile(
            filter_src.replace("mvp.atptour.bsr.constants", "bsr_shipped_constants"),
            "<bsr_shipped_filter>", "exec",
        ),
        filter_mod.__dict__,
    )
    return filter_mod


def _matches(n: int = 220, seed: int = 11, circuits=("tour", "chal", "itf")):
    """A match stream with the shape the pass sees: repeat opponents, gaps in
    the calendar, all four surfaces, both circuits, some rows without counts
    and some with one side only."""
    rng = np.random.default_rng(seed)
    players = [f"P{i}" for i in range(14)]
    surfaces = ["Hard", "Clay", "Grass", "Carpet", None]
    d = date(2015, 3, 2)
    for _ in range(n):
        a, b = (str(x) for x in rng.choice(players, 2, replace=False))
        surface = surfaces[int(rng.integers(0, 5))]
        circuit = circuits[int(rng.integers(0, len(circuits)))]
        indoor = bool(rng.random() < 0.25)
        n_a = int(rng.integers(30, 120))
        n_b = int(rng.integers(30, 120))
        y_a = int(rng.binomial(n_a, 0.64))
        y_b = int(rng.binomial(n_b, 0.60))
        r = rng.random()
        if r < 0.15:
            y_a = n_a = y_b = n_b = None
        elif r < 0.25:
            y_b = n_b = None
        elif r < 0.30:
            y_a = n_a = None
        d = d + timedelta(days=int(rng.integers(0, 40)))
        yield dict(
            a=a, b=b, surface=surface, circuit=circuit, indoor=indoor, d=d,
            y_a=y_a, n_a=n_a, y_b=y_b, n_b=n_b,
            elo_a=(1400.0 + 300.0 * rng.random(), 1400.0 + 300.0 * rng.random()),
            elo_b=(1400.0 + 300.0 * rng.random(), 1400.0 + 300.0 * rng.random()),
        )


def test_shipped_twelve_are_bit_identical():
    shipped = _load_shipped()
    old = shipped.BsrTracker(shipped.BsrConfig())
    new = BsrTracker(_serve_outdoor_config())
    new.begin(1)
    j = STREAM_INDEX["serve"]
    assert j == 0
    for m in _matches():
        old_cap = old.capture_match(
            m["a"], m["b"], m["surface"], m["circuit"], m["indoor"], m["d"],
            m["y_a"], m["n_a"], m["y_b"], m["n_b"],
            m["elo_a"][0], m["elo_a"][1], m["elo_b"][0], m["elo_b"][1],
        )
        k_p = [-1] * N_STREAMS
        n_p = [-1] * N_STREAMS
        k_o = [-1] * N_STREAMS
        n_o = [-1] * N_STREAMS
        k_p[j] = -1 if m["y_a"] is None else m["y_a"]
        n_p[j] = -1 if m["n_a"] is None else m["n_a"]
        k_o[j] = -1 if m["y_b"] is None else m["y_b"]
        n_o[j] = -1 if m["n_b"] is None else m["n_b"]
        new_cap = new.capture_match(
            m["a"], m["b"], m["surface"], m["circuit"], m["indoor"], m["d"],
            k_p, n_p, k_o, n_o,
            {"serve_elo": m["elo_a"][0], "return_elo": m["elo_a"][1]},
            {"serve_elo": m["elo_b"][0], "return_elo": m["elo_b"][1]},
        )
        for name in shipped.BSR_VALUE_NAMES:
            assert old_cap.player[name] == new_cap.player[name], (name, m["d"])
            assert old_cap.opp[name] == new_cap.opp[name], (name, m["d"])
        old.apply(old_cap)
        new.apply(new_cap)

    assert set(old._state) == set(new._state)
    for pid, os_ in old._state.items():
        ns = new._state[pid]
        assert os_.sm == ns.sm[j] and os_.sv == ns.sv[j], pid
        assert os_.rm == ns.rm[j] and os_.rv == ns.rv[j], pid
        assert os_.ssm == ns.ssm[j] and os_.ssv == ns.ssv[j], pid
        assert os_.rsm == ns.rsm[j] and os_.rsv == ns.rsv[j], pid
        assert os_.last_s == ns.last_s[j] and os_.last_r == ns.last_r[j], pid
        assert os_.last_ss == ns.last_ss[j] and os_.last_rs == ns.last_rs[j], pid
        assert os_.n_s == ns.n_s[j] and os_.n_r == ns.n_r[j], pid


def test_other_streams_do_not_reach_the_shipped_stream():
    """Feeding every other stream must not move the pooled stream by a bit.

    This is the case the multi-stream build could plausibly break: a player's
    state now comes into being at the first observation of ANY stream, and on
    the live aggregate 744 in-domain rows carry a tiebreak the pooled stream
    cannot see. If the pooled axis were frozen at whichever row created the
    state, its whole trajectory would move. Every match here feeds all 27
    streams while the pooled stream is missing on some rows, so a seed frozen
    early would show up immediately.

    In-domain circuits only: outside the domain the shipped filter emits null
    because it has no state at all, whereas this one emits the state another
    stream gave the player. That is a null becoming a value on ITF rows, never
    a value changing.
    """
    shipped = _load_shipped()
    old = shipped.BsrTracker(shipped.BsrConfig())
    new = BsrTracker(_serve_outdoor_config())
    j = 0
    for m in _matches(n=120, seed=5, circuits=("tour", "chal")):
        old_cap = old.capture_match(
            m["a"], m["b"], m["surface"], m["circuit"], m["indoor"], m["d"],
            m["y_a"], m["n_a"], m["y_b"], m["n_b"],
            m["elo_a"][0], m["elo_a"][1], m["elo_b"][0], m["elo_b"][1],
        )
        # Every stream fed, not just the pooled one.
        k_p = [3] * N_STREAMS
        n_p = [9] * N_STREAMS
        k_o = [6] * N_STREAMS
        n_o = [9] * N_STREAMS
        k_p[j] = -1 if m["y_a"] is None else m["y_a"]
        n_p[j] = -1 if m["n_a"] is None else m["n_a"]
        k_o[j] = -1 if m["y_b"] is None else m["y_b"]
        n_o[j] = -1 if m["n_b"] is None else m["n_b"]
        new_cap = new.capture_match(
            m["a"], m["b"], m["surface"], m["circuit"], m["indoor"], m["d"],
            k_p, n_p, k_o, n_o,
            {"serve_elo": m["elo_a"][0], "return_elo": m["elo_a"][1]},
            {"serve_elo": m["elo_b"][0], "return_elo": m["elo_b"][1]},
        )
        for name in shipped.BSR_VALUE_NAMES:
            assert old_cap.player[name] == new_cap.player[name], (name, m["d"])
            assert old_cap.opp[name] == new_cap.opp[name], (name, m["d"])
        old.apply(old_cap)
        new.apply(new_cap)
    for pid, os_ in old._state.items():
        ns = new._state[pid]
        assert os_.sm == ns.sm[j] and os_.sv == ns.sv[j], pid
        assert os_.rm == ns.rm[j] and os_.rv == ns.rv[j], pid


def test_default_config_matches_outdoors_and_only_diverges_indoors():
    """The `serve` indoor axis is the ONE thing the default config changes.

    The three tests above run with the axis off, which is what makes their
    "identical" claim meaningful. This one runs the DEFAULT config against the
    shipped filter and pins the divergence to exactly the rows it is supposed
    to touch: every outdoor row still agrees to the bit, and at least one
    indoor row does not.
    """
    shipped = _load_shipped()
    old = shipped.BsrTracker(shipped.BsrConfig())
    new = BsrTracker(BsrConfig())
    j = 0
    indoor_diffs = 0
    for m in _matches(n=160, seed=23, circuits=("tour", "chal")):
        old_cap = old.capture_match(
            m["a"], m["b"], m["surface"], m["circuit"], m["indoor"], m["d"],
            m["y_a"], m["n_a"], m["y_b"], m["n_b"],
            m["elo_a"][0], m["elo_a"][1], m["elo_b"][0], m["elo_b"][1],
        )
        k_p = [-1] * N_STREAMS
        n_p = [-1] * N_STREAMS
        k_o = [-1] * N_STREAMS
        n_o = [-1] * N_STREAMS
        k_p[j] = -1 if m["y_a"] is None else m["y_a"]
        n_p[j] = -1 if m["n_a"] is None else m["n_a"]
        k_o[j] = -1 if m["y_b"] is None else m["y_b"]
        n_o[j] = -1 if m["n_b"] is None else m["n_b"]
        new_cap = new.capture_match(
            m["a"], m["b"], m["surface"], m["circuit"], m["indoor"], m["d"],
            k_p, n_p, k_o, n_o,
            {"serve_elo": m["elo_a"][0], "return_elo": m["elo_a"][1]},
            {"serve_elo": m["elo_b"][0], "return_elo": m["elo_b"][1]},
        )
        differs = any(
            getattr(old_cap, side)[name] != getattr(new_cap, side)[name]
            for side in ("player", "opp")
            for name in shipped.BSR_VALUE_NAMES
        )
        if m["indoor"]:
            indoor_diffs += differs
        else:
            # An outdoor row can only differ if an EARLIER indoor row moved
            # the player's overall axes, which is the same divergence; what
            # must never happen is an outdoor row diverging before any indoor
            # row has been applied.
            assert not (differs and indoor_diffs == 0), m["d"]
        old.apply(old_cap)
        new.apply(new_cap)
    assert indoor_diffs > 0, "the serve indoor axis never moved anything"


def test_out_of_domain_is_the_only_divergence_and_it_is_null_to_value():
    """Names the one behavioural difference and holds it to null -> value.

    Outside the tuning domain the shipped filter emits null for a player it has
    never had state for. Here a player observed on any stream has state, so the
    pooled columns can be non-null on an ITF row where they used to be null.
    No non-null value may differ.
    """
    shipped = _load_shipped()
    old = shipped.BsrTracker(shipped.BsrConfig())
    new = BsrTracker(_serve_outdoor_config())
    elo_a = {"serve_elo": 1620.0, "return_elo": 1560.0}
    elo_b = {"serve_elo": 1480.0, "return_elo": 1510.0}
    tb = STREAM_INDEX["tb"]

    def _new_cap(circuit, d, feed_pooled, row=None):
        k_p = [-1] * N_STREAMS
        n_p = [-1] * N_STREAMS
        k_o = [-1] * N_STREAMS
        n_o = [-1] * N_STREAMS
        k_p[tb], n_p[tb] = 1, 2
        k_o[tb], n_o[tb] = 1, 2
        if feed_pooled:
            k_p[0], n_p[0] = 55, 90
            k_o[0], n_o[0] = 48, 90
        return new.capture_match(
            "A", "B", "Hard", circuit, False, d, k_p, n_p, k_o, n_o,
            elo_a, elo_b, row,
        )

    def _old_cap(circuit, d, feed_pooled):
        y, n = (55, 90) if feed_pooled else (None, None)
        yo, no = (48, 90) if feed_pooled else (None, None)
        return old.capture_match(
            "A", "B", "Hard", circuit, False, d, y, n, yo, no,
            elo_a["serve_elo"], elo_a["return_elo"],
            elo_b["serve_elo"], elo_b["return_elo"],
        )

    # 1. A tour match with a tiebreak and no serve counts: only the new filter
    #    takes anything from it, and only on the tb stream.
    d1 = date(2019, 5, 1)
    old.apply(_old_cap("tour", d1, False))
    new.apply(_new_cap("tour", d1, False))
    assert old._state == {}
    assert new._state["A"].n_s[tb] == 1
    assert new._state["A"].seeded[0] is False

    # 2. An ITF row: the shipped filter has no state and emits null. This one
    #    has a slot (the tiebreak stream created it) but the pooled stream has
    #    never committed, so out of domain its means are null too — a seed is
    #    not a posterior — while the tiebreak stream's own values are emitted.
    d2 = date(2019, 6, 1)
    o2 = _old_cap("itf", d2, True)
    n2 = _new_cap("itf", d2, True)
    assert all(v is None for v in o2.player.values())
    assert n2.player["bsr_serve_mu"] is None
    assert n2.player["bsr_serve_sd"] is not None  # the prior variance exists
    assert n2.player["bsr_pserve_logit"] is None  # still no fixed effect
    tb_mu = new.new_value_names.index("bsr_tb_mu")
    assert n2.player_new[tb_mu] == n2.player_new[tb_mu]  # not NaN

    # 3. Back in domain, every shipped value agrees to the bit — the pooled
    #    seed was NOT frozen by step 1.
    d3 = date(2019, 7, 1)
    o3 = _old_cap("tour", d3, True)
    n3 = _new_cap("tour", d3, True)
    for side in ("player", "opp"):
        for name in shipped.BSR_VALUE_NAMES:
            assert getattr(o3, side)[name] == getattr(n3, side)[name], (side, name)
    old.apply(o3)
    new.apply(n3)
    assert new._state["A"].seeded[0] is True
    assert old._state["A"].sm == new._state["A"].sm[0]

    # 4. And it stays identical afterwards.
    d4 = date(2019, 8, 15)
    o4 = _old_cap("chal", d4, True)
    n4 = _new_cap("chal", d4, True)
    for side in ("player", "opp"):
        for name in shipped.BSR_VALUE_NAMES:
            assert getattr(o4, side)[name] == getattr(n4, side)[name], (side, name)


def test_importlib_is_not_needed_but_the_module_loads():
    """Guards the loader itself: a silent import failure would make both
    tests above compare the new filter with the new filter."""
    shipped = _load_shipped()
    # The shipped twelve are a prefix of the current names; the four added
    # later (the pooled stream's indoor residuals) have no shipped counterpart.
    assert shipped.BSR_VALUE_NAMES == BSR_VALUE_NAMES[:len(shipped.BSR_VALUE_NAMES)]
    assert importlib.util is not None
    # The shipped state object is the pre-multi-stream one: scalar slots.
    st = shipped._PlayerState(0.0, 0.0, 0.1, 0.01)
    assert isinstance(st.sm, float) and isinstance(st.ssm, list)
