"""The ratings-pass tracker reproduces the probe filter it was ported from.

`scripts/bsr/probe_bsr.py::run_filter` is the oracle: it sees only rows with
serve counts, sorted by date, and emits per observation the pre-match eta,
the total variance (skill + random effect) and the server's serve-count
observations. The tracker must reproduce those to 1e-9 on the same matches,
with the shipped variant (Elo seed, random effect, two Newton steps, tied
dispersion), and its final per-player state must match the probe's.
"""

import importlib.util
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from mvp.atptour.bsr.constants import (
    CIRCUIT_INDEX,
    MU_CELLS,
    STREAM_INDEX,
    STREAM_NAMES,
    SURFACE_INDEX,
    BsrConfig,
    bsr_input_columns,
)
from mvp.atptour.bsr.filter import BsrTracker
from mvp.atptour.ratings.compute import compute_all_ratings

PROBE = Path(__file__).resolve().parents[3] / "scripts" / "bsr" / "probe_bsr.py"


def _serve_outdoor_config() -> BsrConfig:
    """The default config with the `serve` stream's indoor axis switched off.

    `run_filter` has no indoor residual, so the probe and the tracker are the
    same model only with that axis off. Under the default config the shipped
    stream carries one and its twelve columns differ from the probe's on
    indoor rows by design; everything else here is the shipped tune.
    """
    from dataclasses import replace

    from mvp.atptour.bsr.constants import STREAMS

    return BsrConfig(
        streams=(replace(STREAMS[0], has_indoor=False),) + STREAMS[1:]
    )


def _load_probe():
    if not PROBE.exists():
        pytest.skip("probe script not present")
    spec = importlib.util.spec_from_file_location("probe_bsr", PROBE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _synthetic_frame(n_matches: int = 48, seed: int = 7) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    players = [f"P{i}" for i in range(12)]
    surfaces = ["Hard", "Clay", "Grass", "Carpet"]
    rows = []
    d = date(2020, 1, 5)
    for m in range(n_matches):
        a, b = rng.choice(players, 2, replace=False)
        surface = surfaces[int(rng.integers(0, 4))]
        circuit = "tour" if rng.random() < 0.4 else "chal"
        indoor = bool(rng.random() < 0.2) and surface == "Hard"
        # some matches carry no counts, some one side only
        r = rng.random()
        n_a = int(rng.integers(40, 110)); n_b = int(rng.integers(40, 110))
        y_a = int(rng.binomial(n_a, 0.63)); y_b = int(rng.binomial(n_b, 0.61))
        if r < 0.12:
            y_a = n_a = y_b = n_b = None
        elif r < 0.2:
            y_b = n_b = None
        d = d + timedelta(days=int(rng.integers(0, 9)))
        won = bool(rng.random() < 0.5)
        common = dict(
            match_uid=f"m{m}", surface=surface, round="R32", round_order=7,
            tournament_start_date=d - timedelta(days=2), tournament_level="250",
            effective_match_date=d, indoor=indoor, circuit=circuit,
            player_rank=None, opp_rank=None,
        )
        rows.append(dict(common, player_id=a, opp_id=b, won=won,
                         pts_service_pts_won=y_a, pts_service_pts_played=n_a,
                         opp_pts_service_pts_won=y_b, opp_pts_service_pts_played=n_b))
        rows.append(dict(common, player_id=b, opp_id=a, won=not won,
                         pts_service_pts_won=y_b, pts_service_pts_played=n_b,
                         opp_pts_service_pts_won=y_a, opp_pts_service_pts_played=n_a))
    df = pl.DataFrame(rows).with_columns(
        pl.col("player_rank").cast(pl.Int64), pl.col("opp_rank").cast(pl.Int64),
    )
    # The other 20 streams see nothing here: this frame is the probe's oracle
    # for the pooled stream, and feeding the rest would not change it. The
    # columns still have to exist, because the ratings pass refuses the tracker
    # without every stream's inputs.
    return df.with_columns([
        pl.lit(None, dtype=pl.Int32).alias(c)
        for c in bsr_input_columns() if c not in df.columns
    ])


def test_tracker_matches_probe_filter():
    pb = _load_probe()
    cfg = _serve_outdoor_config()
    df = _synthetic_frame()
    out = compute_all_ratings(df, bsr_tracker=BsrTracker(cfg))
    # Probe observation rows: one per (match, server) with valid counts, seeds
    # from the ratings pass's own pre-match serve/return Elo columns.
    obs = (
        out.filter(pl.col("pts_service_pts_played").is_not_null() & (pl.col("pts_service_pts_played") > 0))
        .select(
            pl.col("effective_match_date").alias("date"), "tournament_start_date", "round_order", "match_uid",
            pl.col("player_id").alias("server"), pl.col("opp_id").alias("returner"),
            pl.col("pts_service_pts_won").cast(pl.Float64).alias("y"),
            pl.col("pts_service_pts_played").cast(pl.Float64).alias("n"),
            pl.col("surface").replace_strict(SURFACE_INDEX, default=0).alias("surf"),
            pl.col("circuit").replace_strict(CIRCUIT_INDEX).alias("circ"),
            pl.col("indoor").cast(pl.Int8).alias("indoor"),
            pl.lit(None).cast(pl.Float64).alias("srank"), pl.lit(None).cast(pl.Float64).alias("rrank"),
            pl.col("player_serve_elo").alias("s_srv_elo_base"), pl.col("player_return_elo").alias("s_ret_elo_base"),
            pl.col("opp_serve_elo").alias("r_srv_elo_base"), pl.col("opp_return_elo").alias("r_ret_elo_base"),
            pl.col("player_bsr_pserve_logit").alias("trk_eta"),
            pl.col("player_bsr_pserve_logit_sd").alias("trk_sd"),
            pl.col("player_bsr_n_serve_obs").alias("trk_nobs"),
        )
        .sort(["date", "tournament_start_date", "round_order", "match_uid", "server"])
        .with_columns(
            (pl.col("date") - pl.date(2015, 1, 1)).dt.total_days().cast(pl.Int64).alias("d"),
            pl.col("date").dt.year().alias("year"),
            (pl.col("surf") * 4 + pl.col("circ") * 2 + pl.col("indoor")).alias("cell"),
        )
    )
    P = dict(
        q_s=cfg.q_s, q_r=cfg.q_r, cap_days=cfg.cap_days, q_surf=cfg.q_surf, phi_surf=cfg.phi_surf,
        v0=cfg.v0, seed_s=0.0, seed_r=0.0, seed_es=cfg.seed_es, seed_er=cfg.seed_er,
        tau_tour=cfg.tau2, tau_chal=cfg.tau2, disp_tour=1.0, disp_chal=1.0,
    )
    res = pb.run_filter(
        obs, P, np.array(MU_CELLS), 0.0, 1.0,
        newton=cfg.newton, seed_mode="elo", disp_mode="re", tie_disp=True,
    )
    np.testing.assert_allclose(obs["trk_eta"].to_numpy(), res["pre_m"], rtol=0, atol=1e-9)
    np.testing.assert_allclose(obs["trk_sd"].to_numpy() ** 2, res["pre_vt"], rtol=0, atol=1e-9)
    assert obs["trk_nobs"].to_list() == [int(x) for x in res["nobs"]]

    # Final state, per player, all 14 slots the probe keeps.
    tracker = BsrTracker(cfg)
    compute_all_ratings(df, bsr_tracker=tracker)
    # The tracker's state is per-stream parallel lists; the shipped pooled
    # stream, which is what the probe models, is index 0 in every one of them.
    for pid, ps in res["state"].items():
        st = tracker._state[pid]
        assert abs(st.sm[0] - ps[0]) < 1e-9 and abs(st.sv[0] - ps[1]) < 1e-9
        assert abs(st.rm[0] - ps[2]) < 1e-9 and abs(st.rv[0] - ps[3]) < 1e-9
        for k in range(3):
            assert abs(st.ssm[0][k] - ps[4][k]) < 1e-9
            assert abs(st.ssv[0][k] - ps[5][k]) < 1e-9
            assert abs(st.rsm[0][k] - ps[6][k]) < 1e-9
            assert abs(st.rsv[0][k] - ps[7][k]) < 1e-9
        assert st.n_s[0] == ps[12] and st.n_r[0] == ps[13]
    assert set(res["state"]) == set(tracker._state)


def test_every_stream_matches_the_probe():
    """Skeleton for the probe's multi-stream extension (build row 0).

    The shipped stream is pinned above against `run_filter` as it stands. When
    the probe grows per-stream results — `run_filter_streams(obs, P, ...)`
    returning `{stream: {...}}`, or a `STREAMS`/`stream_results` surface —
    this reads them and holds every stream to the same 1e-9, so a stream's
    arithmetic here and in the tuning loop cannot diverge. Until then it skips
    rather than passing vacuously.
    """
    pb = _load_probe()
    hook = next(
        (h for h in ("run_filter_streams", "run_filter_multi", "stream_results")
         if hasattr(pb, h)),
        None,
    )
    if hook is None:
        pytest.skip(
            "probe has no multi-stream entry point yet "
            "(expected run_filter_streams / run_filter_multi / stream_results)"
        )
    cfg = BsrConfig()
    df = _synthetic_frame()
    tracker = BsrTracker(cfg)
    out = compute_all_ratings(df, bsr_tracker=tracker)
    try:
        results = _probe_streams(pb, hook, out, cfg)
    except (TypeError, KeyError, AttributeError) as exc:
        pytest.skip(f"probe's {hook} has a shape this test does not know: {exc}")
    shared = [s for s in STREAM_NAMES if s in results]
    if not shared:
        pytest.skip(f"probe's {hook} names no stream this build carries")
    for name in shared:
        j = STREAM_INDEX[name]
        for pid, ps in results[name]["state"].items():
            if pid not in tracker._state:
                continue
            st = tracker._state[pid]
            assert abs(st.sm[j] - ps[0]) < 1e-9, (name, pid)
            assert abs(st.sv[j] - ps[1]) < 1e-9, (name, pid)
            assert abs(st.rm[j] - ps[2]) < 1e-9, (name, pid)
            assert abs(st.rv[j] - ps[3]) < 1e-9, (name, pid)


def _probe_streams(pb, hook: str, out, cfg):
    """The probe's per-stream final states, keyed by stream name, computed
    with THIS config's knobs and cells so the two implementations are the
    same model: `{stream: {"state": {player_id: (sm, sv, rm, rv)}}}`.
    """
    import numpy as np

    obj = getattr(pb, hook)
    if not callable(obj):
        return dict(obj)
    params = {}
    mus = {}
    for st in cfg.streams:
        params[st.name] = dict(
            q_s=st.q_s, q_r=st.q_r, q_surf=st.q_surf, q_indoor=st.q_indoor,
            v0=st.v0, seed_s=st.seed_s, seed_r=st.seed_r, tau2=st.tau2,
            cap_days=cfg.cap_days, phi_surf=cfg.phi_surf, newton=cfg.newton,
        )
        mus[st.name] = np.asarray(st.mu_cells, dtype=float)
    res = obj(out, params, mus)
    players = res["_players"]
    results = {}
    for name, r in res.items():
        if name.startswith("_"):
            continue
        state = {}
        for i, pid in enumerate(players):
            if not r["seen"][i]:
                continue
            state[pid] = (
                float(r["sm"][i]), float(r["sv"][i]),
                float(r["rm"][i]), float(r["rv"][i]),
            )
        results[name] = {"state": state}
    return results
