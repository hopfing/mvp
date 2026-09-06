"""Tests for the serve/return skill filter tracker."""

import math
from datetime import date

from mvp.atptour.bsr.constants import MU_CELLS, BsrConfig
from mvp.atptour.bsr.filter import BSR_VALUE_NAMES, BsrTracker

D0 = date(2020, 1, 10)


def _cap(tracker, a="A", b="B", y_p=50, n_p=80, y_o=40, n_o=80, surface="Hard",
         circuit="tour", indoor=False, d=D0, elo_a=(1600.0, 1550.0), elo_b=(1500.0, 1500.0)):
    return tracker.capture_match(
        a, b, surface, circuit, indoor, d, y_p, n_p, y_o, n_o,
        elo_a[0], elo_a[1], elo_b[0], elo_b[1],
    )


class TestSeedAndShape:
    def test_output_columns(self):
        cols = BsrTracker().output_columns()
        assert len(cols) == 24
        assert cols[0] == "player_bsr_serve_mu" and cols[1] == "opp_bsr_serve_mu"
        assert set(c.split("_", 1)[1] for c in cols) == set(BSR_VALUE_NAMES)

    def test_first_sight_emits_the_elo_seed(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        cap = _cap(t)
        exp_sm = cfg.seed_es * (1600.0 - 1500.0) / 100.0
        exp_rm = cfg.seed_er * (1550.0 - 1500.0) / 100.0
        assert cap.player["bsr_serve_mu"] == exp_sm
        assert cap.player["bsr_return_mu"] == exp_rm
        assert cap.player["bsr_serve_sd"] == math.sqrt(cfg.v0)
        assert cap.player["bsr_serve_surface_mu"] == 0.0
        assert cap.player["bsr_n_serve_obs"] == 0
        assert cap.player["bsr_days_since_serve_obs"] is None
        # cell = Hard(0)*4 + tour(0)*2 + outdoor(0)
        exp_eta = MU_CELLS[0] + exp_sm - 0.0
        assert math.isclose(cap.player["bsr_pserve_logit"], exp_eta)
        exp_sd = math.sqrt(cfg.v0 + cfg.q_surf + cfg.v0 + cfg.q_surf + cfg.tau2)
        assert math.isclose(cap.player["bsr_pserve_logit_sd"], exp_sd)

    def test_unseen_player_without_counts_emits_the_seed_in_domain(self):
        """A debutant's pending (count-less) row emits the same seed values
        its settled row will carry; nothing is stored until an observation."""
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        cap = _cap(t, y_p=None, n_p=None, y_o=None, n_o=None)
        assert cap.player["bsr_serve_mu"] == cfg.seed_es * (1600.0 - 1500.0) / 100.0
        assert cap.player["bsr_n_serve_obs"] == 0
        assert cap.player["bsr_pserve_logit"] is not None
        assert cap.pending is None
        t.apply(cap)  # no-op, no state created
        assert t._state == {}
        # the settled version of the same row emits identical values
        cap2 = _cap(t, y_p=50, n_p=80, y_o=40, n_o=80)
        assert cap2.player == cap.player and cap2.opp == cap.opp

    def test_unseen_player_out_of_domain_is_null(self):
        t = BsrTracker()
        cap = _cap(t, circuit="itf", y_p=None, n_p=None, y_o=None, n_o=None)
        assert all(v is None for v in cap.player.values())
        assert all(v is None for v in cap.opp.values())
        assert cap.pending is None

    def test_out_of_domain_creates_no_state(self):
        t = BsrTracker()
        assert _cap(t, circuit="itf").pending is None
        assert _cap(t, d=date(2014, 6, 1)).pending is None
        assert _cap(t, circuit=None).pending is None
        assert t._state == {}

    def test_invalid_counts_are_skipped(self):
        t = BsrTracker()
        assert _cap(t, y_p=50, n_p=0, y_o=None, n_o=None).pending is None
        assert _cap(t, y_p=90, n_p=80, y_o=None, n_o=None).pending is None
        assert _cap(t, y_p=None, n_p=80, y_o=None, n_o=None).pending is None
        cap = _cap(t, y_p=50, n_p=0, y_o=40, n_o=80)  # only the opponent's serve counts
        assert cap.pending is not None
        assert cap.pending[4] is None and cap.pending[5] is not None

    def test_carpet_and_null_surface_map_to_hard(self):
        t1, t2, t3 = BsrTracker(), BsrTracker(), BsrTracker()
        a = _cap(t1, surface="Hard").player["bsr_pserve_logit"]
        b = _cap(t2, surface="Carpet").player["bsr_pserve_logit"]
        c = _cap(t3, surface=None).player["bsr_pserve_logit"]
        assert a == b == c


class TestUpdate:
    def test_update_moves_toward_the_observation(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        cap = _cap(t, y_p=70, n_p=80, y_o=40, n_o=80)  # A serves far above expectation
        t.apply(cap)
        a, b = t._state["A"], t._state["B"]
        seed_a = cfg.seed_es * (1600.0 - 1500.0) / 100.0
        assert a.sm > seed_a
        assert a.sv < cfg.v0
        assert b.rm < 0.0  # B returned worse than expected
        assert a.n_s == 1 and b.n_r == 1 and b.n_s == 1 and a.n_r == 1
        assert a.last_s == D0 and a.last_ss[0] == D0

    def test_variance_share_split(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        cap = _cap(t, y_p=70, n_p=80, y_o=None, n_o=None)
        t.apply(cap)
        a, b = t._state["A"], t._state["B"]
        seed_a = cfg.seed_es * (1600.0 - 1500.0) / 100.0
        d_sm = a.sm - seed_a
        d_ssm = a.ssm[0] - 0.0
        # each component moves by (its prior var / vt) * delta
        assert math.isclose(d_sm / d_ssm, cfg.v0 / cfg.q_surf, rel_tol=1e-9)
        d_rm = 0.0 - b.rm
        assert math.isclose(d_rm, d_sm, rel_tol=1e-9)  # same prior var, opposite sign

    def test_only_touched_axes_change(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        t.apply(_cap(t, y_p=70, n_p=80, y_o=None, n_o=None))
        a, b = t._state["A"], t._state["B"]
        # B's serve axis and A's return axis were not observed
        assert b.sv == cfg.v0 and b.n_s == 0 and b.last_s is None
        assert a.rv == cfg.v0 and a.n_r == 0 and a.last_r is None
        assert b.ssm[0] == 0.0 and a.rsm[0] == 0.0

    def test_both_observations_equal_sequential(self):
        """Batched (one capture, two observations) equals two sequential
        single-observation matches at the same date — the disjoint-slot
        property the plan relies on."""
        t_batch = BsrTracker()
        t_batch.apply(_cap(t_batch, y_p=60, n_p=80, y_o=45, n_o=80))
        t_seq = BsrTracker()
        t_seq.apply(_cap(t_seq, y_p=60, n_p=80, y_o=None, n_o=None))
        t_seq.apply(_cap(t_seq, y_p=None, n_p=None, y_o=45, n_o=80))
        for pid in ("A", "B"):
            sa, sb = t_batch._state[pid], t_seq._state[pid]
            for slot in ("sm", "sv", "rm", "rv", "ssm", "ssv", "rsm", "rsv", "n_s", "n_r"):
                assert getattr(sa, slot) == getattr(sb, slot), (pid, slot)


class TestPredictiveState:
    def test_countless_row_emits_the_drifted_state(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        t.apply(_cap(t, y_p=60, n_p=80, y_o=45, n_o=80))
        sv_after = t._state["A"].sv
        d1 = date(2020, 4, 19)  # 100 days later
        cap_none = _cap(t, y_p=None, n_p=None, y_o=None, n_o=None, d=d1)
        sd = cap_none.player["bsr_serve_sd"]
        assert math.isclose(sd * sd, sv_after + cfg.q_s * 100, rel_tol=1e-12)
        assert cap_none.player["bsr_days_since_serve_obs"] == 100
        # surface residual took one AR step
        ssm_after = t._state["A"].ssm[0]
        assert math.isclose(cap_none.player["bsr_serve_surface_mu"], cfg.phi_surf * ssm_after)
        # stored state untouched by a count-less capture
        assert t._state["A"].sv == sv_after
        # the same date WITH counts emits the identical pre-match values
        cap_obs = _cap(t, y_p=55, n_p=80, y_o=40, n_o=80, d=d1)
        for k in BSR_VALUE_NAMES:
            assert cap_obs.player[k] == cap_none.player[k], k
        t.apply(cap_obs)
        assert t._state["A"].n_s == 2

    def test_cap_on_elapsed_days(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        t.apply(_cap(t, y_p=60, n_p=80, y_o=45, n_o=80))
        sv_after = t._state["A"].sv
        far = date(2025, 1, 1)
        cap = _cap(t, y_p=None, n_p=None, y_o=None, n_o=None, d=far)
        sd = cap.player["bsr_serve_sd"]
        assert math.isclose(sd * sd, sv_after + cfg.q_s * cfg.cap_days, rel_tol=1e-12)

    def test_both_rows_of_a_match_read_one_capture(self):
        t = BsrTracker()
        cap = _cap(t)
        assert set(cap.player) == set(BSR_VALUE_NAMES) == set(cap.opp)
        # The opponent's values are the same dict the driver will replay on
        # the second row, so nothing depends on row order.
        assert cap.opp["bsr_serve_mu"] == BsrConfig().seed_es * 0.0

    def test_deterministic(self):
        seq = [dict(y_p=60, n_p=80, y_o=45, n_o=80), dict(y_p=50, n_p=70, y_o=48, n_o=70, d=date(2020, 2, 1))]
        outs = []
        for _ in range(2):
            t = BsrTracker()
            vals = []
            for kw in seq:
                cap = _cap(t, **kw)
                vals.append((cap.player, cap.opp))
                t.apply(cap)
            outs.append(vals)
        assert outs[0] == outs[1]


class TestDatetimeInput:
    def test_datetime_match_date_equals_date(self):
        """The aggregate's effective_match_date is a datetime; the filter's
        clocks are day-grained and must not raise on the comparison."""
        from datetime import datetime

        t_date, t_dt = BsrTracker(), BsrTracker()
        t_date.apply(_cap(t_date, d=D0))
        t_dt.apply(_cap(t_dt, d=datetime(D0.year, D0.month, D0.day)))
        later = date(2020, 3, 1)
        a = _cap(t_date, y_p=None, n_p=None, y_o=None, n_o=None, d=later)
        b = _cap(t_dt, y_p=None, n_p=None, y_o=None, n_o=None, d=datetime(2020, 3, 1, 15, 30))
        assert a.player == b.player
        assert b.player["bsr_days_since_serve_obs"] == (later - D0).days


class TestOutOfDomainEmission:
    def test_state_emitted_but_matchup_null_outside_domain(self):
        t = BsrTracker()
        t.apply(_cap(t, y_p=60, n_p=80, y_o=45, n_o=80))
        cap = _cap(t, circuit="itf", y_p=55, n_p=80, y_o=40, n_o=80, d=date(2020, 2, 1))
        assert cap.pending is None  # no update outside the domain
        assert cap.player["bsr_serve_mu"] is not None  # state still emitted
        assert cap.player["bsr_n_serve_obs"] == 1
        assert cap.player["bsr_pserve_logit"] is None
        assert cap.player["bsr_pserve_logit_sd"] is None
