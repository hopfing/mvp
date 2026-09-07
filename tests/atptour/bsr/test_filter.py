"""Tests for the serve/return skill filter tracker."""

import math
from datetime import date

import numpy as np

from mvp.atptour.bsr.constants import (
    MU_CELLS,
    N_STREAMS,
    STREAM_INDEX,
    BsrConfig,
)
from mvp.atptour.bsr.filter import (
    BSR_NEW_VALUE_NAMES,
    BSR_VALUE_NAMES,
    BsrTracker,
)

D0 = date(2020, 1, 10)

# Every stream's seed source, defaulted to 1500 so a test that does not care
# about a component gets a zero seed from it.
_ELO_A = {
    "serve_elo": 1600.0, "return_elo": 1550.0,
    "first_serve_power": 1500.0, "second_serve_reliability": 1500.0,
    "ace_resistance": 1500.0, "serve_clutch": 1500.0,
    "return_clutch": 1500.0, "tb_clutch": 1500.0,
}
_ELO_B = dict(_ELO_A, serve_elo=1500.0, return_elo=1500.0)


def _counts(pairs: dict[str, tuple] | None) -> tuple[list[int], list[int]]:
    """(k, n) arrays over every stream, -1 everywhere `pairs` is silent."""
    k = [-1] * N_STREAMS
    n = [-1] * N_STREAMS
    for name, (kk, nn) in (pairs or {}).items():
        j = STREAM_INDEX[name]
        k[j] = -1 if kk is None else kk
        n[j] = -1 if nn is None else nn
    return k, n


def _cap(tracker, a="A", b="B", y_p=50, n_p=80, y_o=40, n_o=80, surface="Hard",
         circuit="tour", indoor=False, d=D0, elo_a=(1600.0, 1550.0),
         elo_b=(1500.0, 1500.0), extra_p=None, extra_o=None, row=None):
    """Capture one match. `y_*`/`n_*` feed the pooled `serve` stream, the way
    every shipped test did; `extra_p`/`extra_o` feed named streams as well."""
    p = {"serve": (y_p, n_p)}
    p.update(extra_p or {})
    o = {"serve": (y_o, n_o)}
    o.update(extra_o or {})
    kp, np_ = _counts(p)
    ko, no = _counts(o)
    ea = dict(_ELO_A, serve_elo=elo_a[0], return_elo=elo_a[1])
    eb = dict(_ELO_B, serve_elo=elo_b[0], return_elo=elo_b[1])
    return tracker.capture_match(
        a, b, surface, circuit, indoor, d, kp, np_, ko, no, ea, eb, row,
    )


class TestSeedAndShape:
    def test_output_columns(self):
        cols = BsrTracker().output_columns()
        assert len(cols) == 32  # 16 shipped-stream value names x 2 sides
        assert cols[0] == "player_bsr_serve_mu" and cols[1] == "opp_bsr_serve_mu"
        assert set(c.split("_", 1)[1] for c in cols) == set(BSR_VALUE_NAMES)

    def test_new_output_columns_are_two_sides_of_every_new_value(self):
        t = BsrTracker()
        cols = t.new_output_columns()
        assert len(cols) == 2 * len(BSR_NEW_VALUE_NAMES)
        assert cols[: len(BSR_NEW_VALUE_NAMES)] == [
            f"player_{n}" for n in BSR_NEW_VALUE_NAMES
        ]
        # No overlap with the shipped twelve: the two emission paths are
        # disjoint, so a name cannot be written by both.
        assert not set(cols) & set(t.output_columns())

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
        assert np.array_equal(cap2.player_new, cap.player_new, equal_nan=True)

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
        assert a.sm[0] > seed_a
        assert a.sv[0] < cfg.v0
        assert b.rm[0] < 0.0  # B returned worse than expected
        assert a.n_s[0] == 1 and b.n_r[0] == 1 and b.n_s[0] == 1 and a.n_r[0] == 1
        assert a.last_s[0] == D0 and a.last_ss[0][0] == D0

    def test_variance_share_split(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        cap = _cap(t, y_p=70, n_p=80, y_o=None, n_o=None)
        t.apply(cap)
        a, b = t._state["A"], t._state["B"]
        seed_a = cfg.seed_es * (1600.0 - 1500.0) / 100.0
        d_sm = a.sm[0] - seed_a
        d_ssm = a.ssm[0][0] - 0.0
        # each component moves by (its prior var / vt) * delta
        assert math.isclose(d_sm / d_ssm, cfg.v0 / cfg.q_surf, rel_tol=1e-9)
        d_rm = 0.0 - b.rm[0]
        assert math.isclose(d_rm, d_sm, rel_tol=1e-9)  # same prior var, opposite sign

    def test_only_touched_axes_change(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        t.apply(_cap(t, y_p=70, n_p=80, y_o=None, n_o=None))
        a, b = t._state["A"], t._state["B"]
        # B's serve axis and A's return axis were not observed
        assert b.sv[0] == cfg.v0 and b.n_s[0] == 0 and b.last_s[0] is None
        assert a.rv[0] == cfg.v0 and a.n_r[0] == 0 and a.last_r[0] is None
        assert b.ssm[0][0] == 0.0 and a.rsm[0][0] == 0.0

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
            for slot in ("sm", "sv", "rm", "rv", "ssm", "ssv", "rsm", "rsv",
                         "n_s", "n_r"):
                assert getattr(sa, slot) == getattr(sb, slot), (pid, slot)


class TestStreams:
    """The 20 streams the multi-stream build adds."""

    def test_every_stream_updates_its_own_axes_only(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        j = STREAM_INDEX["bp"]
        cap = _cap(t, y_p=None, n_p=None, y_o=None, n_o=None,
                   extra_p={"bp": (6, 6)})
        t.apply(cap)
        a, b = t._state["A"], t._state["B"]
        # bp moved, on both the server's and the returner's axis. Each stream
        # carries its OWN tuned v0, so the untouched prior is the stream's,
        # not the shipped pooled one.
        v0 = [s.v0 for s in cfg.streams]
        assert a.sm[j] > 0.0 and a.sv[j] < v0[j]
        assert b.rm[j] < 0.0 and b.rv[j] < v0[j]
        assert a.n_s[j] == 1 and b.n_r[j] == 1
        # nothing else did
        for other in range(N_STREAMS):
            if other == j:
                continue
            assert a.n_s[other] == 0 and b.n_r[other] == 0, other
            assert a.sv[other] == v0[other], other
            assert b.rv[other] == v0[other], other

    def test_zero_n_skips_that_stream_only(self):
        """No break points faced is not an observation of break-point skill,
        and must not stop the other streams observing this match."""
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        cap = _cap(t, y_p=50, n_p=80, extra_p={"bp": (0, 0), "ace": (7, 80)})
        t.apply(cap)
        a = t._state["A"]
        bp = STREAM_INDEX["bp"]
        assert a.n_s[bp] == 0
        assert a.sv[bp] == cfg.streams[bp].v0
        assert a.n_s[STREAM_INDEX["ace"]] == 1
        assert a.n_s[STREAM_INDEX["serve"]] == 1

    def test_missing_marker_and_out_of_range_k_are_skipped(self):
        t = BsrTracker()
        cap = _cap(t, y_p=None, n_p=None, y_o=None, n_o=None,
                   extra_p={"ace": (None, None), "df": (90, 80)})
        assert cap.pending is None

    def test_a_stream_without_a_returner_leaves_the_returner_untouched(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        j = STREAM_INDEX["fsi"]
        assert not t.streams[j].has_returner
        t.apply(_cap(t, y_p=None, n_p=None, y_o=None, n_o=None,
                     extra_p={"fsi": (50, 80)}))
        a, b = t._state["A"], t._state["B"]
        v0 = cfg.streams[j].v0
        assert a.n_s[j] == 1 and a.sv[j] < v0
        assert b.n_r[j] == 0 and b.rv[j] == v0 and b.rm[j] == 0.0

    def test_auxiliary_streams_have_no_surface_axes(self):
        t = BsrTracker()
        surface_streams = {
            s.name for s in t.streams if s.has_surface
        }
        assert surface_streams == {"serve", "fsi", "w1", "w2", "hold"}
        st = t._state.get("A")
        t.apply(_cap(t, extra_p={"ace": (7, 80)}))
        st = t._state["A"]
        assert st.ssm[STREAM_INDEX["ace"]] is None
        assert st.ssm[STREAM_INDEX["serve"]] is not None

    def test_indoor_axis_only_moves_on_indoor_matches(self):
        cfg = BsrConfig()
        j = STREAM_INDEX["w1"]
        assert BsrTracker().streams[j].has_indoor
        out_t = BsrTracker(cfg)
        out_t.apply(_cap(out_t, indoor=False, extra_p={"w1": (40, 50)}))
        assert out_t._state["A"].ism[j] == 0.0
        assert out_t._state["A"].isv[j] == cfg.streams[j].q_indoor
        assert out_t._state["A"].last_is[j] is None
        in_t = BsrTracker(cfg)
        in_t.apply(_cap(in_t, indoor=True, extra_p={"w1": (40, 50)}))
        assert in_t._state["A"].ism[j] != 0.0
        assert in_t._state["A"].last_is[j] == D0
        # The shipped stream now carries one too, and it moves on the same
        # indoor row — that is what makes the twelve shipped columns differ
        # from the pre-multi-stream filter on indoor rows and only there.
        s = STREAM_INDEX["serve"]
        assert BsrTracker().streams[s].has_indoor
        assert in_t._state["A"].ism[s] != 0.0
        assert out_t._state["A"].ism[s] == 0.0

    def test_orientation_is_favourable_for_every_seeded_stream(self):
        """k is the server- or player-favourable event wherever an Elo seed
        signs the prior, so a high seed means a high rate. The two streams
        whose k is unfavourable carry no seed."""
        t = BsrTracker()
        for st in t.streams:
            if st.name in ("hardhold", "deuce"):
                assert st.seed_src_s is None and st.seed_src_r is None, st.name
        df = t.streams[STREAM_INDEX["df"]]
        assert df.k_terms == (
            (1, "svc_second_serve_pts_played"), (-1, "svc_double_faults"),
        )
        hold = t.streams[STREAM_INDEX["hold"]]
        assert hold.k_terms == (
            (1, "svc_games_played"), (-1, "svc_bp_faced"), (1, "svc_bp_saved"),
        )

    def test_df_orientation_moves_up_when_no_double_faults(self):
        t = BsrTracker()
        j = STREAM_INDEX["df"]
        t.apply(_cap(t, y_p=None, n_p=None, y_o=None, n_o=None,
                     extra_p={"df": (30, 30)}))
        assert t._state["A"].sm[j] > 0.0
        t2 = BsrTracker()
        t2.apply(_cap(t2, y_p=None, n_p=None, y_o=None, n_o=None,
                      extra_p={"df": (0, 30)}))
        assert t2._state["A"].sm[j] < 0.0


class TestNameVectorLockstep:
    """`new_value_names` and `_new_values` are two hand-written branch
    sequences over the same flags. If they ever disagree the slab silently
    misaligns — every column after the offending stream reads a neighbour's
    value — so hold them together under flag combinations the shipped table
    does not contain."""

    @staticmethod
    def _cfg(**flips):
        from dataclasses import replace

        from mvp.atptour.bsr.constants import STREAMS

        streams = tuple(
            replace(s, **flips.get(s.name, {})) for s in STREAMS
        )
        return BsrConfig(streams=streams)

    def test_vector_length_matches_name_count_for_every_flag_mix(self):
        from mvp.atptour.bsr.filter import new_value_names

        variants = [
            {},
            {"ace": {"has_surface": True, "has_indoor": True}},
            {"serve": {"has_indoor": False}},
            {"bp": {"has_returner": False}},
            {"fsi": {"has_returner": True}},
            {"bh": {"has_surface": True}},
            # returner + one residual axis but not the other, neither of which
            # the shipped table contains
            {"ace": {"has_indoor": True}},
            {"bp": {"has_surface": True}},
        ]
        for flips in variants:
            cfg = self._cfg(**flips)
            t = BsrTracker(cfg)
            names = new_value_names(cfg.streams)
            assert t.new_value_names == names
            cap = _cap(t)
            assert len(cap.player_new) == len(names), flips
            assert len(cap.opp_new) == len(names), flips
            t.apply(cap)
            cap2 = _cap(t, d=date(2020, 3, 1))
            assert len(cap2.player_new) == len(names), flips

    def test_a_flag_flip_changes_both_sides_together(self):
        from mvp.atptour.bsr.filter import new_value_names

        base = BsrConfig()
        # ace carries a returner, so a surface axis is two MEANS, the
        # server's and the returner's (its spreads are not emitted: only the
        # pooled stream's residual spreads are, see StreamConfig).
        flipped = self._cfg(ace={"has_surface": True})
        assert (
            len(new_value_names(flipped.streams))
            == len(new_value_names(base.streams)) + 2
        )
        t = BsrTracker(flipped)
        assert len(_cap(t).player_new) == len(new_value_names(flipped.streams))
        # a stream with no returner gains only the server's mean (its spread
        # is not emitted off the pooled stream)
        no_ret = self._cfg(df={"has_indoor": True})
        assert (
            len(new_value_names(no_ret.streams))
            == len(new_value_names(base.streams)) + 1
        )


class TestPerStreamEmission:
    """The three groups the emission sequence added: the days-since clock and
    the returner's surface and indoor residuals."""

    @staticmethod
    def _val(cap, name, side="player"):
        names = list(BSR_NEW_VALUE_NAMES)
        vec = cap.player_new if side == "player" else cap.opp_new
        return vec[names.index(name)]

    def test_days_since_is_per_stream_and_nan_before_the_first_obs(self):
        # Clocks are emitted on one stream per feed (bp keeps its own; ace's
        # is a copy of the pooled stream's and is not emitted).
        t = BsrTracker()
        cap0 = _cap(t, extra_p={"bp": (3, 5)})
        # nothing observed yet on either stream
        assert math.isnan(self._val(cap0, "bsr_bp_days_since"))
        assert math.isnan(self._val(cap0, "bsr_tb_days_since"))
        t.apply(cap0)
        d1 = date(2020, 4, 19)  # 100 days later
        cap1 = _cap(t, y_p=None, n_p=None, y_o=None, n_o=None, d=d1)
        assert self._val(cap1, "bsr_bp_days_since") == 100
        # tb never observed, so its clock is still null
        assert math.isnan(self._val(cap1, "bsr_tb_days_since"))
        # and it is the SERVER-side clock, as the shipped column is: B
        # returned that break-point observation but never served one
        assert math.isnan(self._val(cap1, "bsr_bp_days_since", side="opp"))

    def test_returner_surface_and_indoor_residuals_are_emitted(self):
        """`bsr_w1_r_surface_mu` is the player's own return-side residual for
        this match's surface, the way `bsr_return_surface_mu` is."""
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        j = STREAM_INDEX["w1"]
        # A serves, so B's return axes move on w1.
        t.apply(_cap(t, y_p=None, n_p=None, y_o=None, n_o=None,
                     indoor=True, extra_p={"w1": (45, 50)}))
        b = t._state["B"]
        assert b.rsm[j][0] != 0.0 and b.irm[j] != 0.0
        cap = _cap(t, a="B", b="A", y_p=None, n_p=None, y_o=None, n_o=None,
                   indoor=True, d=date(2020, 2, 1))
        # emitted on B's own row, for this match's surface (Hard = index 0)
        assert math.isclose(
            self._val(cap, "bsr_w1_r_surface_mu"), cfg.phi_surf * b.rsm[j][0]
        )
        assert math.isclose(
            self._val(cap, "bsr_w1_r_indoor_mu"), cfg.phi_surf * b.irm[j]
        )
        # a different surface reads that surface's residual, still zero
        clay = _cap(t, a="B", b="A", y_p=None, n_p=None, y_o=None, n_o=None,
                    surface="Clay", d=date(2020, 2, 1))
        assert self._val(clay, "bsr_w1_r_surface_mu") == 0.0

    def test_streams_without_a_returner_emit_no_returner_residuals(self):
        assert "bsr_fsi_r_surface_mu" not in BSR_NEW_VALUE_NAMES
        assert "bsr_fsi_r_indoor_mu" not in BSR_NEW_VALUE_NAMES
        # and a returner stream with no surface axis emits no surface pair
        assert "bsr_ace_r_surface_mu" not in BSR_NEW_VALUE_NAMES
        assert "bsr_ace_r_mu" in BSR_NEW_VALUE_NAMES


class TestArrayOutput:
    def test_vector_carries_nan_where_the_dict_carries_none(self):
        t = BsrTracker()
        t.begin(1)
        cap = _cap(t, circuit="itf", y_p=None, n_p=None, y_o=None, n_o=None,
                   row=0)
        assert all(v is None for v in cap.player.values())
        assert all(math.isnan(v) for v in cap.player_new)
        s = t.new_series()
        assert all(x.null_count() == 1 for x in s)

    def test_out_of_domain_logit_is_nan_while_state_is_not(self):
        t = BsrTracker()
        t.begin(2)
        t.apply(_cap(t, row=0, extra_p={"ace": (7, 80)}))
        cap = _cap(t, circuit="itf", d=date(2020, 2, 1), row=1,
                   y_p=None, n_p=None, y_o=None, n_o=None)
        names = list(BSR_NEW_VALUE_NAMES)
        assert not math.isnan(cap.player_new[names.index("bsr_ace_mu")])
        assert math.isnan(cap.player_new[names.index("bsr_ace_logit")])
        assert "bsr_ace_n_obs" not in names  # ace's count copies the pooled one

    def test_second_row_replay_swaps_the_two_sides(self):
        t = BsrTracker()
        t.begin(2)
        cap = _cap(t, y_p=70, n_p=80, y_o=40, n_o=80, row=0)
        t.replay_row(1, cap, "B")
        h = len(BSR_NEW_VALUE_NAMES)
        out = t._out
        np.testing.assert_array_equal(out[:h, 1], out[h:, 0])
        np.testing.assert_array_equal(out[h:, 1], out[:h, 0])

    def test_scatter_series_places_rows_and_nulls_the_rest(self):
        t = BsrTracker()
        t.begin(2)
        _cap(t, row=0, extra_p={"ace": (7, 80)})
        _cap(t, a="C", b="D", row=1)
        idx = np.array([1, 3], dtype=np.uint32)
        series = t.scatter_series(idx, 5)
        assert t._out is None
        by_name = {s.name: s for s in series}
        col = by_name["player_bsr_ace_mu"]
        assert len(col) == 5
        assert col.null_count() == 3
        assert col[0] is None and col[1] is not None and col[3] is not None


class TestPredictiveState:
    def test_countless_row_emits_the_drifted_state(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        t.apply(_cap(t, y_p=60, n_p=80, y_o=45, n_o=80))
        sv_after = t._state["A"].sv[0]
        d1 = date(2020, 4, 19)  # 100 days later
        cap_none = _cap(t, y_p=None, n_p=None, y_o=None, n_o=None, d=d1)
        sd = cap_none.player["bsr_serve_sd"]
        assert math.isclose(sd * sd, sv_after + cfg.q_s * 100, rel_tol=1e-12)
        assert cap_none.player["bsr_days_since_serve_obs"] == 100
        # surface residual took one AR step
        ssm_after = t._state["A"].ssm[0][0]
        assert math.isclose(
            cap_none.player["bsr_serve_surface_mu"], cfg.phi_surf * ssm_after
        )
        # stored state untouched by a count-less capture
        assert t._state["A"].sv[0] == sv_after
        # the same date WITH counts emits the identical pre-match values
        cap_obs = _cap(t, y_p=55, n_p=80, y_o=40, n_o=80, d=d1)
        for k in BSR_VALUE_NAMES:
            assert cap_obs.player[k] == cap_none.player[k], k
        assert np.array_equal(cap_obs.player_new, cap_none.player_new, equal_nan=True)
        t.apply(cap_obs)
        assert t._state["A"].n_s[0] == 2

    def test_cap_on_elapsed_days(self):
        cfg = BsrConfig()
        t = BsrTracker(cfg)
        t.apply(_cap(t, y_p=60, n_p=80, y_o=45, n_o=80))
        sv_after = t._state["A"].sv[0]
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
        seq = [dict(y_p=60, n_p=80, y_o=45, n_o=80),
               dict(y_p=50, n_p=70, y_o=48, n_o=70, d=date(2020, 2, 1))]
        outs = []
        for _ in range(2):
            t = BsrTracker()
            vals = []
            for kw in seq:
                cap = _cap(t, **kw)
                vals.append((cap.player, cap.opp, [None if v != v else v for v in cap.player_new]))
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
        b = _cap(t_dt, y_p=None, n_p=None, y_o=None, n_o=None,
                 d=datetime(2020, 3, 1, 15, 30))
        assert a.player == b.player
        assert b.player["bsr_days_since_serve_obs"] == (later - D0).days


class TestInputColumns:
    def test_every_stream_names_columns_the_aggregate_carries(self):
        """A stream whose count column does not exist would be fed -1 on every
        row and sit at its seed for the whole pass. The ratings pass refuses
        the tracker in that case, which turns it into a broken live tick
        rather than a broken column — so check the names here instead."""
        from pathlib import Path

        import polars as pl
        import pytest

        from mvp.atptour.bsr.constants import bsr_input_columns

        agg = Path("B:/aggregate/atptour/matches.parquet")
        if not agg.exists():
            pytest.skip("aggregate not mounted")
        have = set(pl.scan_parquet(agg).collect_schema().names())
        missing = sorted(c for c in bsr_input_columns() if c not in have)
        assert missing == []

    def test_mirrors_cover_the_three_naming_conventions(self):
        from mvp.atptour.bsr.constants import mirror_col

        assert mirror_col("svc_aces") == "opp_svc_aces"
        assert mirror_col("pts_service_pts_won") == "opp_pts_service_pts_won"
        assert mirror_col("mb_player_easy_holds") == "mb_opp_easy_holds"
        assert mirror_col("player_sp_winners") == "opp_sp_winners"
        assert mirror_col("player_fh_winners") == "opp_fh_winners"


class TestOutOfDomainEmission:
    def test_state_emitted_but_matchup_null_outside_domain(self):
        t = BsrTracker()
        t.apply(_cap(t, y_p=60, n_p=80, y_o=45, n_o=80))
        cap = _cap(t, circuit="itf", y_p=55, n_p=80, y_o=40, n_o=80,
                   d=date(2020, 2, 1))
        assert cap.pending is None  # no update outside the domain
        assert cap.player["bsr_serve_mu"] is not None  # state still emitted
        assert cap.player["bsr_n_serve_obs"] == 1
        assert cap.player["bsr_pserve_logit"] is None
        assert cap.player["bsr_pserve_logit_sd"] is None
