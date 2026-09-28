"""Coherent surface-indoor Elo (elo/composite.py + the compute_all_ratings seam).

Plan: mvp-docs/plans/2026-09-28-surface-indoor-mov-elo.md, with its revisions."""

from datetime import date, timedelta

import polars as pl
import pytest

from mvp.atptour.elo.composite import COMPOSITE_AXES, CompositeEloTracker
from mvp.atptour.elo.constants import (
    DEFAULT_ELO,
    DEFAULT_RD,
    REVERSION_RATE,
    SURFACE_K_MULT,
)
from mvp.atptour.elo.mov import MELO_K_SCALE, MovTracker, games_share
from mvp.atptour.elo.ratings import (
    apply_inactivity_rd,
    expected_score,
    k_factor_from,
    update_rd,
)
from mvp.atptour.ratings.compute import ALL_RATING_COLUMNS, compute_all_ratings

D0 = date(2024, 1, 1)


def _fresh_pair(variants=("celo_si", "melo_si")) -> CompositeEloTracker:
    t = CompositeEloTracker(variants)
    t.ensure_player("A", 1500.0)
    t.ensure_player("B", 1500.0)
    return t


def _update(t, won=True, surface="Hard", indoor=False, pg=13, og=11,
            valid=True, result_type=None, when=D0):
    t.update_match(
        "A", "B", won, "R32", "250", surface, indoor, pg, og, valid,
        result_type, when,
    )


def _st(t, pid="A", v="celo_si"):
    return t._state[v][pid]


class TestUpdateMath:
    def test_one_expectation_on_indoor_hard(self):
        """The surprise comes from ONE expectation off the full effective
        rating (base + hard + indoor), not the champion's E = 0.5 off
        base + surface."""
        t = _fresh_pair(("celo_si",))
        _st(t).axes["indoor"].adj = 100.0
        _update(t, surface="Hard", indoor=True)
        k = k_factor_from(DEFAULT_RD, 0, "R32", "250")
        assert k == pytest.approx(57.6)
        assert expected_score(1600.0, 1500.0) == pytest.approx(0.6401, abs=1e-4)
        a = _st(t)
        assert a.rating - 1500.0 == pytest.approx(20.629, abs=1e-3)
        assert a.axes["hard"].adj == pytest.approx(6.189, abs=1e-3)
        assert a.axes["indoor"].adj == pytest.approx(105.689, abs=1e-3)

    def test_axes_engaged(self):
        cases = {
            ("Clay", False): {"clay"},
            ("Hard", True): {"hard", "indoor"},
            ("Clay", True): {"clay"},
            ("Grass", False): {"grass"},
            ("Carpet", True): set(),
            ("Carpet", False): set(),
        }
        for (surface, indoor), engaged in cases.items():
            t = _fresh_pair()
            _update(t, surface=surface, indoor=indoor)
            for v in t.variants:
                for pid in ("A", "B"):
                    moved = {
                        a for a in COMPOSITE_AXES
                        if _st(t, pid, v).axes[a].match_count == 1
                    }
                    assert moved == engaged, (surface, indoor, v, pid)

    def test_gated_reversion(self):
        t = _fresh_pair(("celo_si",))
        g = _st(t).axes["grass"]
        g.adj, g.rd = 50.0, 200.0
        _update(t, surface="Hard")
        assert (g.adj, g.rd, g.match_count) == (50.0, 200.0, 0)

        a, b = _st(t), _st(t, "B")
        eff_a, eff_b = a.rating + 50.0, b.rating
        k = k_factor_from(a.rd, a.match_count, "R32", "250")
        _update(t, surface="Grass")
        raw = 50.0 + k * SURFACE_K_MULT * (1.0 - expected_score(eff_a, eff_b))
        expected = raw * (1.0 - REVERSION_RATE * 200.0 / DEFAULT_RD)
        assert g.adj == pytest.approx(expected)
        assert g.rd == update_rd(200.0)
        assert g.match_count == 1

    def test_axis_clock(self):
        """An axis's rd grows from its own last date, not the base's."""
        t = _fresh_pair(("celo_si",))
        _update(t, surface="Grass", when=D0)
        grass_rd = _st(t).axes["grass"].rd = 100.0  # off the MAX_RD cap
        # A hard match 30 days later keeps the base clock current, not grass's.
        d1 = D0 + timedelta(days=30)
        t.apply_inactivity("A", d1)
        t.apply_inactivity("B", d1)
        _update(t, surface="Hard", when=d1)
        d2 = D0 + timedelta(days=60)
        t.apply_inactivity("A", d2)
        assert _st(t).axes["grass"].rd == pytest.approx(
            apply_inactivity_rd(grass_rd, D0, d2)
        )
        # clay never trained: no clock, no growth
        assert _st(t).axes["clay"].rd == DEFAULT_RD

    def test_axis_clock_no_compounding(self):
        t = _fresh_pair(("celo_si",))
        for pid in ("A", "B"):
            g = _st(t, pid).axes["grass"]
            g.rd, g.last_date = 100.0, D0
            _st(t, pid).last_match_date = D0
        for week in range(1, 11):
            d = D0 + timedelta(days=7 * week)
            t.apply_inactivity("A", d)
            t.apply_inactivity("B", d)
            _update(t, surface="Hard", when=d)
        end = D0 + timedelta(days=70)
        assert _st(t).axes["grass"].rd == pytest.approx(
            apply_inactivity_rd(100.0, D0, end)
        )
        assert _st(t).axes["grass"].rd == pytest.approx(135.0)

    def test_melo_share_and_fallback(self):
        k = k_factor_from(DEFAULT_RD, 0, "R32", "250")
        # Valid margin: games share at MELO_K_SCALE (carpet: no axes).
        t = _fresh_pair()
        _update(t, surface="Carpet", pg=13, og=11, valid=True)
        raw = 1500.0 + k * MELO_K_SCALE * (games_share(13, 11) - 0.5)
        assert _st(t, v="melo_si").rating == pytest.approx(
            raw + REVERSION_RATE * (DEFAULT_ELO - raw)
        )
        binary = 1500.0 + k * 0.5
        assert _st(t, v="celo_si").rating == pytest.approx(
            binary + REVERSION_RATE * (DEFAULT_ELO - binary)
        )
        # Invalid margin (a flagged retirement): binary at scale 1 for both.
        t = _fresh_pair()
        _update(t, surface="Carpet", pg=7, og=3, valid=False)
        assert _st(t, v="melo_si").rating == _st(t, v="celo_si").rating

    def test_retirement_margin_invalid_for_melo_si(self):
        """result_type "retirement" with a null reason passes margin_is_valid;
        melo_si still falls back to binary."""
        t = _fresh_pair()
        _update(t, surface="Hard", pg=7, og=3, valid=True,
                result_type="retirement")
        for pid in ("A", "B"):
            assert _st(t, pid, "melo_si").rating == _st(t, pid, "celo_si").rating
            assert (
                _st(t, pid, "melo_si").axes["hard"].adj
                == _st(t, pid, "celo_si").axes["hard"].adj
            )

    def test_null_result_skips_update(self):
        t = _fresh_pair()
        _update(t, won=None, surface="Hard", indoor=True)
        for v in t.variants:
            for pid in ("A", "B"):
                st = _st(t, pid, v)
                assert (st.rating, st.rd, st.match_count) == (1500.0, DEFAULT_RD, 0)
                assert all(ax.match_count == 0 for ax in st.axes.values())

    def test_null_result_does_not_double_count_inactivity(self):
        """Real match day 0, no-result row day 10, real match day 20: the base
        rd grows by 20 days in total, not 30."""
        t = _fresh_pair(("celo_si",))
        _update(t, surface="Hard", when=D0)
        _st(t).rd = 80.0
        for day, won in ((10, None), (20, True)):
            d = D0 + timedelta(days=day)
            t.apply_inactivity("A", d)
            t.apply_inactivity("B", d)
            if day == 20:
                assert _st(t).rd == pytest.approx(apply_inactivity_rd(80.0, D0, d))
            _update(t, won=won, surface="Hard", when=d)

    def test_zero_sum_on_equal_k(self):
        t = _fresh_pair()
        _update(t, surface="Hard", indoor=True)
        for v in t.variants:
            a, b = _st(t, "A", v), _st(t, "B", v)
            assert (a.rating - 1500.0) + (b.rating - 1500.0) == pytest.approx(0.0, abs=1e-9)
            for ax in ("hard", "indoor"):
                assert a.axes[ax].adj + b.axes[ax].adj == pytest.approx(0.0, abs=1e-9)

    def test_capture_is_pre_match_and_condition_specific(self):
        t = _fresh_pair()
        for v in t.variants:
            _st(t, v=v).axes["clay"].adj = 20.0
            _st(t, v=v).axes["grass"].adj = -10.0
        assert t.capture("A", "Clay", False) == {"celo_si": 1520.0, "melo_si": 1520.0}
        assert t.capture("A", "Grass", False) == {"celo_si": 1490.0, "melo_si": 1490.0}
        before = t.capture("A", "Clay", False)
        _update(t, surface="Clay", pg=12, og=0)
        after = t.capture("A", "Clay", False)
        assert all(after[v] > before[v] for v in t.variants)

    def test_unknown_variant_raises(self):
        with pytest.raises(ValueError, match="unknown composite variant"):
            CompositeEloTracker(("celo_si", "nope"))


def _composite_match_df(surfaces=None, indoor=None) -> pl.DataFrame:
    n_matches = 6
    surfaces = surfaces or ["Hard", "Clay", "Grass", "Hard", "Carpet", "Hard"]
    indoor = indoor or [True, False, False, False, True, True]
    pairs = [("A", "B"), ("C", "A"), ("B", "C"), ("A", "B"), ("C", "B"), ("A", "C")]
    games = [(12, 3), (13, 11), (7, 3), (12, 0), (0, 0), (13, 12)]
    result_type = [None, None, "retirement", None, "walkover", None]
    rows = []
    for m in range(n_matches):
        p, o = pairs[m]
        pg, og = games[m]
        for pid, oid, won, g1, g2 in ((p, o, True, pg, og), (o, p, False, og, pg)):
            rows.append({
                "match_uid": f"m{m}", "player_id": pid, "opp_id": oid,
                "won": won, "surface": surfaces[m], "indoor": indoor[m],
                "round": "R32", "round_order": 7,
                "tournament_start_date": date(2020, 1, 1),
                "tournament_level": "250",
                "effective_match_date": D0 + timedelta(days=10 * m),
                "player_rank": {"A": 10, "B": 20, "C": 30}[pid],
                "opp_rank": {"A": 10, "B": 20, "C": 30}[oid],
                "player_set1_games": g1, "opp_set1_games": g2,
                "reason": None, "result_type": result_type[m],
            })
    return pl.DataFrame(rows)


class TestSeam:
    def test_default_path_untouched_by_tracker(self):
        df = _composite_match_df()
        plain = compute_all_ratings(df)
        with_c = compute_all_ratings(df, composite_tracker=CompositeEloTracker())
        for col in ALL_RATING_COLUMNS:
            assert plain[col].to_list() == with_c[col].to_list(), col
        for col in ("player_celo_si", "opp_celo_si", "player_melo_si", "opp_melo_si"):
            assert col in with_c.columns and col not in plain.columns
            assert with_c[col].is_not_null().all()

    def test_beside_mov_tracker(self):
        df = _composite_match_df()
        mov_only = compute_all_ratings(df, mov_tracker=MovTracker(("melo",)))
        both = compute_all_ratings(
            df, mov_tracker=MovTracker(("melo",)),
            composite_tracker=CompositeEloTracker(),
        )
        for col in ALL_RATING_COLUMNS + ["player_melo", "opp_melo"]:
            assert mov_only[col].to_list() == both[col].to_list(), col

    def test_symmetric_across_orientations(self):
        out = compute_all_ratings(
            _composite_match_df(), composite_tracker=CompositeEloTracker()
        )
        for m in out["match_uid"].unique().to_list():
            rows = out.filter(pl.col("match_uid") == m)
            r0, r1 = rows.row(0, named=True), rows.row(1, named=True)
            for v in ("celo_si", "melo_si"):
                assert r0[f"player_{v}"] == r1[f"opp_{v}"], (m, v)
                assert r0[f"opp_{v}"] == r1[f"player_{v}"], (m, v)

    def test_carpet_only_matches_champions(self):
        """No axes engage on carpet, so celo_si reduces to the champion's base
        elo and melo_si to melo. Agreement is to float rounding, not bitwise:
        the champion computes the opponent's expectation directly, the
        trackers as 1 - E."""
        df = _composite_match_df(
            surfaces=["Carpet"] * 6,
            indoor=[True, False, True, False, True, False],
        ).with_columns(
            pl.when(pl.col("result_type") == "retirement")
            .then(None).otherwise(pl.col("result_type")).alias("result_type")
        )
        out = compute_all_ratings(
            df, mov_tracker=MovTracker(("melo",)),
            composite_tracker=CompositeEloTracker(),
        )
        for side in ("player", "opp"):
            assert out[f"{side}_celo_si"].to_list() == pytest.approx(
                out[f"{side}_elo"].to_list(), rel=1e-12
            )
            assert out[f"{side}_melo_si"].to_list() == pytest.approx(
                out[f"{side}_melo"].to_list(), rel=1e-12
            )

    def test_missing_games_columns_refused(self):
        df = _composite_match_df().drop(["player_set1_games", "opp_set1_games"])
        with pytest.raises(ValueError, match="composite_tracker passed but"):
            compute_all_ratings(df, composite_tracker=CompositeEloTracker())

    def test_missing_result_type_refused(self):
        with pytest.raises(ValueError, match="result_type"):
            compute_all_ratings(
                _composite_match_df().drop("result_type"),
                composite_tracker=CompositeEloTracker(),
            )
