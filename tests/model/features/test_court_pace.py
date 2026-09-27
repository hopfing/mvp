"""court_pace_index: the per-edition court pace transform.

Covers the window (strictly lower rounds, earlier editions with a one-year
half-life), the shrink toward the three-prior-year cell mean, the qualifying
correction (each contribution by its own year's profile), scope and pending
rows, the surface offset, and an independent plain-loop reference written from
the plan's formulas (mvp-docs/plans/2026-09-27-court-pace-index.md)."""

from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from mvp.model.features.court_pace import (
    PRIOR_STRENGTH,
    _court_pace_transform,
    _qualifying_profile,
    _surface_offset,
)

K = PRIOR_STRENGTH
_SCHEMA = {
    "match_uid": pl.String,
    "player_id": pl.String,
    "tournament_id": pl.String,
    "year": pl.Int32,
    "surface": pl.String,
    "indoor": pl.Boolean,
    "circuit": pl.String,
    "draw_type": pl.String,
    "round_order": pl.Int64,
    "svc_first_serve_pts_won": pl.Int64,
    "svc_first_serve_pts_played": pl.Int64,
    "svc_second_serve_pts_won": pl.Int64,
    "svc_second_serve_pts_played": pl.Int64,
    "player_bsr_pserve_logit": pl.Float64,
}


def row(
    uid,
    tid="T",
    year=2020,
    rnd=7,
    *,
    pid="a",
    surface="Hard",
    indoor=False,
    circuit="tour",
    draw="singles",
    P=100,
    W=50,
    logit=0.0,
):
    """One player-perspective row. With logit 0 the expectation is P/2, so R = W - P/2.
    P=None makes a pending row (no serve counts)."""
    return {
        "match_uid": uid,
        "player_id": pid,
        "tournament_id": tid,
        "year": year,
        "surface": surface,
        "indoor": indoor,
        "circuit": circuit,
        "draw_type": draw,
        "round_order": rnd,
        "svc_first_serve_pts_won": W if P is not None else None,
        "svc_first_serve_pts_played": P,
        "svc_second_serve_pts_won": 0 if P is not None else None,
        "svc_second_serve_pts_played": 0 if P is not None else None,
        "player_bsr_pserve_logit": logit,
    }


def run(rows) -> pl.DataFrame:
    return _court_pace_transform(pl.DataFrame(rows, schema=_SCHEMA))


def val(out, uid, col="court_pace", pid="a"):
    return out.filter((pl.col("match_uid") == uid) & (pl.col("player_id") == pid))[
        col
    ].item()


def test_window_uses_strictly_lower_rounds_of_the_edition():
    base = [
        row("r5", rnd=5, W=70),
        row("r6", rnd=6, W=60),
        row("r6b", rnd=6, W=40),
        row("r7", rnd=7, W=30),
    ]
    v = val(run(base), "r6")
    same_round_and_later = [
        row("r5", rnd=5, W=70),
        row("r6", rnd=6, W=60),
        row("r6b", rnd=6, W=90),
        row("r7", rnd=7, W=90),
    ]
    assert val(run(same_round_and_later), "r6") == pytest.approx(v, abs=1e-12)
    lower_changed = [
        row("r5", rnd=5, W=20),
        row("r6", rnd=6, W=60),
        row("r6b", rnd=6, W=40),
        row("r7", rnd=7, W=30),
    ]
    assert val(run(lower_changed), "r6") != pytest.approx(v, abs=1e-9)


def test_prior_editions_weigh_half_and_quarter():
    out = run(
        [
            row("y18", year=2018, W=70),
            row("y19", year=2019, W=40),
            row("t", year=2020, rnd=5, W=50),
        ]
    )
    c = (20 - 10) / 200  # raw cell mean over 2017-2019
    expected = 100 * (0.5 * -10 + 0.25 * 20 + K * c) / (0.5 * 100 + 0.25 * 100 + K)
    assert val(out, "t") == pytest.approx(expected, abs=1e-12)
    assert val(out, "t", "court_pace_n") == pytest.approx(75.0, abs=1e-12)


def test_first_edition_gets_the_cell_mean():
    out = run(
        [row("a19", tid="A", year=2019, W=70), row("b20", tid="B", year=2020, rnd=5)]
    )
    assert val(out, "b20") == pytest.approx(100 * 20 / 100, abs=1e-12)
    assert val(out, "b20", "court_pace_n") == 0.0


def test_pending_row_reads_completed_lower_rounds_and_contributes_nothing():
    # A pending round-3 row sees its prior edition (weight 0.5) and completed rounds
    # 1-2: n = 0.5 * 100 + 100 + 100.
    completed = [
        row("p19", year=2019, rnd=7, W=60),
        row("r1", rnd=1, W=70),
        row("r2", rnd=2, W=45),
        row("r3a", rnd=3, W=55),
        row("r4", rnd=4, W=65),
    ]
    pending = row("r3p", rnd=3, P=None, logit=None)
    with_pending = run(completed + [pending])
    without = run(completed)
    assert val(with_pending, "r3p") == pytest.approx(
        val(with_pending, "r3a"), abs=1e-12
    )
    assert val(with_pending, "r3p", "court_pace_n") == pytest.approx(250.0, abs=1e-12)
    for uid in ("p19", "r1", "r2", "r3a", "r4"):
        for col in ("court_pace", "court_pace_n", "court_pace_full"):
            assert val(with_pending, uid, col) == pytest.approx(
                val(without, uid, col), abs=1e-12
            )


def test_partial_serve_counts_contribute_nothing():
    rows = [row("r5", rnd=5, W=70), row("r7", rnd=7)]
    partial = row("r6", rnd=6, W=90)
    partial["svc_second_serve_pts_won"] = None
    out = run(rows + [partial])
    assert val(out, "r7", "court_pace_n") == pytest.approx(100.0, abs=1e-12)
    assert val(out, "r7") == pytest.approx(val(run(rows), "r7"), abs=1e-12)


def test_round_order_outside_the_grid_raises():
    with pytest.raises(ValueError, match="round_order outside 1..12"):
        run([row("r5", rnd=5), row("r13", rnd=13)])


def test_cell_mean_uses_the_three_prior_years_only():
    rows = [
        row("a16", tid="A", year=2016, W=90),
        row("a19", tid="A", year=2019, W=70),
        row("c20", tid="C", year=2020, rnd=5, W=100),
        row("b20", tid="B", year=2020, rnd=5),
    ]
    assert val(run(rows), "b20") == pytest.approx(100 * 20 / 100, abs=1e-12)
    moved = [
        row("a16", tid="A", year=2016, W=0),
        row("a19", tid="A", year=2019, W=70),
        row("c20", tid="C", year=2020, rnd=5, W=0),
        row("b20", tid="B", year=2020, rnd=5),
    ]
    assert val(run(moved), "b20") == pytest.approx(100 * 20 / 100, abs=1e-12)


def test_out_of_scope_rows_are_null():
    rows = [
        row("ok", year=2020),
        row("dbl", draw="doubles"),
        row("itf", circuit="itf"),
        row("old", year=2014),
        row("noround", rnd=None),
    ]
    out = run(rows)
    assert val(out, "ok") is not None
    for uid in ("dbl", "itf", "old", "noround"):
        for col in ("court_pace", "court_pace_n", "court_pace_full"):
            assert val(out, uid, col) is None


def test_a_2014_row_with_a_logit_moves_nothing():
    rows = [
        row("y15", year=2015, W=70),
        row("y16", year=2016, rnd=5, W=40),
        row("y16b", year=2016, rnd=6, W=55),
    ]
    base = run(rows)
    with_2014 = run(rows + [row("y14", year=2014, W=100, logit=0.4)])
    for uid in ("y15", "y16", "y16b"):
        for col in ("court_pace", "court_pace_n", "court_pace_full"):
            assert val(with_2014, uid, col) == pytest.approx(
                val(base, uid, col), abs=1e-12
            )


# Qualifying history on clay, so it sets the tour profile without touching the
# hard cell's mean.
_HISTORY_2019 = [
    row("hq", tid="H", year=2019, rnd=1, surface="Clay", W=40),
    row("hm", tid="H", year=2019, rnd=7, surface="Clay", W=60),
]


def test_qualifying_contribution_uses_earlier_years_profile_only():
    # q(tour, qualifying, 2020) = -10/100 - 0/200 = -0.1, so R' of a 2020 qualifying
    # row with R = 0 is +10.
    rows = _HISTORY_2019 + [
        row("xq", tid="X", year=2020, rnd=1, W=50),
        row("xt", tid="X", year=2020, rnd=5),
    ]
    expected = 100 * 10 / (100 + K)
    assert val(run(rows), "xt") == pytest.approx(expected, abs=1e-12)
    same_year_qualifying = rows + [
        row("zq", tid="Z", year=2020, rnd=1, surface="Clay", W=0)
    ]
    assert val(run(same_year_qualifying), "xt") == pytest.approx(expected, abs=1e-12)


def test_main_draw_contribution_is_corrected_too():
    # q(tour, main, 2020) = +10/100 - 0/200 = +0.1, so a 2020 main-draw row with
    # R = 0 contributes -10.
    rows = _HISTORY_2019 + [
        row("x6", tid="X", year=2020, rnd=6, W=50),
        row("x7", tid="X", year=2020, rnd=7),
    ]
    out = val(run(rows), "x7")
    assert out == pytest.approx(100 * -10 / (100 + K), abs=1e-12)
    assert out != pytest.approx(0.0, abs=1e-6)


def test_prior_edition_is_corrected_with_its_own_years_profile():
    # q(main, 2018) = 10/100 - 0/200 = +0.1 from 2017 alone. q(main, 2019), from
    # 2017-2018 including K's own 2018 main-draw row, is 20/300 - 30/500 = +0.00667.
    # K's 2018 edition has R = +10.
    rows = [
        row("q17", tid="H", year=2017, rnd=1, surface="Clay", W=40),
        row("m17", tid="H", year=2017, rnd=7, surface="Clay", W=60),
        row("q18", tid="H", year=2018, rnd=1, surface="Clay", W=70),
        row("m18", tid="H", year=2018, rnd=7, surface="Clay", W=50),
        row("k18", tid="K", year=2018, rnd=7, W=60),
        row("k19", tid="K", year=2019, rnd=5),
    ]
    c = 10 / 100
    own_year = 100 * (0.5 * (10 - 100 * 0.1) + K * c) / (0.5 * 100 + K)
    target_year = (
        100 * (0.5 * (10 - 100 * (20 / 300 - 30 / 500)) + K * c) / (0.5 * 100 + K)
    )
    got = val(run(rows), "k19")
    assert got == pytest.approx(own_year, abs=1e-12)
    assert got != pytest.approx(target_year, abs=1e-6)


def test_qualifying_profile_weighted_by_prior_serve_points_sums_to_zero():
    rng = np.random.default_rng(3)
    recs = [
        {
            "circuit": c,
            "qual": q,
            "year": y,
            "R": float(rng.normal(0, 5)),
            "P": float(rng.integers(50, 500)),
        }
        for c in ("tour", "chal")
        for q in (False, True)
        for y in range(2016, 2020)
    ]
    con = pl.DataFrame(
        recs,
        schema={
            "circuit": pl.String,
            "qual": pl.Boolean,
            "year": pl.Int32,
            "R": pl.Float64,
            "P": pl.Float64,
        },
    )
    years = pl.DataFrame({"year": list(range(2017, 2021))}, schema={"year": pl.Int32})
    q = _qualifying_profile(con, years)
    for circ in ("tour", "chal"):
        for y in range(2017, 2021):
            prior_p = (
                con.filter((pl.col("circuit") == circ) & (pl.col("year") < y))
                .group_by("qual")
                .agg(pl.col("P").sum())
            )
            qy = q.filter((pl.col("circuit") == circ) & (pl.col("year") == y))
            total = (
                prior_p.join(qy, on="qual")
                .select((pl.col("P") * pl.col("q")).sum())
                .item()
            )
            assert total == pytest.approx(0.0, abs=1e-9)


@pytest.mark.parametrize(
    ("surface", "indoor", "expected"),
    [
        ("Hard", False, 0.000),
        ("Hard", True, 1.061),
        ("Clay", False, -2.012),
        ("Clay", True, -1.524),
        ("Grass", False, 2.717),
        ("Grass", True, 2.717),
    ],
)
def test_surface_offset_table(surface, indoor, expected):
    assert _surface_offset(surface, indoor) == pytest.approx(expected, abs=5e-4)
    out = run([row("t", year=2020, surface=surface, indoor=indoor)])
    assert val(out, "t", "court_pace_full") - val(out, "t") == pytest.approx(
        _surface_offset(surface, indoor), abs=1e-12
    )


def test_surface_change_starts_a_fresh_key():
    out = run(
        [
            row("hard19", tid="A", year=2019, W=70),
            row("clay20", tid="A", year=2020, rnd=5, surface="Clay"),
        ]
    )
    assert val(out, "clay20", "court_pace_n") == 0.0
    assert val(out, "clay20") == pytest.approx(0.0, abs=1e-12)


def test_both_orientation_rows_carry_identical_values():
    out = run(
        [
            row("m1", rnd=5, pid="a", W=60),
            row("m1", rnd=5, pid="b", W=45),
            row("m2", rnd=6, pid="a", W=55),
            row("m2", rnd=6, pid="b", W=50),
        ]
    )
    for col in ("court_pace", "court_pace_n", "court_pace_full"):
        assert val(out, "m2", col, "a") == pytest.approx(
            val(out, "m2", col, "b"), abs=1e-12
        )


def test_round_robin_rows_do_not_see_each_other():
    groups = {"g1": 70, "g2": 40, "g3": 55}

    def rows(ws):
        return [row(g, rnd=4, W=w) for g, w in ws.items()] + [row("sf", rnd=10)]

    base = run(rows(groups))
    assert val(base, "sf", "court_pace_n") == pytest.approx(300.0, abs=1e-12)
    for moved in groups:
        w = run(rows({**groups, moved: 95}))
        assert val(w, "sf") != pytest.approx(val(base, "sf"), abs=1e-9)
        for other in groups:
            if other != moved:
                assert val(w, other) == pytest.approx(val(base, other), abs=1e-12)


def _reference(rows):
    """Plain loops over the plan's formulas; shares no code with the transform
    except the offset table."""

    def s3(s):
        return s if s in ("Clay", "Grass") else "Hard"

    def in_scope(r):
        return (
            r["draw_type"] == "singles"
            and r["circuit"] in ("tour", "chal")
            and r["tournament_id"] is not None
            and r["year"] is not None
            and r["year"] >= 2015
            and r["round_order"] is not None
        )

    contribs = []
    for r in rows:
        if (
            not in_scope(r)
            or r["svc_first_serve_pts_played"] is None
            or r["player_bsr_pserve_logit"] is None
        ):
            continue
        P = r["svc_first_serve_pts_played"] + r["svc_second_serve_pts_played"]
        if P <= 0:
            continue
        W = r["svc_first_serve_pts_won"] + r["svc_second_serve_pts_won"]
        R = W - P / (1 + math.exp(-r["player_bsr_pserve_logit"]))
        key = (
            r["tournament_id"],
            s3(r["surface"]),
            bool(r["indoor"]) if r["indoor"] is not None else False,
        )
        contribs.append(
            (
                key,
                r["circuit"],
                r["year"],
                r["round_order"],
                r["round_order"] <= 3,
                R,
                P,
            )
        )

    def q(circ, qual, y):
        qr = qp = ar = ap = 0.0
        for _k, c, yy, _ro, qu, R, P in contribs:
            if c == circ and yy < y:
                ar += R
                ap += P
                if qu == qual:
                    qr += R
                    qp += P
        return qr / qp - ar / ap if qp > 0 else 0.0

    corrected = [
        (k, c, y, ro, R - P * q(c, qu, y), R, P) for k, c, y, ro, qu, R, P in contribs
    ]
    out = {}
    for r in rows:
        if not in_scope(r):
            out[(r["match_uid"], r["player_id"])] = (None, None, None)
            continue
        key = (
            r["tournament_id"],
            s3(r["surface"]),
            bool(r["indoor"]) if r["indoor"] is not None else False,
        )
        y, ro, circ = r["year"], r["round_order"], r["circuit"]
        cr = cp_ = 0.0
        for k, c, yy, _ro, _rq, R, P in corrected:
            if (k[1], c, k[2]) == (key[1], circ, key[2]) and y - 3 <= yy < y:
                cr += R
                cp_ += P
        cell = cr / cp_ if cp_ > 0 else 0.0
        num = den = 0.0
        for k, _c, yy, rr, rq, _R, P in corrected:
            if k != key:
                continue
            if yy < y:
                w = 0.5 ** (y - yy)
                num += w * rq
                den += w * P
            elif yy == y and rr < ro:
                num += rq
                den += P
        pace = 100 * (num + K * cell) / (den + K)
        out[(r["match_uid"], r["player_id"])] = (
            pace,
            den,
            pace + _surface_offset(key[1], key[2]),
        )
    return out


def test_transform_equals_a_plain_loop_reference():
    rng = np.random.default_rng(11)
    rows = []
    for i in range(700):
        circuit = "tour" if rng.random() < 0.5 else "chal"
        tid = f"{circuit[0]}{int(rng.integers(0, 6))}"
        pending = rng.random() < 0.05
        rows.append(
            row(
                f"m{i}",
                tid=tid,
                year=int(rng.integers(2015, 2021)),
                rnd=int(rng.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12])),
                pid="a",
                surface=str(rng.choice(["Hard", "Clay", "Grass", "Carpet"])),
                indoor=bool(rng.random() < 0.3),
                circuit=circuit,
                P=None if pending else int(rng.integers(40, 120)),
                W=int(rng.integers(20, 80)),
                logit=None if pending else float(rng.normal(0.4, 0.3)),
            )
        )
        rows[-1]["svc_first_serve_pts_won"] = (
            None
            if pending
            else min(
                rows[-1]["svc_first_serve_pts_won"],
                rows[-1]["svc_first_serve_pts_played"],
            )
        )
    rows += [
        row("oos1", draw="doubles"),
        row("oos2", year=2014),
        row("oos3", circuit="itf"),
    ]
    out = run(rows)
    ref = _reference(rows)
    for r in out.iter_rows(named=True):
        exp = ref[(r["match_uid"], r["player_id"])]
        for got, want in zip(
            (r["court_pace"], r["court_pace_n"], r["court_pace_full"]), exp
        ):
            if want is None:
                assert got is None
            else:
                assert got == pytest.approx(want, abs=1e-12)


def test_outputs_resolve_to_the_transform_and_enter_the_pool():
    from mvp.model.discovery.discover import get_all_feature_specs
    from mvp.model.registry import get_registry

    registry = get_registry()
    for name in ("court_pace", "court_pace_n", "court_pace_full"):
        assert registry.transform_for_output(name).name == "court_pace_index"
    specs = set(get_all_feature_specs(window_sizes=[0]))
    assert {"court_pace", "court_pace_n", "court_pace_full"} <= specs
