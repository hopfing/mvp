"""court_pace_serve: court pace times centred serve power, player / opp / diff.

Covers the products against the pace and the prior 730-day field mean, the
calendar-day window edges, scope, orientation symmetry, nulls, and pool
registration (mvp-docs/plans/2026-09-27-court-pace-serve-interactions.md)."""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from mvp.model.discovery.discover import get_all_feature_specs
from mvp.model.discovery.families import family_of
from mvp.model.features.court_pace import (
    _SERVE_OUTPUTS,
    _SERVE_PACES,
    _SERVE_STEMS,
    _court_pace_serve_transform,
    _court_pace_transform,
)
from mvp.model.registry import get_registry

_SCHEMA = {
    "match_uid": pl.String,
    "player_id": pl.String,
    "opp_id": pl.String,
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
    "effective_match_date": pl.Datetime("us"),
    "player_bsr_ace_mu": pl.Float32,
    "opp_bsr_ace_mu": pl.Float32,
    "player_bsr_serve_mu": pl.Float64,
    "opp_bsr_serve_mu": pl.Float64,
    "player_bsr_return_mu": pl.Float64,
    "opp_bsr_return_mu": pl.Float64,
    "player_style_axis_serve": pl.Float64,
    "opp_style_axis_serve": pl.Float64,
    "player_style_avg_1st_serve_speed": pl.Float64,
    "opp_style_avg_1st_serve_speed": pl.Float64,
}
_ATTR_COLS = {
    "style_serve": "style_axis_serve",
    "bsr_ace": "bsr_ace_mu",
    "serve": "bsr_serve_mu",
    "ret": "bsr_return_mu",
    "serve_speed": "style_avg_1st_serve_speed",
}
ROUND1 = datetime(2020, 6, 1)
ROUND2 = datetime(2020, 6, 3)


def attrs(style=0.5, ace=0.25, serve=0.125, ret=-0.25, speed=190.0):
    """One player's attribute values; dyadic so Float32 ace skill is exact."""
    return {
        "style_serve": style,
        "bsr_ace": ace,
        "serve": serve,
        "ret": ret,
        "serve_speed": speed,
    }


def match(
    uid, a, b, *, when=ROUND2, rnd=2, tid="T", year=2020, surface="Clay", draw="singles"
):
    """Both orientation rows of one match: player 'a' vs 'b', 100 serve points
    each, 60 won, logit 0 (serve residual +10), so completed lower rounds make
    court_pace non-zero."""
    out = []
    for pid, oid, mine, theirs in (
        (f"{uid}a", f"{uid}b", a, b),
        (f"{uid}b", f"{uid}a", b, a),
    ):
        r = {
            "match_uid": uid,
            "player_id": pid,
            "opp_id": oid,
            "tournament_id": tid,
            "year": year,
            "surface": surface,
            "indoor": False,
            "circuit": "tour",
            "draw_type": draw,
            "round_order": rnd,
            "svc_first_serve_pts_won": 60,
            "svc_first_serve_pts_played": 100,
            "svc_second_serve_pts_won": 0,
            "svc_second_serve_pts_played": 0,
            "player_bsr_pserve_logit": 0.0,
            "effective_match_date": when,
        }
        for key, col in _ATTR_COLS.items():
            r[f"player_{col}"] = mine[key]
            r[f"opp_{col}"] = theirs[key]
        out.append(r)
    return out


def base():
    """Two completed round-1 matches of T on 2020-06-01 and the round-2 match
    under test on 2020-06-03."""
    return (
        match(
            "r1x",
            attrs(style=1.0, ace=0.5, serve=0.25, ret=0.0, speed=200.0),
            attrs(style=0.0, ace=-0.5, serve=0.0, ret=0.25, speed=180.0),
            when=ROUND1,
            rnd=1,
        )
        + match(
            "r1y",
            attrs(style=0.5, ace=0.25, serve=0.5, ret=0.125, speed=190.0),
            attrs(style=-0.5, ace=0.0, serve=-0.25, ret=-0.125, speed=184.0),
            when=ROUND1,
            rnd=1,
        )
        + match(
            "r2",
            attrs(style=1.5, ace=0.75, serve=0.375, ret=-0.125, speed=204.0),
            attrs(style=-0.25, ace=-0.25, serve=0.0, ret=0.5, speed=182.0),
        )
    )


def frame(rows):
    return pl.DataFrame(rows, schema=_SCHEMA)


def run(rows):
    return _court_pace_serve_transform(frame(rows))


def val(out, pid, col):
    return out.filter(pl.col("player_id") == pid)[col].item()


def a_value(row, k, side="player"):
    if k == "bsr_balance":
        return row[f"{side}_bsr_serve_mu"] - row[f"{side}_bsr_return_mu"]
    return row[f"{side}_{_ATTR_COLS[k]}"]


def split(stem):
    for p in sorted(_SERVE_PACES, key=len, reverse=True):
        if stem.startswith(p + "_"):
            return p, stem[len(p) + 1 :]
    raise ValueError(stem)


def paces(rows):
    return _court_pace_transform(frame(rows))


def field_mean(rows, k):
    vals = [a_value(r, k) for r in rows if r["effective_match_date"] == ROUND1]
    return sum(vals) / len(vals)


def assert_pace_nonzero(pc, pids):
    for pid in pids:
        for p in _SERVE_PACES:
            assert val(pc, pid, p) != pytest.approx(0.0, abs=1e-9)


def test_player_output_is_pace_times_centred_attribute():
    rows = base()
    out, pc = run(rows), paces(rows)
    assert_pace_nonzero(pc, ["r2a", "r2b"])
    for r in [r for r in rows if r["match_uid"] == "r2"]:
        for stem in _SERVE_STEMS:
            p, k = split(stem)
            expected = val(pc, r["player_id"], p) * (
                a_value(r, k) - field_mean(rows, k)
            )
            assert val(out, r["player_id"], f"player_{stem}") == pytest.approx(
                expected, abs=1e-12
            )


def _observed_c(rows, pid, stem="court_pace_full_bsr_ace"):
    out, pc = run(rows), paces(rows)
    p, k = split(stem)
    pace = val(pc, pid, p)
    assert pace != pytest.approx(0.0, abs=1e-9)
    row = next(r for r in rows if r["player_id"] == pid)
    return a_value(row, k) - val(out, pid, f"player_{stem}") / pace


def test_field_window_is_the_730_days_before_the_calendar_day():
    target = datetime(2020, 6, 3, 10, 0)
    rows = [
        dict(r, effective_match_date=target) if r["match_uid"] == "r2" else r
        for r in base()
    ]
    c0 = _observed_c(rows, "r2a")
    assert c0 == pytest.approx(field_mean(rows, "bsr_ace"), abs=1e-12)
    far = attrs(ace=3.0)
    edge = rows + match(
        "u",
        far,
        far,
        when=target.replace(hour=0) - timedelta(days=730),
        rnd=1,
        tid="U",
        year=2018,
    )
    assert _observed_c(edge, "r2a") != pytest.approx(c0, abs=1e-6)
    past = rows + match(
        "u",
        far,
        far,
        when=target.replace(hour=0) - timedelta(days=731),
        rnd=1,
        tid="U",
        year=2018,
    )
    assert _observed_c(past, "r2a") == pytest.approx(c0, abs=1e-12)
    same_day = rows + match("v", far, far, when=target.replace(hour=9), rnd=1, tid="V")
    assert _observed_c(same_day, "r2a") == pytest.approx(c0, abs=1e-12)


def test_out_of_scope_rows_do_not_move_the_field():
    rows = base()
    c0 = _observed_c(rows, "r2a")
    far = attrs(ace=3.0)
    doubles = rows + match("d", far, far, when=ROUND1, rnd=1, tid="D", draw="doubles")
    assert _observed_c(doubles, "r2a") == pytest.approx(c0, abs=1e-12)


def test_opp_output_equals_the_opponents_player_output():
    out = run(base())
    for stem in _SERVE_STEMS:
        assert val(out, "r2a", f"opp_{stem}") == pytest.approx(
            val(out, "r2b", f"player_{stem}"), abs=1e-12
        )
        assert val(out, "r2b", f"opp_{stem}") == pytest.approx(
            val(out, "r2a", f"player_{stem}"), abs=1e-12
        )


def test_diff_is_pace_times_the_attribute_gap_whatever_the_field():
    rows = base()
    out, pc = run(rows), paces(rows)
    assert_pace_nonzero(pc, ["r2a", "r2b"])
    moved = [
        dict(r, **{f"player_{c}": r[f"player_{c}"] + 1.0 for c in _ATTR_COLS.values()})
        if r["match_uid"] != "r2"
        else r
        for r in rows
    ]
    out_moved = run(moved)
    for r in [r for r in rows if r["match_uid"] == "r2"]:
        pid = r["player_id"]
        for stem in _SERVE_STEMS:
            p, k = split(stem)
            expected = val(pc, pid, p) * (a_value(r, k) - a_value(r, k, "opp"))
            assert val(out, pid, f"player_{stem}_diff") == pytest.approx(
                expected, abs=1e-12
            )
            assert val(out_moved, pid, f"player_{stem}_diff") == pytest.approx(
                expected, abs=1e-12
            )
    assert val(out_moved, "r2a", "player_court_pace_full_bsr_ace") != pytest.approx(
        val(out, "r2a", "player_court_pace_full_bsr_ace"), abs=1e-6
    )


def test_out_of_scope_row_is_null_in_all_outputs():
    rows = base() + match("dbl", attrs(), attrs(), tid="D", draw="doubles")
    out = run(rows)
    for col in _SERVE_OUTPUTS:
        assert val(out, "dbla", col) is None


def test_null_attribute_nulls_its_stems_only():
    rows = base()
    nulled = [
        dict(r, player_bsr_ace_mu=None)
        if r["player_id"] == "r2a"
        else dict(r, opp_bsr_ace_mu=None)
        if r["player_id"] == "r2b"
        else r
        for r in rows
    ]
    out, ref = run(nulled), run(rows)
    ace = [s for s in _SERVE_STEMS if s.endswith("_bsr_ace")]
    for s in ace:
        assert val(out, "r2a", f"player_{s}") is None
        assert val(out, "r2b", f"opp_{s}") is None
        assert val(out, "r2a", f"player_{s}_diff") is None
        assert val(out, "r2b", f"player_{s}_diff") is None
    for s in [s for s in _SERVE_STEMS if s not in ace]:
        for pid in ("r2a", "r2b"):
            for col in (f"player_{s}", f"opp_{s}", f"player_{s}_diff"):
                assert val(out, pid, col) == pytest.approx(
                    val(ref, pid, col), abs=1e-12
                )


def test_first_in_scope_day_has_no_field_and_is_null():
    out = run(base())
    for pid in ("r1xa", "r1xb", "r1ya", "r1yb"):
        for col in _SERVE_OUTPUTS:
            assert val(out, pid, col) is None


def test_duplicate_keys_raise():
    rows = base()
    with pytest.raises(ValueError, match="duplicate \\(match_uid, player_id\\) rows"):
        run(rows + [rows[0]])


def test_outputs_resolve_to_the_transform_enter_the_pool_and_have_families():
    registry = get_registry()
    specs = set(get_all_feature_specs(window_sizes=[0]))
    assert len(_SERVE_OUTPUTS) == 24
    for stem in _SERVE_STEMS:
        for col in (f"player_{stem}", f"opp_{stem}", f"player_{stem}_diff"):
            assert registry.transform_for_output(col).name == "court_pace_serve"
            assert col in specs
            assert family_of(col) == stem
