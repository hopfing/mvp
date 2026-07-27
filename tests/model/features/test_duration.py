"""Tests for on-court-duration workload features.

The leak-critical property is that a player's EARLIER match on the same date is
inside the window while the current match and anything after it is not. Before
2026 ``effective_match_date`` carries no clock time, so same-date rows share a
timestamp and the plain ``rolling_*`` helpers drop them via ``closed="left"``;
the ``*_same_day`` primitives order on ``round_order`` to separate them.
"""

from datetime import datetime

import polars as pl
import pytest

from mvp.model.features import fitness as _fitness  # noqa: F401
from mvp.model.features import tournament_points as _tp  # noqa: F401
from mvp.model.features._duration_helpers import (
    match_minutes,
    match_minutes_for_rates,
)
from mvp.model.registry import get_registry

# m1 a week earlier, then three matches on one date in ascending round order.
_ROWS = [
    # uid, date,       round_order, minutes, points
    ("m1", "2026-07-20", 1, 60.0, 100),
    ("m2", "2026-07-27", 1, 148.0, 200),
    ("m3", "2026-07-27", 2, 96.0, 150),
    ("m4", "2026-07-27", 7, 80.0, 120),
]


def _frame(shuffled: bool = False) -> pl.DataFrame:
    rows = list(reversed(_ROWS)) if shuffled else _ROWS
    return pl.DataFrame({
        "match_uid": [r[0] for r in rows],
        "effective_match_date": [datetime.fromisoformat(r[1]) for r in rows],
        "round_order": [r[2] for r in rows],
        "duration_seconds": [r[3] * 60 for r in rows],
        "pts_total_pts_played": [r[4] for r in rows],
        "sets_played": [2] * len(rows),
        "svc_games_played": [10] * len(rows),
        "ret_games_played": [10] * len(rows),
        "player_id": ["A"] * len(rows),
        "tournament_id": ["T1"] * len(rows),
        "year": [2026] * len(rows),
        "draw_type": ["singles"] * len(rows),
        "tournament_start_date": [datetime(2026, 7, 26)] * len(rows),
        "reason": [None] * len(rows),
        "result_type": ["completed"] * len(rows),
    })


def _val(df: pl.DataFrame, expr: pl.Expr) -> dict:
    out = df.with_columns(expr.alias("v"))
    return dict(zip(out["match_uid"], out["v"]))


# The full duration surface, in one place so the registration, introspection and
# scoping tests cannot drift apart from the implementation.
#
# POOLED_VOLUME accumulates across draw types (a doubles minute is real court
# time); each has a SINGLES_TWIN on a draw-type-partitioned key so the pair is
# decomposable. SCOPED_RATES divide by a stats-feed denominator that describes a
# doubles TEAM's match, so they must never pool.
SINGLES_TWINS = {
    "tourn_minutes_played": "tourn_singles_minutes_played",
    "tourn_last_match_minutes": "tourn_singles_last_match_minutes",
    "tourn_max_match_minutes": "tourn_singles_max_match_minutes",
    "tourn_minutes_per_elapsed_day": "tourn_singles_minutes_per_elapsed_day",
    "minutes_played": "singles_minutes_played",
    "last_match_minutes_per_rest_day": "singles_last_match_minutes_per_rest_day",
}
POOLED_VOLUME = list(SINGLES_TWINS)
SCOPED_RATES = [
    "tourn_minutes_per_point", "tourn_minutes_per_game", "tourn_minutes_per_set",
    "tourn_minutes_per_match", "tourn_minutes_excess",
    "minutes_per_point", "minutes_per_game", "minutes_per_set",
    "minutes_per_match", "minutes_std",
]
ALL_DURATION = POOLED_VOLUME + list(SINGLES_TWINS.values()) + SCOPED_RATES


class TestSameDayVisibility:
    """Requirement 1: an earlier match on the SAME date must be in the window."""

    def test_rolling_sees_same_day_earlier_match(self):
        from mvp.model.features.fitness import minutes_played

        got = _val(_frame(), minutes_played(days=30))
        assert got["m1"] is None, "no prior match"
        assert got["m2"] == pytest.approx(60.0), "sees m1 only"
        assert got["m3"] == pytest.approx(208.0), "must see same-day m2 (60+148)"
        assert got["m4"] == pytest.approx(304.0), "must see same-day m2+m3"

    def test_tourn_cumulative_sees_same_day_earlier_match(self):
        registry = get_registry()
        got = _val(_frame(), registry.get("tourn_minutes_played").func())
        assert got["m1"] is None
        assert got["m2"] == pytest.approx(60.0)
        assert got["m3"] == pytest.approx(208.0)
        assert got["m4"] == pytest.approx(304.0)

    def test_result_is_independent_of_frame_order(self):
        from mvp.model.features.fitness import minutes_played

        assert _val(_frame(), minutes_played(days=30)) == _val(
            _frame(shuffled=True), minutes_played(days=30),
        )


class TestNoLeak:
    """Requirement 2: never see the current match or anything after it."""

    def test_current_match_excluded(self):
        from mvp.model.features.fitness import minutes_played

        got = _val(_frame(), minutes_played(days=30))
        # Each value is the sum of strictly prior matches; m4 must exclude its
        # own 80 minutes (60+148+96 = 304, not 384).
        assert got["m4"] == pytest.approx(304.0)

    def test_later_same_day_round_excluded(self):
        from mvp.model.features.fitness import minutes_played

        got = _val(_frame(), minutes_played(days=30))
        # m2 is the first round of the day: m3 and m4 came after it.
        assert got["m2"] == pytest.approx(60.0)

    def test_every_row_sees_only_strictly_earlier_matches(self):
        """Exhaustive: reconstruct the expected prefix sum in play order."""
        from mvp.model.features.fitness import minutes_played

        got = _val(_frame(), minutes_played(days=3650))
        running = 0.0
        for uid, _d, _ro, minutes, _p in _ROWS:
            expected = None if running == 0.0 else running
            if expected is None:
                assert got[uid] is None
            else:
                assert got[uid] == pytest.approx(expected)
            running += minutes


class TestWindowBoundary:
    """The ordering offset must not evict a match sitting on the window edge.

    Regression: an offset measured in SECONDS shifted the current row forward
    far enough that a prior match exactly N days back fell outside the window
    whenever the current row had the higher round_order. Across 2021-2025 that
    lost more history than the same-day fix recovered (net -1,233 matches).
    """

    def _edge_frame(self, old_round: int, new_round: int) -> pl.DataFrame:
        return pl.DataFrame({
            "match_uid": ["old", "now"],
            "effective_match_date": [datetime(2026, 7, 20), datetime(2026, 7, 27)],
            "round_order": [old_round, new_round],
            "duration_seconds": [100 * 60.0, 50 * 60.0],
            "pts_total_pts_played": [150, 150],
            "sets_played": [2, 2],
            "player_id": ["A", "A"],
        })

    @pytest.mark.parametrize(
        ("old_round", "new_round"),
        [(1, 12), (12, 1), (1, 1), (7, 8), (12, 12)],
    )
    def test_match_exactly_n_days_back_is_still_visible(self, old_round, new_round):
        from mvp.model.features.fitness import minutes_played

        got = _val(self._edge_frame(old_round, new_round), minutes_played(days=7))
        assert got["now"] == pytest.approx(100.0), (
            f"prior match exactly 7d back was evicted "
            f"(old round_order={old_round}, new={new_round})"
        )

    def test_match_outside_window_stays_excluded(self):
        """Widening must not reach a genuinely out-of-window match."""
        from mvp.model.features.fitness import minutes_played

        df = self._edge_frame(1, 12).with_columns(
            pl.Series("effective_match_date",
                      [datetime(2026, 7, 19), datetime(2026, 7, 27)]),
        )
        got = _val(df, minutes_played(days=7))
        assert got["now"] is None, "8 days back must fall outside a 7d window"


class TestCleaning:
    """Junk durations, suspensions, and retirement handling."""

    def _one(self, **over) -> pl.DataFrame:
        base = {
            "duration_seconds": 90 * 60.0, "pts_total_pts_played": 130,
            "sets_played": 2, "reason": None, "result_type": "completed",
        }
        base.update(over)
        return pl.DataFrame({k: [v] for k, v in base.items()})

    def test_unparsed_score_is_nulled(self):
        """The 12% junk block: null sets_played carries a garbage duration."""
        df = self._one(sets_played=None, duration_seconds=300.0)
        assert df.with_columns(match_minutes().alias("v"))["v"][0] is None

    def test_normal_match_passes_through(self):
        got = self._one().with_columns(match_minutes().alias("v"))["v"][0]
        assert got == pytest.approx(90.0)

    def test_suspension_is_winsorized(self):
        """1,861 minutes over 130 points is wall-clock, not court time."""
        df = self._one(duration_seconds=1861 * 60.0)
        got = df.with_columns(match_minutes().alias("v"))["v"][0]
        assert got == pytest.approx(2.0 * 130)  # clamped to MPP_HI * points

    def test_implausibly_fast_is_winsorized_up(self):
        df = self._one(duration_seconds=5 * 60.0)
        got = df.with_columns(match_minutes().alias("v"))["v"][0]
        assert got == pytest.approx(0.2 * 130)  # clamped to MPP_LO * points

    def test_retirement_kept_for_volume_masked_for_rates(self):
        df = self._one(reason="RET", duration_seconds=14 * 60.0,
                       pts_total_pts_played=40)
        volume = df.with_columns(match_minutes().alias("v"))["v"][0]
        assert volume == pytest.approx(14.0)
        assert df.with_columns(match_minutes_for_rates().alias("v"))["v"][0] is None

    def test_retirement_via_result_type_also_masked(self):
        df = self._one(result_type="retirement", duration_seconds=14 * 60.0,
                       pts_total_pts_played=40)
        assert df.with_columns(match_minutes_for_rates().alias("v"))["v"][0] is None


class TestDerivedFeatures:
    def test_last_match_minutes(self):
        registry = get_registry()
        got = _val(_frame(), registry.get("tourn_last_match_minutes").func())
        assert got["m1"] is None
        assert got["m2"] == pytest.approx(60.0)
        assert got["m3"] == pytest.approx(148.0), "the same-day morning match"
        assert got["m4"] == pytest.approx(96.0)

    def test_max_match_minutes(self):
        registry = get_registry()
        got = _val(_frame(), registry.get("tourn_max_match_minutes").func())
        assert got["m1"] is None
        assert got["m2"] == pytest.approx(60.0)
        assert got["m3"] == pytest.approx(148.0)
        assert got["m4"] == pytest.approx(148.0), "m2 is still the longest"

    def test_minutes_per_point(self):
        registry = get_registry()
        got = _val(_frame(), registry.get("tourn_minutes_per_point").func())
        assert got["m1"] is None
        assert got["m2"] == pytest.approx(60.0 / 100)
        assert got["m3"] == pytest.approx(208.0 / 300)
        assert got["m4"] == pytest.approx(304.0 / 450)

    def test_minutes_per_elapsed_day_positive_when_play_precedes_start_date(self):
        """Qualifying runs BEFORE tournament_start_date, so the denominator must
        key off the player's own first match, not the main-draw start."""
        registry = get_registry()
        df = _frame()  # m1 is 2026-07-20, tournament_start_date is 2026-07-26
        got = _val(df, registry.get("tourn_minutes_per_elapsed_day").func())
        assert got["m1"] is None
        # elapsed from the player's first match (07-20) to 07-27 is 7 days, +1
        assert got["m2"] == pytest.approx(60.0 / 8)
        assert got["m3"] == pytest.approx(208.0 / 8)
        assert all(v is None or v > 0 for v in got.values())

    def test_last_match_minutes_per_rest_day(self):
        from mvp.model.features.fitness import last_match_minutes_per_rest_day

        got = _val(_frame(), last_match_minutes_per_rest_day())
        assert got["m1"] is None
        assert got["m2"] == pytest.approx(60.0 / 8), "m1 was 7 days back"
        assert got["m3"] == pytest.approx(148.0 / 1), "m2 was the same day"
        assert got["m4"] == pytest.approx(96.0 / 1)


class TestGrouping:
    """Without a second player, a wrong `.over()` key passes every other test."""

    def _two_players(self) -> pl.DataFrame:
        base = _frame()
        other = base.with_columns(
            pl.col("match_uid") + "_B",
            pl.lit("B").alias("player_id"),
            # Different durations so bleed-through is unmistakable. Points scale
            # too, or minutes-per-point would blow past the winsorize ceiling
            # and the cleaner would clamp these back down.
            (pl.col("duration_seconds") * 10).alias("duration_seconds"),
            (pl.col("pts_total_pts_played") * 10).alias("pts_total_pts_played"),
        )
        # Interleaved, so the two players' rows are not contiguous.
        return pl.concat([base, other]).sort("match_uid")

    def test_players_do_not_bleed_into_each_other(self):
        from mvp.model.features.fitness import minutes_played

        got = _val(self._two_players(), minutes_played(days=30))
        assert got["m2"] == pytest.approx(60.0)
        assert got["m3"] == pytest.approx(208.0)
        assert got["m2_B"] == pytest.approx(600.0)
        assert got["m3_B"] == pytest.approx(2080.0)

    def test_tourn_features_partition_by_player(self):
        registry = get_registry()
        got = _val(self._two_players(), registry.get("tourn_minutes_played").func())
        assert got["m3"] == pytest.approx(208.0)
        assert got["m3_B"] == pytest.approx(2080.0)

    def test_tourn_features_partition_by_tournament(self):
        """A different event must reset the in-tournament accumulation."""
        registry = get_registry()
        df = _frame().with_columns(
            pl.Series("tournament_id", ["T1", "T1", "T2", "T2"]),
        )
        got = _val(df, registry.get("tourn_minutes_played").func())
        assert got["m2"] == pytest.approx(60.0), "still T1"
        assert got["m3"] is None, "first match of T2 has no in-tournament history"
        assert got["m4"] == pytest.approx(96.0), "only m3, not T1's minutes"


class TestDrawTypeHandling:
    """Volume counts doubles; rates stay draw-type-scoped."""

    def _with_doubles(self) -> pl.DataFrame:
        df = _frame()
        dbl = df.head(1).with_columns(
            pl.lit("dbl").alias("match_uid"),
            pl.lit(datetime(2026, 7, 27)).alias("effective_match_date"),
            pl.lit(1, dtype=pl.Int64).alias("round_order"),
            pl.lit(200 * 60.0, dtype=pl.Float64).alias("duration_seconds"),
            # 200 min over 300 points keeps minutes-per-point inside the
            # winsorize bounds, so the cleaner passes it through unchanged.
            pl.lit(300, dtype=pl.Int64).alias("pts_total_pts_played"),
            pl.lit("doubles").alias("draw_type"),
        )
        return pl.concat([df, dbl])

    def test_doubles_counts_toward_volume(self):
        registry = get_registry()
        got = _val(self._with_doubles(), registry.get("tourn_minutes_played").func())
        # m3 is Q2 on 07-27; the doubles match shares the date at round_order 1,
        # so it sorts alongside m2 and both precede m3.
        assert got["m3"] == pytest.approx(60.0 + 148.0 + 200.0)

    def test_doubles_excluded_from_rates(self):
        registry = get_registry()
        got = _val(self._with_doubles(), registry.get("tourn_minutes_per_point").func())
        # Rates group on TOURN_GROUP (includes draw_type), so the singles rate
        # is unchanged by a doubles match at the same event.
        assert got["m3"] == pytest.approx(208.0 / 300)

    def test_rolling_rates_exclude_doubles(self):
        """The rolling rates sit on DRAW_GROUP, matching their in-tournament twins.

        Regression: these were originally on WORKLOAD_GROUP (player_id alone),
        which divided pooled minutes by a denominator mixing singles points with
        a doubles TEAM's points. Pooled would give 408/600; scoped gives 208/300.
        """
        from mvp.model.features.fitness import minutes_per_point

        got = _val(self._with_doubles(), minutes_per_point(days=30))
        assert got["m3"] == pytest.approx(208.0 / 300)
        assert got["m3"] != pytest.approx(408.0 / 600)


class TestSinglesCounterparts:
    """Each pooled volume feature has a twin that excludes doubles.

    The pair must be decomposable: pooled minus singles is the doubles load.
    A twin accidentally left on the pooled key would make the difference 0.
    """

    DOUBLES_MINUTES = 200.0

    def _with_doubles(self) -> pl.DataFrame:
        df = _frame()
        dbl = df.head(1).with_columns(
            # Sorts AFTER m2 and before m3, so it is m3's immediate predecessor
            # — otherwise the last-match features can't tell the twins apart.
            pl.lit("m2z_dbl").alias("match_uid"),
            pl.lit(datetime(2026, 7, 27)).alias("effective_match_date"),
            pl.lit(1, dtype=pl.Int64).alias("round_order"),
            pl.lit(self.DOUBLES_MINUTES * 60.0, dtype=pl.Float64)
            .alias("duration_seconds"),
            pl.lit(300, dtype=pl.Int64).alias("pts_total_pts_played"),
            pl.lit("doubles").alias("draw_type"),
        )
        return pl.concat([df, dbl])

    def test_tourn_volume_pair_decomposes_to_doubles_load(self):
        registry = get_registry()
        df = self._with_doubles()
        pooled = _val(df, registry.get("tourn_minutes_played").func())
        singles = _val(df, registry.get("tourn_singles_minutes_played").func())
        assert pooled["m3"] == pytest.approx(60.0 + 148.0 + self.DOUBLES_MINUTES)
        assert singles["m3"] == pytest.approx(60.0 + 148.0)
        assert pooled["m3"] - singles["m3"] == pytest.approx(self.DOUBLES_MINUTES)

    def test_rolling_volume_pair_decomposes_to_doubles_load(self):
        from mvp.model.features.fitness import minutes_played, singles_minutes_played

        df = self._with_doubles()
        pooled = _val(df, minutes_played(days=30))
        singles = _val(df, singles_minutes_played(days=30))
        assert pooled["m3"] - singles["m3"] == pytest.approx(self.DOUBLES_MINUTES)

    def test_tourn_last_match_minutes_twin_skips_the_doubles_match(self):
        registry = get_registry()
        df = self._with_doubles()
        pooled = _val(df, registry.get("tourn_last_match_minutes").func())
        singles = _val(df, registry.get("tourn_singles_last_match_minutes").func())
        assert pooled["m3"] == pytest.approx(self.DOUBLES_MINUTES), "doubles is nearest"
        assert singles["m3"] == pytest.approx(148.0), "nearest SINGLES is m2"

    def test_rolling_rest_day_twin_skips_the_doubles_match(self):
        from mvp.model.features.fitness import (
            last_match_minutes_per_rest_day,
            singles_last_match_minutes_per_rest_day,
        )

        df = self._with_doubles()
        # Same date, so rest is 0 days and the ratio is just the minutes.
        pooled = _val(df, last_match_minutes_per_rest_day())
        singles = _val(df, singles_last_match_minutes_per_rest_day())
        assert pooled["m3"] == pytest.approx(self.DOUBLES_MINUTES)
        assert singles["m3"] == pytest.approx(148.0)

    def test_tourn_max_match_minutes_twin_ignores_a_longer_doubles_match(self):
        registry = get_registry()
        df = self._with_doubles()
        pooled = _val(df, registry.get("tourn_max_match_minutes").func())
        singles = _val(df, registry.get("tourn_singles_max_match_minutes").func())
        assert pooled["m3"] == pytest.approx(self.DOUBLES_MINUTES)
        assert singles["m3"] == pytest.approx(148.0)

    def test_tourn_minutes_per_elapsed_day_twin_excludes_doubles(self):
        registry = get_registry()
        df = self._with_doubles()
        pooled = _val(df, registry.get("tourn_minutes_per_elapsed_day").func())
        singles = _val(df, registry.get("tourn_singles_minutes_per_elapsed_day").func())
        # m3 is 7 days after the player's first match here, so denominator = 8.
        both = 60.0 + 148.0 + self.DOUBLES_MINUTES
        assert pooled["m3"] == pytest.approx(both / 8.0)
        assert singles["m3"] == pytest.approx((60.0 + 148.0) / 8.0)

    def _doubles_opens_the_week(self) -> pl.DataFrame:
        """Doubles two days BEFORE the first singles match, so the two candidate
        elapsed-day clocks disagree: any-type dates from 07-18, singles from 07-20.
        """
        df = _frame()
        dbl = df.head(1).with_columns(
            pl.lit("a_dbl").alias("match_uid"),
            pl.lit(datetime(2026, 7, 18)).alias("effective_match_date"),
            pl.lit(1, dtype=pl.Int64).alias("round_order"),
            pl.lit(self.DOUBLES_MINUTES * 60.0, dtype=pl.Float64)
            .alias("duration_seconds"),
            pl.lit(300, dtype=pl.Int64).alias("pts_total_pts_played"),
            pl.lit("doubles").alias("draw_type"),
        )
        return pl.concat([df, dbl])

    def test_elapsed_day_twin_keeps_the_pooled_clock(self):
        """Only the NUMERATOR is scoped; the denominator stays the any-type clock.

        Scoping the clock too would date the twin from the first SINGLES match
        (denominator 8 here instead of 10), and the difference of two rates over
        different denominators is not a doubles quantity.
        """
        registry = get_registry()
        df = self._doubles_opens_the_week()
        pooled = _val(df, registry.get("tourn_minutes_per_elapsed_day").func())
        singles = _val(df, registry.get("tourn_singles_minutes_per_elapsed_day").func())
        # m3 on 07-27 is 9 days after the doubles match on 07-18 -> denominator 10.
        assert pooled["m3"] == pytest.approx(
            (60.0 + 148.0 + self.DOUBLES_MINUTES) / 10.0
        )
        assert singles["m3"] == pytest.approx((60.0 + 148.0) / 10.0), (
            "twin must divide by the any-type clock, not the singles-only one"
        )

    def test_elapsed_day_pair_decomposes_to_the_doubles_rate(self):
        registry = get_registry()
        df = self._doubles_opens_the_week()
        pooled = _val(df, registry.get("tourn_minutes_per_elapsed_day").func())
        singles = _val(df, registry.get("tourn_singles_minutes_per_elapsed_day").func())
        assert pooled["m3"] - singles["m3"] == pytest.approx(
            self.DOUBLES_MINUTES / 10.0
        ), "shared denominator is what makes the difference interpretable"


class TestUnusableHistory:
    """"No usable prior duration" must read as null, never as a confident zero."""

    def _junk_then_good(self) -> pl.DataFrame:
        return _frame().with_columns(
            # m1/m2 have unparsed scores -> duration cleaned away.
            pl.Series("sets_played", [None, None, 2, 2], dtype=pl.Int64),
        )

    def test_volume_is_null_not_zero(self):
        from mvp.model.features.fitness import minutes_played

        got = _val(self._junk_then_good(), minutes_played(days=30))
        assert got["m3"] is None, "two priors, neither usable -> unknown, not 0.0"
        assert got["m4"] == pytest.approx(96.0), "m3 is usable"

    def test_tourn_volume_is_null_not_zero(self):
        registry = get_registry()
        got = _val(self._junk_then_good(), registry.get("tourn_minutes_played").func())
        assert got["m3"] is None
        assert got["m4"] == pytest.approx(96.0)

    def test_retirement_in_a_multi_row_frame(self):
        """Volume keeps a retirement's real minutes; rates drop it entirely."""
        from mvp.model.features.fitness import minutes_per_point, minutes_played

        df = _frame().head(2).with_columns(
            pl.Series("reason", ["RET", None]),
        )
        assert _val(df, minutes_played(days=30))["m2"] == pytest.approx(60.0)
        assert _val(df, minutes_per_point(days=30))["m2"] is None


class TestExcessAndStd:
    def test_minutes_excess(self):
        registry = get_registry()
        got = _val(_frame(), registry.get("tourn_minutes_excess").func())
        c = 0.678
        e1 = 60.0 - c * 100
        e2 = 148.0 - c * 200
        e3 = 96.0 - c * 150
        assert got["m1"] is None
        assert got["m2"] == pytest.approx(e1)
        assert got["m3"] == pytest.approx(e1 + e2)
        assert got["m4"] == pytest.approx(e1 + e2 + e3)

    def test_minutes_std_alltime(self):
        from mvp.model.features.fitness import minutes_std

        got = _val(_frame(), minutes_std(days=None))
        assert got["m1"] is None, "no prior"
        assert got["m2"] is None, "one prior — a std needs two"
        # priors [60, 148]
        assert got["m3"] == pytest.approx(62.2254, rel=1e-4)
        # priors [60, 148, 96]
        assert got["m4"] == pytest.approx(44.2417, rel=1e-4)

    def test_minutes_std_windowed_needs_two(self):
        from mvp.model.features.fitness import minutes_std

        got = _val(_frame(), minutes_std(days=30))
        assert got["m2"] is None
        assert got["m3"] == pytest.approx(62.2254, rel=1e-4)


class TestOrderingKeyInvariant:
    """The offset must stay far below the window slack it is paired with."""

    def test_offset_is_orders_of_magnitude_under_the_slack(self):
        from mvp.model.primitives import same_day_window_size

        max_offset_us = 12  # ROUND_ORDER tops out at 12 (F)
        slack_us = 1_000_000  # same_day_window_size adds one second
        assert max_offset_us * 1000 < slack_us, (
            "round_order offset must stay far under the window slack"
        )
        assert same_day_window_size(7) == f"{7 * 86_400 + 1}s"

    def test_absurd_round_order_still_within_slack(self):
        """Headroom check: even a nonsense round_order cannot evict a boundary match."""
        from mvp.model.features.fitness import minutes_played

        df = pl.DataFrame({
            "match_uid": ["old", "now"],
            "effective_match_date": [datetime(2026, 7, 20), datetime(2026, 7, 27)],
            "round_order": [1, 999],
            "duration_seconds": [100 * 60.0, 50 * 60.0],
            "pts_total_pts_played": [150, 150],
            "sets_played": [2, 2],
            "player_id": ["A", "A"],
        })
        assert _val(df, minutes_played(days=7))["now"] == pytest.approx(100.0)


class TestScopingContract:
    """Which features may see doubles, asserted on the expression itself.

    A feature can only be draw-type aware if its expression READS `draw_type`,
    so root-name introspection is a sound test in both directions. This is the
    invariant a future edit to a shared group constant would silently break.
    """

    def _roots(self, name: str) -> set[str]:
        fd = get_registry().get(name)
        params = {"days": 7} if "days" in fd.params else {}
        return set(fd.func(**params).meta.root_names())

    @pytest.mark.parametrize("name", POOLED_VOLUME)
    def test_pooled_volume_does_not_read_draw_type(self, name):
        assert "draw_type" not in self._roots(name), (
            f"{name} is the POOLED half of a pair — doubles must count as load"
        )

    @pytest.mark.parametrize("name", sorted(SINGLES_TWINS.values()))
    def test_singles_twin_reads_draw_type(self, name):
        assert "draw_type" in self._roots(name), f"{name} would pool in doubles"

    @pytest.mark.parametrize("name", SCOPED_RATES)
    def test_rates_read_draw_type(self, name):
        assert "draw_type" in self._roots(name), (
            f"{name} divides by a draw-type-specific denominator and cannot pool"
        )

    def test_every_pooled_volume_feature_has_a_twin(self):
        names = set(get_registry().list_features())
        for pooled, twin in SINGLES_TWINS.items():
            assert pooled in names and twin in names, f"{pooled}/{twin} missing"


class TestRegistration:
    def test_all_registered_with_diffs(self):
        registry = get_registry()
        names = set(registry.list_features())
        assert len(ALL_DURATION) == 22, "duration surface changed — update the lists"
        for base in ALL_DURATION:
            assert base in names, f"{base} not registered"
            assert f"{base}_diff" in names, f"{base}_diff not registered"
            assert registry.get(base).impute is None, f"{base} must be NaN-passthrough"
            assert registry.get(base).mirror, f"{base} must be mirrored"

    @pytest.mark.parametrize("name", ALL_DURATION)
    def test_source_columns_introspect(self, name):
        """A failed introspection makes the engine load the whole parquet."""
        registry = get_registry()
        fd = registry.get(name)
        params = {"days": 7} if "days" in fd.params else {}
        roots = set(fd.func(**params).meta.root_names())
        assert "duration_seconds" in roots, f"{name} does not read duration"

    @pytest.mark.parametrize(
        "name",
        ["minutes_played", "singles_minutes_played", "minutes_per_point",
         "minutes_per_game", "minutes_per_set", "minutes_per_match", "minutes_std"],
    )
    def test_days_none_path_builds(self, name):
        """days=None is the all-time variant and must not raise."""
        registry = get_registry()
        assert registry.get(name).func(days=None) is not None
