"""Tests for the IID gap-shrink calibration module.

Everything here is synthetic: a stub serve model with a constructed
between-player gap, and match outcomes drawn from the chain at a known true
shrink. Nothing reads B:/.
"""

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.calibration import (
    DEFAULT_GRID,
    DEFAULT_PROXY_K,
    GRID_FLOOR,
    ChainObjective,
    fit_gap_shrink,
    realized_serve_shares,
    score_at_shrink,
)
from mvp.projection.iid.stateful_chain import match_distribution_from_state_fn

_SPREAD = ChainObjective("iid_crps_spread", (), ())
_STEP = round(DEFAULT_GRID[1] - DEFAULT_GRID[0], 10)


def _ladder(first: float, last: float) -> tuple[float, ...]:
    """Every point from `first` to `last` in the default grid's step."""
    count = int(round((last - first) / _STEP)) + 1
    return tuple(round(first + _STEP * i, 1) for i in range(count))


class _StubServeModel:
    """A serve model with a constructed gap and nothing to fit.

    Satisfies the whole contract the calibration module relies on: a
    `gap_shrink` attribute, `predict` returning the two neutral probability
    arrays, and `predict_state_fn` returning two callables that ignore the
    state. The shrink is applied as the real chain model applies it, by
    compressing each player's distance from the pair mean.
    """

    def __init__(self, p_a, p_b, *, gap_shrink: float = 1.0) -> None:
        self._p_a = np.asarray(p_a, dtype=np.float64)
        self._p_b = np.asarray(p_b, dtype=np.float64)
        self.gap_shrink = gap_shrink

    def _shrunk(self) -> tuple[np.ndarray, np.ndarray]:
        mean = 0.5 * (self._p_a + self._p_b)
        return (
            mean + self.gap_shrink * (self._p_a - mean),
            mean + self.gap_shrink * (self._p_b - mean),
        )

    def predict(self, df):
        return self._shrunk()

    def predict_state_fn(self, df):
        p_a, p_b = self._shrunk()
        return (lambda state: p_a), (lambda state: p_b)


def _synthetic_matches(*, true_shrink: float, n: int = 600, seed: int = 7):
    """A stub model, and matches whose outcomes come from it at `true_shrink`.

    Spreads are drawn from the chain's own spread PMF at the true shrink, so
    the scale that generated the data is `true_shrink` by construction and a
    proper scoring rule is minimised there. Games are reported as a fixed base
    plus the drawn spread; the spread metrics read only the difference.
    """
    rng = np.random.default_rng(seed)
    base = rng.uniform(0.60, 0.68, n)
    half = rng.uniform(0.01, 0.09, n)
    model = _StubServeModel(base + half, base - half)

    matches = pl.DataFrame(
        {
            "match_uid": [f"m{i}" for i in range(n)],
            "player_id": np.arange(n, dtype=np.int64),
            "opp_id": np.arange(n, 2 * n, dtype=np.int64),
            "best_of": np.full(n, 3, dtype=np.int64),
        }
    )

    model.gap_shrink = true_shrink
    p_a_fn, p_b_fn = model.predict_state_fn(matches)
    p_a, p_b = model.predict(matches)
    dist = match_distribution_from_state_fn(
        p_a_fn, p_b_fn, p_a, p_b, matches["best_of"].to_numpy().astype(np.int64)
    )
    cdf = np.cumsum(dist.spread_pmf, axis=1)
    drawn = (cdf < rng.random(n)[:, None]).sum(axis=1)
    spread = drawn - dist.spread_offset
    won = (rng.random(n) < dist.p_match_win_a).astype(np.int64)
    model.gap_shrink = 1.0

    matches = matches.with_columns(
        _target_games_a=pl.Series((12 + spread).astype(np.float64)),
        _target_games_b=pl.Series(np.full(n, 12.0)),
        won=pl.Series(won),
    )
    return model, matches


def _fit(model, matches, **kwargs):
    """`fit_gap_shrink` with the arguments every grid test shares."""
    defaults = dict(method="grid", objective=_SPREAD)
    defaults.update(kwargs)
    return fit_gap_shrink(model, matches, None, **defaults)


class TestRealizedServeShares:
    def test_hand_computed_shares_on_tiny_frame(self):
        """Both servers present, nulls dropped, serve 1 and 2 pooled."""
        matches = pl.DataFrame(
            {
                "match_uid": ["m1", "m2"],
                "player_id": [1, 3],
                "opp_id": [2, 4],
            }
        )
        points = pl.DataFrame(
            {
                "match_uid": ["m1"] * 8 + ["m2"] * 6,
                "server_id": [1, 1, 1, 1, 2, 2, 2, 2] + [3] * 6,
                "serve": [1, 1, 2, 2, 1, 2, 1, 2] + [1, 2, 1, 2, 1, 2],
                "point_won_by_server": [
                    1, 0, 1, 1,           # server 1: 3/4
                    0, 0, 1, None,        # server 2: 1/3 once the null is dropped
                    1, 1, 0, 0, 1, None,  # server 3: 3/5 once the null is dropped
                ],
            },
            schema_overrides={"point_won_by_server": pl.Int64},
        )

        share_a, share_b = realized_serve_shares(points, matches)

        np.testing.assert_allclose(share_a, [0.75, 0.6])
        np.testing.assert_allclose(share_b[0], 1.0 / 3.0)
        # Player 4 serves no points in the frame, so its share is missing.
        assert np.isnan(share_b[1])


class TestScoreAtShrink:
    def test_returns_the_shrink_to_the_model(self):
        """A probe value never survives the call, so callers can hand in a
        model they still intend to use."""
        model, matches = _synthetic_matches(true_shrink=1.3, n=20)
        model.gap_shrink = 0.42

        score = score_at_shrink(model, matches, _SPREAD, 1.9)

        assert np.isfinite(score)
        assert model.gap_shrink == 0.42

    def test_score_moves_with_the_shrink(self):
        """The shrink argument reaches the distribution the metric scores."""
        model, matches = _synthetic_matches(true_shrink=1.3, n=100)

        assert score_at_shrink(model, matches, _SPREAD, 0.6) != score_at_shrink(
            model, matches, _SPREAD, 2.0
        )


class TestFitGapShrinkGrid:
    def test_grid_recovers_the_constructed_scale(self):
        model, matches = _synthetic_matches(true_shrink=1.3)

        fit = _fit(model, matches)

        assert fit.shrink == pytest.approx(1.3)
        assert fit.method == "grid"
        assert fit.evaluated == DEFAULT_GRID
        assert len(fit.scores) == len(DEFAULT_GRID)
        assert fit.slope is None
        assert fit.extended is False
        assert fit.edge is False

    def test_direction_comes_from_the_metric(self):
        """A maximise metric takes the grid's best score, not its lowest."""
        model, matches = _synthetic_matches(true_shrink=1.3)

        fit = _fit(
            model, matches, objective=ChainObjective("iid_match_win_auc", (), ()),
        )

        scores = np.asarray(fit.scores)
        assert fit.shrink == fit.evaluated[int(np.argmax(scores))]
        # Would be vacuous if every grid point scored alike.
        assert (
            fit.evaluated[int(np.argmax(scores))]
            != fit.evaluated[int(np.argmin(scores))]
        )

    def test_model_shrink_is_unchanged_by_the_fit(self):
        model, matches = _synthetic_matches(true_shrink=1.3)
        model.gap_shrink = 0.42

        _fit(model, matches)

        assert model.gap_shrink == 0.42

    def test_model_shrink_is_restored_when_scoring_raises(self):
        """`iid_total_cal` raises without lines; the model still comes back."""
        model, matches = _synthetic_matches(true_shrink=1.3, n=20)
        model.gap_shrink = 0.42

        with pytest.raises(ValueError):
            _fit(model, matches, objective=ChainObjective("iid_total_cal", (), ()))

        assert model.gap_shrink == 0.42

    def test_unknown_method_raises(self):
        model, matches = _synthetic_matches(true_shrink=1.3, n=20)

        with pytest.raises(ValueError, match="sideways"):
            _fit(model, matches, method="sideways")

    @pytest.mark.parametrize(
        "column", ["best_of", "_target_games_a", "_target_games_b", "won"],
    )
    def test_missing_match_column_raises_naming_it(self, column):
        model, matches = _synthetic_matches(true_shrink=1.3, n=20)

        with pytest.raises(KeyError, match=column):
            _fit(model, matches.drop(column))


class TestFitGapShrinkGridExtension:
    def test_extends_below_the_grid_and_recovers_the_scale(self):
        """True scale 0.3 is under the default grid's bottom point."""
        model, matches = _synthetic_matches(true_shrink=0.3)

        fit = _fit(model, matches)

        assert fit.shrink == pytest.approx(0.3)
        assert fit.extended is True
        # The optimum is interior to the extended grid, so nothing is truncated.
        assert fit.edge is False
        # One extension of the grid's own span, stopped at the floor rather
        # than run down through zero into a sign flip.
        assert fit.evaluated == _ladder(GRID_FLOOR, DEFAULT_GRID[-1])
        assert len(fit.scores) == len(fit.evaluated)

    def test_edge_flag_when_the_extended_optimum_is_still_at_an_end(self):
        """A two-point grid extends by its own span, one point, and stops."""
        model, matches = _synthetic_matches(true_shrink=1.3)

        fit = _fit(model, matches, grid=(1.0, 1.1))

        assert fit.evaluated == (1.0, 1.1, 1.2)
        assert fit.shrink == pytest.approx(1.2)
        assert fit.extended is True
        assert fit.edge is True

    def test_no_extension_when_the_optimum_is_interior(self):
        model, matches = _synthetic_matches(true_shrink=1.3)

        fit = _fit(model, matches, grid=(1.1, 1.2, 1.3, 1.4, 1.5))

        assert fit.shrink == pytest.approx(1.3)
        assert fit.evaluated == (1.1, 1.2, 1.3, 1.4, 1.5)
        assert fit.extended is False
        assert fit.edge is False


# The proxy fixture is built backwards from the answer: the realised shares are
# chosen first, as exact counts out of a fixed number of points, and the model's
# neutral gap is then set so that the share gap is an exact affine function of
# it. The least-squares slope of an exactly affine relation is the coefficient
# itself, so the expected slope is known analytically rather than recomputed.
_PROXY_SLOPE = 1.8
_PROXY_INTERCEPT = 0.005
_PROXY_POINTS_PER_SERVER = 200


def _proxy_fixture(*, n_finite: int = 12, n_missing: int = 0):
    n = n_finite + n_missing
    wins_a = np.array([100 + d for d in range(-16, -16 + 4 * n, 4)], dtype=np.int64)
    wins_b = np.full(n, 100, dtype=np.int64)
    share_gap = (wins_a - wins_b) / _PROXY_POINTS_PER_SERVER
    neutral_gap = (share_gap - _PROXY_INTERCEPT) / _PROXY_SLOPE

    model = _StubServeModel(0.65 + neutral_gap / 2, 0.65 - neutral_gap / 2)
    matches = pl.DataFrame(
        {
            "match_uid": [f"m{i}" for i in range(n)],
            "player_id": np.arange(n, dtype=np.int64),
            "opp_id": np.arange(n, 2 * n, dtype=np.int64),
            "best_of": np.full(n, 3, dtype=np.int64),
            "_target_games_a": np.full(n, 13.0),
            "_target_games_b": np.full(n, 11.0),
            "won": np.ones(n, dtype=np.int64),
        }
    )

    uid: list[str] = []
    server: list[int] = []
    outcome: list[int] = []
    for i in range(n):
        sides = [(int(matches["player_id"][i]), int(wins_a[i]))]
        # The trailing matches leave the opponent's side of the frame empty, so
        # its realised share comes back NaN and the row drops out of the fit.
        if i < n_finite:
            sides.append((int(matches["opp_id"][i]), int(wins_b[i])))
        for server_id, won_count in sides:
            uid.extend([f"m{i}"] * _PROXY_POINTS_PER_SERVER)
            server.extend([server_id] * _PROXY_POINTS_PER_SERVER)
            outcome.extend(
                [1] * won_count + [0] * (_PROXY_POINTS_PER_SERVER - won_count)
            )
    points = pl.DataFrame(
        {"match_uid": uid, "server_id": server, "point_won_by_server": outcome},
        schema_overrides={"point_won_by_server": pl.Int64},
    )
    return model, matches, points


class TestFitGapShrinkProxy:
    def test_returns_the_constant_times_the_measured_slope(self):
        model, matches, points = _proxy_fixture()
        model.gap_shrink = 1.7

        fit = fit_gap_shrink(
            model, matches, points, method="proxy", objective=_SPREAD,
        )

        assert fit.slope == pytest.approx(_PROXY_SLOPE)
        assert fit.shrink == pytest.approx(DEFAULT_PROXY_K * _PROXY_SLOPE)
        assert fit.method == "proxy"
        # The proxy evaluates no chain, so it records no points and no scores.
        assert fit.evaluated == ()
        assert fit.scores == ()
        assert fit.edge is False
        assert fit.extended is False
        # The slope is measured at shrink 1.0 whatever the model arrived at, and
        # the model leaves as it came.
        assert model.gap_shrink == 1.7

    def test_proxy_k_is_a_parameter(self):
        model, matches, points = _proxy_fixture()

        fit = fit_gap_shrink(
            model, matches, points,
            method="proxy",
            objective=_SPREAD,
            proxy_k=0.5,
        )

        assert fit.shrink == pytest.approx(0.5 * _PROXY_SLOPE)

    def test_without_a_points_frame_raises(self):
        """The grid takes None for `points`; the proxy cannot."""
        model, matches, _ = _proxy_fixture()
        model.gap_shrink = 1.7

        with pytest.raises(ValueError, match="requires a points frame"):
            fit_gap_shrink(
                model, matches, None, method="proxy", objective=_SPREAD,
            )

        assert model.gap_shrink == 1.7

    def test_too_few_finite_rows_raises(self):
        """Nine matches with both shares present is under the floor."""
        model, matches, points = _proxy_fixture(n_finite=9, n_missing=4)

        with pytest.raises(ValueError, match="needs at least"):
            fit_gap_shrink(
                model, matches, points, method="proxy", objective=_SPREAD,
            )
