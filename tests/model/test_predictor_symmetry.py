"""The calibrator that SHIPS is fit on odd-projected OOF predictions.

test_symmetry.py proves the transform and test_runner_symmetry.py proves the
eval path's wiring. This file covers the third consumer, and the one that
actually reaches production: `ProductionPredictor._train_single`, whose Platt
fit lands inside the `.joblib` that serves live.

EVERY fixture here is PAIRED -- two rows per match_uid, one per orientation.
That is load-bearing, not incidental: on a one-row-per-match frame the pair
index finds nothing, `symmetrize` is an exact no-op, and every assertion below
passes whether or not the projection is wired up. `test_fixture_is_paired`
exists to keep that from rotting silently.

The models here are XGBoost for the same reason. A logistic regression fit on a
balanced paired frame is antisymmetric at its optimum -- the loss is invariant
under (w, b) -> (-w, -b), so the fit lands on w_player = -w_opp, b = 0 -- which
makes its predictions already complementary and the projection another no-op.
Trees carry a real even component (split thresholds are not symmetric about
zero), which is exactly the production case: the shipped lead is XGBoost, and
its OOF pairs sum to anywhere from 0.73 to 1.21.
"""

import importlib
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import joblib
import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model.calibration import PlattCalibrator


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.elo
    import mvp.model.features.static

    importlib.reload(mvp.model.features.elo)
    importlib.reload(mvp.model.features.static)


def _matches_frame(n_matches: int, *, drop_one_side_of: set[int] | None = None):
    """Two rows per match, player/opp swapped and `won` flipped.

    `drop_one_side_of` omits the second row for the named match indices, which
    is how a real frame goes unpaired (an eval filter on an antisymmetric
    feature, or an upstream ingest that lost a side).
    """
    rng = np.random.default_rng(11)
    drop = drop_one_side_of or set()
    rows: list[dict] = []
    base = date(2024, 1, 1)
    for m in range(n_matches):
        p1, p2 = f"P{m % 40:02d}", f"P{(m + 17) % 40:02d}"
        if p1 == p2:
            p2 = f"P{(m + 18) % 40:02d}"
        d = datetime.combine(base + timedelta(days=m // 2), datetime.min.time())
        elo_a = 1500.0 + rng.normal(0, 120)
        elo_b = 1500.0 + rng.normal(0, 120)
        # Outcome follows the Elo gap, so the features carry real signal and
        # the fitted calibrator slope is a meaningful number rather than noise.
        p_win = 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))
        a_won = bool(rng.random() < p_win)
        sides = [
            (p1, p2, a_won, elo_a, elo_b, date(1995, 1, 1), date(1993, 1, 1)),
            (p2, p1, not a_won, elo_b, elo_a, date(1993, 1, 1), date(1995, 1, 1)),
        ]
        if m in drop:
            sides = sides[:1]
        for pid, oid, won, e_p, e_o, bd_p, bd_o in sides:
            rows.append({
                "match_uid": f"M{m:05d}",
                "player_id": pid,
                "opp_id": oid,
                "effective_match_date": d,
                "won": won,
                "draw_type": "singles",
                "circuit": "tour" if m % 3 else "chal",
                "surface": "Hard" if m % 2 == 0 else "Clay",
                "round": "R32",
                "reason": None,
                "result_type": "COMPLETED",
                "sets_played": 3 if m % 4 == 0 else 2,
                "best_of": 3,
                "tournament_id": "580",
                "tournament_name": "Test Open",
                "player_first_name": f"F{pid}", "player_last_name": f"L{pid}",
                "opp_first_name": f"F{oid}", "opp_last_name": f"L{oid}",
                "player_display_name": f"F{pid} L{pid}",
                "opp_display_name": f"F{oid} L{oid}",
                "player_elo": e_p, "opp_elo": e_o,
                "player_hard_adj": e_p * 0.01, "opp_hard_adj": e_o * 0.01,
                "player_clay_adj": e_p * -0.01, "opp_clay_adj": e_o * -0.01,
                "player_grass_adj": 0.0, "opp_grass_adj": 0.0,
                "player_serve_elo": e_p + rng.normal(0, 40),
                "opp_serve_elo": e_o + rng.normal(0, 40),
                "player_return_elo": e_p + rng.normal(0, 40),
                "opp_return_elo": e_o + rng.normal(0, 40),
                "player_birth_date": bd_p, "opp_birth_date": bd_o,
                "draw_p1_id": p1,
                "scheduled_datetime": d,
            })
    return pl.DataFrame(rows)


@pytest.fixture
def paired_matches(tmp_path: Path) -> Path:
    path = tmp_path / "matches.parquet"
    _matches_frame(900).write_parquet(path)
    return path


@pytest.fixture
def half_unpaired_matches(tmp_path: Path) -> Path:
    """Four matches in five lose an orientation, putting paired rows below the
    50% floor. Must degrade -- and warn -- rather than raise."""
    path = tmp_path / "matches.parquet"
    dropped = {m for m in range(900) if m % 5}
    _matches_frame(900, drop_one_side_of=dropped).write_parquet(path)
    return path


_FEATURES = [
    "player_elo_surface_diff",
    "player_svc_elo_diff",
    "player_ret_elo_diff",
    "player_age_diff",
]

_XGB = {
    "type": "xgboost",
    "params": {
        "n_estimators": 40, "max_depth": 4, "learning_rate": 0.2,
        "random_state": 42, "n_jobs": 1,
    },
}


def _write_configs(
    tmp_path: Path, *, validation: dict | None, target: str = "won",
) -> Path:
    model_config: dict = {
        "data": {
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "filters": {"draw_type": "singles", "circuit": ["tour", "chal"]},
        },
        "features": {"include": _FEATURES},
        "model": _XGB,
        "target": target,
    }
    if validation is not None:
        model_config["validation"] = validation
    model_path = tmp_path / "model.yaml"
    model_path.write_text(yaml.dump(model_config))

    prod_path = tmp_path / "production.yaml"
    prod_path.write_text(yaml.dump({
        "active": {
            "config": str(model_path),
            "artifact": str(tmp_path / "production.joblib"),
            "trained_at": None,
            "train_date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "filters": {"draw_type": "singles", "circuit": ["tour", "chal"]},
        },
        "history": [],
    }))
    return prod_path


def _train(prod_path: Path, matches: Path, tmp_path: Path):
    from mvp.model.predictor import ProductionPredictor

    ProductionPredictor(
        production_config_path=prod_path,
        matches_path=matches,
        cache_dir=tmp_path / "cache",
    ).train()
    return joblib.load(tmp_path / "production.joblib")


class _ProjectionSpy:
    """Records what the projection was handed and what it returned.

    The recorded INPUT is the only place the even component still exists after
    the fix, and it is what makes these tests falsifiable: an intercept of zero
    proves nothing unless the values feeding the fit were genuinely
    non-complementary to begin with.
    """

    def __init__(self, monkeypatch, name: str) -> None:
        import mvp.model.predictor as predictor_mod

        self.calls: list[tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = []
        real = getattr(predictor_mod, name)

        if name == "symmetrize_indexed":
            def spy(y_prob, i, j):
                out = real(y_prob, i, j)
                self.calls.append((np.asarray(y_prob), out, i, j))
                return out
        else:
            def spy(y_prob, match_uid):
                from mvp.model.symmetry import pair_index

                out, n_pairs, n_solo = real(y_prob, match_uid)
                i, j, _ = pair_index(np.asarray(match_uid))
                self.calls.append((np.asarray(y_prob), out, i, j))
                return out, n_pairs, n_solo

        monkeypatch.setattr(predictor_mod, name, spy)

    @property
    def n_pairs(self) -> int:
        return sum(int(c[2].size) for c in self.calls)

    def input_even_components(self) -> np.ndarray:
        """(logit(p_i) + logit(p_j)) / 2 over every pair the spy saw.

        This is the quantity a Platt fit turns into an intercept: on an
        unprojected frame the optimal intercept is -slope * mean(even).
        """
        out = []
        for y_prob, _, i, j in self.calls:
            p = np.clip(np.asarray(y_prob, dtype=np.float64), 1e-15, 1 - 1e-15)
            lg = np.log(p / (1 - p))
            out.append(0.5 * (lg[i] + lg[j]))
        return np.concatenate(out) if out else np.array([])

    def output_pair_sums(self) -> np.ndarray:
        return np.concatenate(
            [out[i] + out[j] for _, out, i, j in self.calls]
        ) if self.calls else np.array([])


# --------------------------------------------------------------------------
# The anti-trap guard
# --------------------------------------------------------------------------

def test_fixture_is_paired(paired_matches):
    """If this ever fails, every other test in this file is vacuous.

    A one-row-per-match frame makes the pair index empty, the projection an
    identity, and all the intercept assertions below trivially true.
    """
    df = pl.read_parquet(paired_matches)
    counts = df.group_by("match_uid").len()
    assert counts["len"].unique().to_list() == [2], "fixture is not paired"

    per_match = df.group_by("match_uid").agg(
        pl.col("won").cast(pl.Int64).sum().alias("wins"),
        pl.col("player_id").n_unique().alias("n_players"),
    )
    assert per_match["wins"].unique().to_list() == [1], (
        "both rows of a match carry the same label -- these are not the two "
        "sides of one match"
    )
    assert per_match["n_players"].unique().to_list() == [2]


def test_half_unpaired_fixture_really_is_half_unpaired(half_unpaired_matches):
    df = pl.read_parquet(half_unpaired_matches)
    counts = df.group_by("match_uid").len()["len"].value_counts().sort("len")
    assert set(counts["len"].to_list()) == {1, 2}
    solo = int(counts.filter(pl.col("len") == 1)["count"][0])
    assert solo / len(df) > 0.5, "not enough solo rows to trip the floor"


# --------------------------------------------------------------------------
# (a) A paired fixture drives the shipped Platt intercept to zero
# --------------------------------------------------------------------------

def test_temporal_cv_calibrator_intercept_is_zero(
    paired_matches, tmp_path, monkeypatch
):
    """The decisive one.

    Each match contributes rows (L, y=1) and (-L, y=0) once the predictions are
    projected, so the Platt loss is -log s(aL+b) - log s(aL-b), whose derivative
    in b is s(aL+b) - s(aL-b) -- zero at b=0, on a convex problem. A materially
    non-zero intercept in the artifact is therefore a fingerprint of a fit on
    UNPROJECTED predictions.
    """
    spy = _ProjectionSpy(monkeypatch, "symmetrize_indexed")
    prod = _write_configs(tmp_path, validation={
        "type": "date_expanding", "initial_train_months": 4, "test_months": 4,
    })
    artifact = _train(prod, paired_matches, tmp_path)

    assert spy.n_pairs > 0, "projection never ran -- the test proved nothing"

    # There was real work to do: the raw fold predictions were NOT already
    # complementary, and their even component had a non-zero mean -- which is
    # precisely what an unprojected fit would have deposited in the intercept.
    even = spy.input_even_components()
    assert np.abs(even).mean() > 0.01, (
        f"model is effectively antisymmetric here (mean |even| = "
        f"{np.abs(even).mean():.5f}); the intercept assertion below would pass "
        f"without the projection and proves nothing"
    )
    assert abs(even.mean()) > 1e-3, (
        f"even component is mean-zero (mean = {even.mean():.6f}), so an "
        f"unprojected fit would also land on intercept 0"
    )

    sums = spy.output_pair_sums()
    assert sums == pytest.approx(1.0, abs=1e-12), "projection output not complementary"

    cal = artifact["calibrator"]
    assert isinstance(cal, PlattCalibrator)
    assert abs(cal.intercept) < 1e-3, (
        f"shipped intercept {cal.intercept:+.6f} -- the calibrator was fit on "
        f"unprojected OOF predictions"
    )
    assert cal.slope > 0


def test_kfold_fallback_calibrator_intercept_is_zero(
    paired_matches, tmp_path, monkeypatch
):
    """The no-validation-block path takes a different branch to the same fit.

    It projects the completed OOF vector in one pass rather than per fold, so
    it needs its own coverage -- the temporal test above never enters it.
    """
    spy = _ProjectionSpy(monkeypatch, "symmetrize")
    prod = _write_configs(tmp_path, validation=None)
    artifact = _train(prod, paired_matches, tmp_path)

    assert spy.n_pairs > 0, "projection never ran -- the test proved nothing"
    even = spy.input_even_components()
    assert np.abs(even).mean() > 0.01, "no even component to remove"

    cal = artifact["calibrator"]
    assert isinstance(cal, PlattCalibrator)
    assert abs(cal.intercept) < 1e-3, (
        f"shipped intercept {cal.intercept:+.6f} on the K-fold fallback path"
    )


def test_unprojected_control_lands_on_a_nonzero_intercept(
    paired_matches, tmp_path, monkeypatch
):
    """The counterfactual, so the zero above is a result and not a tautology.

    Re-fits Platt on the SAME predictions the run projected, but taking the
    projection's inputs instead of its outputs. If that also came out at zero,
    the assertions in the two tests above would be measuring nothing.
    """
    spy = _ProjectionSpy(monkeypatch, "symmetrize_indexed")
    prod = _write_configs(tmp_path, validation={
        "type": "date_expanding", "initial_train_months": 4, "test_months": 4,
    })
    _train(prod, paired_matches, tmp_path)
    assert spy.calls, "projection never ran"

    # Labels are recoverable from the projected outputs' own pairing: within a
    # pair exactly one side won, and the fixture's outcome follows the Elo gap.
    # Simpler and exact: rebuild y from the frame by uid is unnecessary -- the
    # intercept's sign and size come from the even component alone.
    raw = np.concatenate([c[0] for c in spy.calls])
    proj = np.concatenate([c[1] for c in spy.calls])
    assert np.abs(raw - proj).max() > 1e-4, (
        "projection was a no-op on these predictions"
    )

    # A synthetic label vector built the way the real one is (one winner per
    # pair) reproduces the mechanism without needing the run's y arrays.
    y = np.zeros(raw.size, dtype=int)
    offset = 0
    for y_prob, _, i, j in spy.calls:
        # Higher projected probability is not the label; use the raw pair's
        # order deterministically so the control is reproducible.
        y[offset + i] = 1
        offset += y_prob.size
    unprojected = PlattCalibrator().fit(raw, y)
    projected = PlattCalibrator().fit(proj, y)
    assert abs(projected.intercept) < abs(unprojected.intercept), (
        "projection did not shrink the fitted intercept"
    )


# --------------------------------------------------------------------------
# (b) An EVEN target must not be projected
# --------------------------------------------------------------------------

def test_deciding_set_is_not_projected(paired_matches, tmp_path, monkeypatch):
    """`deciding_set` is EVEN under the orientation swap -- both rows carry the
    identical match-level label, so the two probabilities should be EQUAL.
    Odd-projecting it would force them apart, silently and without error.
    """
    spy_idx = _ProjectionSpy(monkeypatch, "symmetrize_indexed")
    spy_uid = _ProjectionSpy(monkeypatch, "symmetrize")
    prod = _write_configs(tmp_path, validation={
        "type": "date_expanding", "initial_train_months": 4, "test_months": 4,
    }, target="deciding_set")
    artifact = _train(prod, paired_matches, tmp_path)

    assert artifact["target"] == "deciding_set"
    assert not spy_idx.calls, "an even target was odd-projected"
    assert not spy_uid.calls, "an even target was odd-projected"


def test_deciding_set_fixture_has_the_even_label(paired_matches):
    """Guards the test above: it only means something if both rows of a match
    really do carry the same deciding_set label."""
    df = pl.read_parquet(paired_matches).with_columns(
        (pl.col("sets_played") == pl.col("best_of")).alias("ds")
    )
    per_match = df.group_by("match_uid").agg(pl.col("ds").n_unique().alias("n"))
    assert per_match["n"].unique().to_list() == [1]


# --------------------------------------------------------------------------
# (c) Unpaired rows degrade rather than crash
# --------------------------------------------------------------------------

def test_unpaired_rows_train_and_warn(half_unpaired_matches, tmp_path, caplog):
    """Solo rows have nothing to average against. They pass through
    unprojected, keeping the even component, and the run must complete -- but
    not quietly, because a silent revert to the old behaviour is exactly the
    failure this whole mechanism exists to prevent.
    """
    prod = _write_configs(tmp_path, validation={
        "type": "date_expanding", "initial_train_months": 4, "test_months": 4,
    })
    with caplog.at_level(logging.WARNING, logger="mvp.model.predictor"):
        artifact = _train(prod, half_unpaired_matches, tmp_path)

    assert isinstance(artifact["calibrator"], PlattCalibrator)
    assert any(
        "orientation pairs" in r.message and r.levelno == logging.WARNING
        for r in caplog.records
    ), f"no unpaired warning; saw {[r.message for r in caplog.records]}"


def test_fully_unpaired_frame_does_not_raise(tmp_path):
    """The degenerate end of the same axis: no match has a second row at all.

    The pair index is empty, every projection is an identity, and training must
    still produce an artifact -- this is the shape every pre-existing test
    fixture has, and it must not become an error path.
    """
    matches = tmp_path / "matches.parquet"
    _matches_frame(900, drop_one_side_of=set(range(900))).write_parquet(matches)
    prod = _write_configs(tmp_path, validation={
        "type": "date_expanding", "initial_train_months": 4, "test_months": 4,
    })
    artifact = _train(prod, matches, tmp_path)
    assert isinstance(artifact["calibrator"], PlattCalibrator)


# --------------------------------------------------------------------------
# Serving: the unpaired count must be visible above DEBUG
# --------------------------------------------------------------------------

def test_serving_warns_below_the_pairing_floor(caplog):
    from mvp.model.predictor import _odd_project_serving

    frame = pl.DataFrame({"match_uid": [f"M{i}" for i in range(10)]})
    probs = np.linspace(0.3, 0.7, 10)
    with caplog.at_level(logging.DEBUG, logger="mvp.model.predictor"):
        out = _odd_project_serving(probs, frame, "won", "lead")
    assert out == pytest.approx(probs), "solo rows must pass through unchanged"
    assert any(r.levelno == logging.WARNING for r in caplog.records), (
        "a fully collapsed pairing was logged at DEBUG only -- invisible live"
    )


def test_serving_stays_quiet_when_pairing_is_healthy(caplog):
    """A steady trickle of solo rows is normal (46 of 63,696 settled matches in
    the production population). Warning on those trains the eye to skip the
    line, so the floor -- not a non-zero count -- is the trigger."""
    from mvp.model.predictor import _odd_project_serving

    uids = [f"M{i // 2}" for i in range(20)] + ["SOLO"]
    frame = pl.DataFrame({"match_uid": uids})
    probs = np.linspace(0.3, 0.7, 21)
    with caplog.at_level(logging.DEBUG, logger="mvp.model.predictor"):
        _odd_project_serving(probs, frame, "won", "lead")
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_serving_gate_holds_for_an_even_target(caplog):
    from mvp.model.predictor import _odd_project_serving

    frame = pl.DataFrame({"match_uid": [f"M{i // 2}" for i in range(20)]})
    probs = np.linspace(0.3, 0.7, 20)
    out = _odd_project_serving(probs, frame, "deciding_set", "lead")
    assert out is probs, "an even target was odd-projected at serving"
