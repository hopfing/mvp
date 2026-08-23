"""FS orientation averaging, on a PAIRED fixture.

Every other discovery fixture builds one row per match (`M{i}`), so the pair
index finds nothing and the averaging is an exact no-op — which means none of
them can tell whether the FS path works, and a mutation test against them
proves nothing. These use `M{i//2}` with mirrored ids and complementary labels,
so the averaging actually engages.
"""

import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking
    import mvp.model.features.serve
    import mvp.model.features.win_rate

    importlib.reload(mvp.model.features.ranking)
    importlib.reload(mvp.model.features.serve)
    importlib.reload(mvp.model.features.win_rate)


@pytest.fixture
def paired_matches(tmp_path: Path) -> Path:
    """Two rows per match: mirrored identities, complementary outcomes."""
    n_matches = 150
    rng = np.random.RandomState(11)
    rows = []
    for m in range(n_matches):
        a, b = f"P{m % 10}", f"P{(m + 5) % 10}"
        pa, pb = int(rng.randint(100, 2000)), int(rng.randint(100, 2000))
        ra, rb = int(rng.randint(1, 200)), int(rng.randint(1, 200))
        won_a = bool(rng.randint(0, 2))
        date = f"2024-{(m % 12) + 1:02d}-{(m % 28) + 1:02d}"
        for pid, oid, pp, op, pr, orr, w in (
            (a, b, pa, pb, ra, rb, won_a),
            (b, a, pb, pa, rb, ra, not won_a),
        ):
            rows.append({
                "match_uid": f"M{m}",
                "player_id": pid,
                "opp_id": oid,
                "effective_match_date": date,
                "won": w,
                "player_rankings_points": pp,
                "opp_rankings_points": op,
                "player_rank": pr,
                "opp_rank": orr,
                "circuit": "tour",
            })
    df = pl.DataFrame(rows).with_columns(
        pl.col("effective_match_date").str.to_datetime()
    )
    path = tmp_path / "matches.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def paired_config(tmp_path: Path) -> Path:
    config_dict = {
        "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "model": {"type": "xgboost"},
        "validation": {
            "type": "walk_forward",
            "n_splits": 2,
            "min_train_size": 100,
            "test_size": 50,
        },
        "discovery": {"metric": "log_loss", "direction": "minimize"},
    }
    path = tmp_path / "discovery.yaml"
    with open(path, "w") as f:
        yaml.dump(config_dict, f)
    return path


def _selector(config_path, matches, cache_dir, features):
    fast = FastForwardSelector(
        config=DiscoveryConfig.from_file(config_path),
        all_feature_specs=features,
        matches_path=matches,
        cache_dir=cache_dir,
    )
    fast.precompute()
    return fast


def test_fixture_actually_pairs(paired_config, paired_matches, tmp_path):
    """Guard the guard: if this fixture stops pairing, every test below goes
    silently vacuous the way the existing discovery fixtures already are."""
    fast = _selector(
        paired_config, paired_matches, tmp_path / "cache",
        ["player_ranking_points_diff"],
    )
    assert fast.row_uids is not None
    uids, counts = np.unique(fast.row_uids, return_counts=True)
    assert (counts == 2).sum() > 0, "no orientation pairs in the fixture"
    assert (counts == 2).all(), "fixture should be fully paired"


def test_averaging_changes_the_score(
    paired_config, paired_matches, tmp_path, monkeypatch
):
    """The FS path is exercised, not inert. Neutralising the averaging must
    move the candidate's score -- if it doesn't, FS isn't averaging at all."""
    import mvp.model.discovery.fast_selection as fs

    features = ["player_ranking_points_diff"]
    fast = _selector(paired_config, paired_matches, tmp_path / "cache", features)
    with_avg = fast.create_scorer("log_loss")(features)

    # Neutralise only the averaging step, leaving the pair index intact.
    monkeypatch.setattr(fs, "symmetrize_indexed", lambda p, i, j: p)
    fast2 = _selector(paired_config, paired_matches, tmp_path / "cache2", features)
    without_avg = fast2.create_scorer("log_loss")(features)

    assert np.isfinite(with_avg) and np.isfinite(without_avg)
    assert with_avg != pytest.approx(without_avg, abs=1e-12), (
        "averaging had no effect -- the FS path is inert on a paired frame"
    )


def test_scorer_matches_runner_on_paired_data(
    paired_config, paired_matches, tmp_path
):
    """FS and the runner must score the SAME quantity.

    The unpaired version of this test passes whatever each side does about
    orientation, because there is nothing to average. On paired data it becomes
    a real frame-consistency check between the two scoring paths -- the one
    assertion that would catch FS and the runner drifting apart.
    """
    from mvp.model.runner import ExperimentRunner

    config = DiscoveryConfig.from_file(paired_config)
    features = ["player_ranking_points_diff"]
    cache_dir = tmp_path / "cache"

    fast_metric = _selector(
        paired_config, paired_matches, cache_dir, features
    ).create_scorer("log_loss")(features)

    exp_config_path = tmp_path / "exp_config.yaml"
    with open(exp_config_path, "w") as f:
        yaml.dump(config.to_experiment_config_dict(features), f)
    result = ExperimentRunner(
        config_path=exp_config_path,
        matches_path=paired_matches,
        cache_dir=cache_dir,
        log_to_mlflow=False,
    ).run()

    assert fast_metric == pytest.approx(
        result["metrics"]["raw_log_loss"], abs=1e-10
    )
