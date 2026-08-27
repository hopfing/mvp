"""restricted_logloss under a fixed, per-round population (findings
2026-08-26 §7d): the incumbent decides which rows are scored, every candidate
in the round is scored on exactly those rows, and the incumbent is re-scored
on the new mask at each round start."""

import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import (
    FastForwardSelector,
    _masked_log_loss,
)
from mvp.model.discovery.selection import FeatureSelector
from mvp.model.discovery.shifted_null import _FoldScorer
from mvp.model.engine import get_feature_columns
from mvp.model.metrics import RESTRICTED_LOGLOSS_TAU, compute_restricted_logloss

BASE_SPEC = "player_ranking_points_diff"
CAND_SPECS = [
    "player_win_pct(days=90)",
    "opp_win_pct(days=90)",
    "player_win_pct_diff(days=90)",
]


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking
    import mvp.model.features.serve
    import mvp.model.features.win_rate

    importlib.reload(mvp.model.features.ranking)
    importlib.reload(mvp.model.features.serve)
    importlib.reload(mvp.model.features.win_rate)


@pytest.fixture
def sample_matches(tmp_path: Path) -> Path:
    n = 300
    rng = np.random.RandomState(42)
    df = pl.DataFrame(
        {
            "match_uid": [f"M{i}" for i in range(n)],
            "player_id": [f"P{i % 10}" for i in range(n)],
            "opp_id": [f"P{(i + 5) % 10}" for i in range(n)],
            "effective_match_date": [
                f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(n)
            ],
            "won": [bool(x) for x in rng.randint(0, 2, n)],
            "player_rankings_points": rng.randint(100, 2000, n).tolist(),
            "opp_rankings_points": rng.randint(100, 2000, n).tolist(),
            "circuit": ["tour" for _ in range(n)],
        }
    ).with_columns(pl.col("effective_match_date").str.to_datetime())
    path = tmp_path / "matches.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def rll_offset_config(tmp_path: Path) -> Path:
    config_dict = {
        "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "model": {"type": "xgboost", "params": {"n_estimators": 20}},
        "validation": {
            "type": "walk_forward",
            "n_splits": 2,
            "min_train_size": 50,
            "test_size": 25,
        },
        "discovery": {
            "metric": "restricted_logloss",
            "features": {"base": [BASE_SPEC]},
        },
        "offset": {"feature": BASE_SPEC},
    }
    path = tmp_path / "rll_offset.yaml"
    with open(path, "w") as f:
        yaml.dump(config_dict, f)
    return path


def _fast(config_path: Path, matches: Path, cache: Path) -> FastForwardSelector:
    fast = FastForwardSelector(
        config=DiscoveryConfig.from_file(config_path),
        all_feature_specs=[BASE_SPEC, *CAND_SPECS],
        matches_path=matches,
        cache_dir=cache,
    )
    fast.precompute()
    return fast


def _collect(scorer, specs):
    rows: list = []
    scorer.score_with_folds(specs, _collect=rows)
    return rows


class TestSetIncumbentMasks:
    def test_masks_cover_only_confident_test_rows(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        fast.create_scorer("restricted_logloss")
        info = fast.set_incumbent_masks([BASE_SPEC])

        assert fast.score_masks is not None
        assert len(fast.score_masks) == len(fast.folds)
        assert len(info["coverage"]) == len(fast.folds)
        for fold_idx, (train_idx, test_idx) in enumerate(fast.folds):
            mask = fast.score_masks[fold_idx]
            assert mask.shape == (fast.X_wide.shape[0],)
            # No train row is ever scored; coverage is the confident share of test rows.
            assert not mask[train_idx].any()
            assert mask.sum() == pytest.approx(
                info["coverage"][fold_idx] * test_idx.size, abs=1
            )
            assert 0.0 < info["coverage"][fold_idx] < 1.0

    def test_empty_incumbent_uses_offset_margin(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        """Round 1 of an unseeded run: the population is the offset's own
        confident rows, so candidates can't set it."""
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        fast.create_scorer("restricted_logloss")
        info = fast.set_incumbent_masks([])
        for fold_idx, (_, test_idx) in enumerate(fast.folds):
            p = 1.0 / (1.0 + np.exp(-fast.fold_margins[fold_idx][test_idx]))
            expected = test_idx[np.abs(p - 0.5) > RESTRICTED_LOGLOSS_TAU]
            got = np.flatnonzero(fast.score_masks[fold_idx])
            np.testing.assert_array_equal(np.sort(got), np.sort(expected))
        # Masked log loss is defined wherever the fold has confident rows
        # (a fold with none is reported as nan, not an error).
        for cov, ll in zip(info["coverage"], info["incumbent_masked_logloss"]):
            assert np.isfinite(ll) == (cov > 0)

    def test_incumbent_rescored_on_its_own_mask_matches_diagnostics(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        """The round-start re-score (selection.py) must equal what the mask
        builder reports for the incumbent, or the stop rule compares
        different numbers."""
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        scorer = fast.create_scorer("restricted_logloss")
        info = fast.set_incumbent_masks([BASE_SPEC])
        assert scorer([BASE_SPEC]) == pytest.approx(
            float(np.mean(info["incumbent_masked_logloss"])), rel=1e-9
        )


class TestFixedPopulationScoring:
    def test_candidate_scored_on_mask_rows_only(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        """With a mask set, a candidate's fold score is plain log loss on the
        mask rows — its own confidence distribution plays no part."""
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        scorer = fast.create_scorer("restricted_logloss")
        fast.set_incumbent_masks([BASE_SPEC])
        specs = [BASE_SPEC, CAND_SPECS[2]]

        _, per_fold = scorer.score_with_folds(specs)
        rows = _collect(scorer, specs)
        assert len(rows) == len(per_fold)
        for (fold_idx, test_idx, y_prob), got in zip(rows, per_fold):
            keep = fast.score_masks[fold_idx][test_idx]
            expected = _masked_log_loss(fast.y[test_idx][keep], y_prob[keep])
            assert got == pytest.approx(expected, rel=1e-9)

    def test_candidate_cannot_move_its_population(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        """The shrink mechanism from §7d, made explicit: hand the scorer a
        mask and check that the scored row set is the mask, whatever the
        candidate predicts — a mask flipped by hand changes the score of the
        SAME predictions."""
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        scorer = fast.create_scorer("restricted_logloss")
        fast.set_incumbent_masks([BASE_SPEC])
        specs = [BASE_SPEC, CAND_SPECS[2]]
        _, before = scorer.score_with_folds(specs)

        # Drop the first half of every fold's scored rows from the mask.
        for fold_idx, (_, test_idx) in enumerate(fast.folds):
            scored = np.flatnonzero(fast.score_masks[fold_idx])
            fast.score_masks[fold_idx][scored[: len(scored) // 2]] = False
        _, after = scorer.score_with_folds(specs)

        rows = _collect(scorer, specs)
        for (fold_idx, test_idx, y_prob), a in zip(rows, after):
            keep = fast.score_masks[fold_idx][test_idx]
            assert a == pytest.approx(
                _masked_log_loss(fast.y[test_idx][keep], y_prob[keep]), rel=1e-9
            )
        assert before != after

    def test_no_mask_keeps_the_metric_as_is(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        scorer = fast.create_scorer("restricted_logloss")
        assert fast.score_masks is None
        specs = [BASE_SPEC, CAND_SPECS[2]]
        _, per_fold = scorer.score_with_folds(specs)
        for (fold_idx, test_idx, y_prob), got in zip(_collect(scorer, specs), per_fold):
            assert got == pytest.approx(
                compute_restricted_logloss(fast.y[test_idx], y_prob), rel=1e-9
            )

    def test_other_metrics_ignore_the_mask(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        scorer = fast.create_scorer("log_loss")
        specs = [BASE_SPEC, CAND_SPECS[2]]
        before = scorer(specs)
        fast.set_incumbent_masks([BASE_SPEC])
        assert scorer(specs) == pytest.approx(before, rel=1e-12)

    def test_shifted_null_scorer_uses_the_same_population(
        self, rll_offset_config, sample_matches, tmp_path
    ):
        fast = _fast(rll_offset_config, sample_matches, tmp_path / "cache")
        scorer = fast.create_scorer("restricted_logloss")
        fast.set_incumbent_masks([BASE_SPEC])
        specs = [BASE_SPEC, *CAND_SPECS]
        _, expected = scorer.score_with_folds(specs)

        fold_scorer = _FoldScorer(fast, "restricted_logloss")
        col_indices = np.array([fast.col_to_idx[c] for c in get_feature_columns(specs)])
        got = [fold_scorer.score_fold(f, col_indices) for f in range(len(fast.folds))]
        np.testing.assert_allclose(got, expected, rtol=1e-6)


class TestRoundStartRescore:
    def test_mask_rebuilt_and_incumbent_rescored_each_round(self):
        """The accept/stop comparison must use the incumbent's score under
        the CURRENT round's mask, not the score it won with last round.

        Under mask 0, `a` wins at 0.80. Under mask 1 the incumbent [a]
        re-scores to 0.70 and `b` scores 0.69: delta 0.01 < min_delta 0.05,
        so selection stops. Against the stale 0.80, `b` would be accepted."""
        state = {"mask": None}
        mask_calls: list[list[str]] = []

        def mask_fn(cols: list[str]) -> dict:
            mask_calls.append(list(cols))
            state["mask"] = len(cols)
            return {"coverage": [0.5], "incumbent_masked_logloss": [0.0]}

        table = {
            (0, "a"): 0.80, (0, "b"): 0.90,
            (1, "a"): 0.70, (1, "a,b"): 0.69,
        }

        def scorer(cols: list[str]) -> float:
            return table[(state["mask"], ",".join(sorted(cols)))]

        selector = FeatureSelector(
            scorer=scorer, all_features=["a", "b"], method="forward",
            direction="minimize", min_delta=0.05, mask_fn=mask_fn,
        )
        result = selector.forward_selection(verbose=False)

        assert result.selected_features == ["a"]
        assert mask_calls == [[], ["a"]]

    def test_stale_best_metric_from_checkpoint_is_replaced(self, tmp_path):
        """A resumed checkpoint carries best_metric from whatever mask it was
        scored under; the round-start re-score overrides it."""
        from datetime import UTC, datetime

        from mvp.model.discovery.checkpoint import (
            SelectionCheckpoint,
            save_checkpoint,
        )

        cp_path = tmp_path / "ckpt.json"
        now = datetime.now(UTC)
        save_checkpoint(cp_path, SelectionCheckpoint(
            run_name="t", started_at=now, updated_at=now,
            completed_rounds=[{"feature": "a", "metric": 0.80}],
            current_round=2, total_candidates=1, current_round_scores={},
            best_metric=0.80, direction="minimize", max_features=2,
        ))
        state = {"mask": None}

        def mask_fn(cols):
            state["mask"] = len(cols)
            return {"coverage": [], "incumbent_masked_logloss": []}

        table = {(1, "a"): 0.70, (1, "a,b"): 0.69}

        def scorer(cols):
            return table[(state["mask"], ",".join(sorted(cols)))]

        selector = FeatureSelector(
            scorer=scorer, all_features=["a", "b"], method="forward",
            direction="minimize", min_delta=0.05, mask_fn=mask_fn,
        )
        result = selector.forward_selection(verbose=False, checkpoint_path=cp_path)
        assert result.selected_features == ["a"]
