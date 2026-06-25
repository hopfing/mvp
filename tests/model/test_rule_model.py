"""Tests for the rule-based ("non-model") predictor."""

import pickle

import numpy as np
import pytest

from mvp.model.models import get_model
from mvp.model.rule_model import RuleBasedModel


def _model(flags, feature_names, **params):
    return RuleBasedModel({"flags": flags, **params}, feature_names=feature_names)


class TestFactory:
    def test_dispatch(self):
        m = get_model(
            "rules",
            {"flags": [{"feature": "player_win_pct_diff(days=30)"}]},
            feature_names=["player_win_pct_diff_30d"],
        )
        assert isinstance(m, RuleBasedModel)
        assert hasattr(m, "fit") and hasattr(m, "predict_proba")

    def test_empty_flags_raises(self):
        with pytest.raises(ValueError, match="flags"):
            get_model("rules", {"flags": []}, feature_names=["x"])

    def test_bad_combine_raises(self):
        with pytest.raises(ValueError, match="combine"):
            RuleBasedModel(
                {"flags": [{"feature": "x"}], "combine": "weighted"},
                feature_names=["x"],
            )


class TestVotes:
    def test_sign_and_deadband(self):
        fn = ["d"]
        m = _model([{"feature": "d", "deadband": 0.1}], fn)
        m._resolved = m._resolve_flags(fn)
        X = np.array([[0.5], [-0.5], [0.05], [-0.05], [0.0]])
        assert list(m._net_votes(X, m._resolved)) == [1, -1, 0, 0, 0]

    def test_nan_neutral(self):
        fn = ["d"]
        m = _model([{"feature": "d"}], fn)
        m._resolved = m._resolve_flags(fn)
        X = np.array([[np.nan], [0.3]])
        assert list(m._net_votes(X, m._resolved)) == [0, 1]

    def test_min_matches_abstain(self):
        fn = [
            "player_win_pct_diff_30d",
            "player_matches_played_30d",
            "opp_matches_played_30d",
        ]
        m = _model(
            [
                {
                    "feature": "player_win_pct_diff(days=30)",
                    "min_matches": 8,
                    "count_feature": "matches_played",
                }
            ],
            fn,
        )
        m._resolved = m._resolve_flags(fn)
        X = np.array(
            [
                [0.3, 10, 10],  # both sides have enough -> vote
                [0.3, 3, 10],   # player thin -> abstain
                [0.3, 10, 3],   # opp thin -> abstain
            ]
        )
        assert list(m._net_votes(X, m._resolved)) == [1, 0, 0]

    def test_pivot_recenters(self):
        # A per-player rate (e.g. h2h win%) votes about pivot 0.5, not 0.
        fn = ["player_h2h_win_pct"]
        m = _model([{"feature": "player_h2h_win_pct", "pivot": 0.5}], fn)
        m._resolved = m._resolve_flags(fn)
        X = np.array([[0.7], [0.3], [0.5], [np.nan]])
        assert list(m._net_votes(X, m._resolved)) == [1, -1, 0, 0]

    def test_pivot_with_deadband(self):
        fn = ["player_h2h_win_pct"]
        m = _model([{"feature": "player_h2h_win_pct", "pivot": 0.5, "deadband": 0.1}], fn)
        m._resolved = m._resolve_flags(fn)
        # 0.55 is within [0.4, 0.6] deadband band -> abstain; 0.65 clears it.
        X = np.array([[0.65], [0.55], [0.35]])
        assert list(m._net_votes(X, m._resolved)) == [1, 0, -1]

    def test_min_matches_side_player_only(self):
        fn = [
            "player_win_pct_diff_30d",
            "player_matches_played_30d",
            "opp_matches_played_30d",
        ]
        m = _model(
            [
                {
                    "feature": "player_win_pct_diff(days=30)",
                    "min_matches": 8,
                    "count_feature": "matches_played",
                    "side": "player",
                }
            ],
            fn,
        )
        m._resolved = m._resolve_flags(fn)
        # player has enough, opp thin -> still votes (opp ignored)
        assert list(m._net_votes(np.array([[0.3, 10, 2]]), m._resolved)) == [1]
        # player thin -> abstain regardless of opp
        assert list(m._net_votes(np.array([[0.3, 2, 10]]), m._resolved)) == [0]

    def test_min_matches_side_opp_only(self):
        fn = [
            "player_win_pct_diff_30d",
            "player_matches_played_30d",
            "opp_matches_played_30d",
        ]
        m = _model(
            [
                {
                    "feature": "player_win_pct_diff(days=30)",
                    "min_matches": 8,
                    "count_feature": "matches_played",
                    "side": "opp",
                }
            ],
            fn,
        )
        m._resolved = m._resolve_flags(fn)
        # opp has enough, player thin -> still votes (player ignored)
        assert list(m._net_votes(np.array([[0.3, 2, 10]]), m._resolved)) == [1]
        # opp thin -> abstain
        assert list(m._net_votes(np.array([[0.3, 10, 2]]), m._resolved)) == [0]

    def test_bad_side_raises(self):
        fn = ["player_win_pct_diff_30d", "player_matches_played_30d", "opp_matches_played_30d"]
        m = _model(
            [
                {
                    "feature": "player_win_pct_diff(days=30)",
                    "min_matches": 8,
                    "count_feature": "matches_played",
                    "side": "sideways",
                }
            ],
            fn,
        )
        with pytest.raises(ValueError, match="side must be"):
            m._resolve_flags(fn)

    def test_net_vote_sums_flags(self):
        fn = ["a", "b", "c"]
        m = _model(
            [{"feature": "a"}, {"feature": "b"}, {"feature": "c"}], fn
        )
        m._resolved = m._resolve_flags(fn)
        X = np.array([[0.2, 0.2, 0.2], [0.2, -0.2, np.nan], [-0.2, -0.2, -0.2]])
        assert list(m._net_votes(X, m._resolved)) == [3, 0, -3]


class TestFitPredict:
    def test_empirical_map_direction(self):
        fn = ["d"]
        rng = np.random.default_rng(0)
        X = np.vstack([np.full((150, 1), 0.5), np.full((150, 1), -0.5)])
        y = np.concatenate(
            [
                (rng.random(150) < 0.8).astype(int),  # +1 vote -> ~80% win
                (rng.random(150) < 0.2).astype(int),  # -1 vote -> ~20% win
            ]
        )
        m = _model([{"feature": "d"}], fn, prior_strength=1)
        m.fit(X, y)
        probs = m.predict_proba(np.array([[0.5], [-0.5], [0.0]]))
        assert probs[0] > 0.6
        assert probs[1] < 0.4
        # net vote = 0 never seen in training -> global base-rate fallback
        assert abs(probs[2] - y.mean()) < 0.05

    def test_output_shape_and_range(self):
        fn = ["d"]
        X = np.array([[0.5], [-0.5], [0.1], [np.nan]])
        y = np.array([1, 0, 1, 0])
        m = _model([{"feature": "d"}], fn)
        m.fit(X, y)
        p = m.predict_proba(X)
        assert p.shape == (4,)
        assert np.all((p >= 0.0) & (p <= 1.0))

    def test_accepts_sample_weight(self):
        fn = ["d"]
        X = np.array([[0.5], [-0.5]])
        y = np.array([1, 0])
        m = _model([{"feature": "d"}], fn)
        m.fit(X, y, sample_weight=np.array([1.0, 2.0]))  # accepted, ignored
        assert m.predict_proba(X).shape == (2,)

    def test_predict_before_fit_raises(self):
        m = _model([{"feature": "d"}], ["d"])
        with pytest.raises(RuntimeError, match="not fitted"):
            m.predict_proba(np.array([[0.1]]))

    def test_picklable(self):
        fn = ["d"]
        X = np.array([[0.5], [-0.5], [0.5], [-0.5]])
        y = np.array([1, 0, 1, 0])
        m = _model([{"feature": "d"}], fn)
        m.fit(X, y)
        m2 = pickle.loads(pickle.dumps(m))
        assert np.allclose(m2.predict_proba(X), m.predict_proba(X))


class TestResolveErrors:
    def test_missing_feature_column_raises(self):
        m = _model([{"feature": "player_win_pct_diff(days=30)"}], ["something_else"])
        with pytest.raises(ValueError, match="not in the feature set"):
            m.fit(np.zeros((2, 1)), np.array([0, 1]))

    def test_min_matches_without_count_feature_raises(self):
        m = _model([{"feature": "d", "min_matches": 5}], ["d"])
        with pytest.raises(ValueError, match="count_feature"):
            m.fit(np.zeros((2, 1)), np.array([0, 1]))

    def test_min_matches_missing_count_column_raises(self):
        m = _model(
            [
                {
                    "feature": "player_win_pct_diff(days=30)",
                    "min_matches": 5,
                    "count_feature": "matches_played",
                }
            ],
            ["player_win_pct_diff_30d"],  # count columns absent
        )
        with pytest.raises(ValueError, match="min_matches needs column"):
            m.fit(np.zeros((2, 1)), np.array([0, 1]))
