"""Tests for fitness/durability feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import fitness as fitness_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _make_fitness_df() -> pl.DataFrame:
    """5 singles matches for player A with retirement history.

    Match 1: won normally
    Match 2: lost via retirement (player retired)
    Match 3: won normally
    Match 4: lost via walkover (player walked over)
    Match 5: won normally
    """
    return pl.DataFrame({
        "player_id": ["A", "A", "A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
            date(2024, 4, 1),
            date(2024, 5, 1),
        ],
        "won": [True, False, True, False, True],
        "reason": [None, "RET", None, "W/O", None],
        "draw_type": ["singles"] * 5,
    }).sort("effective_match_date")


class TestFitnessBaseFeatures:
    """Tests for base fitness features."""

    def test_all_base_registered(self):
        registry = get_registry()
        for name in ["retirement_rate", "last_match_retirement"]:
            feat = registry.get(name)
            assert feat.mirror is True

    def test_retirement_rate_alltime(self):
        from mvp.model.features.fitness import retirement_rate

        df = _make_fitness_df()
        result = df.with_columns(retirement_rate(days=None).alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prior=[m1 ok] -> 0/1 = 0.0
        assert result["val"][1] == pytest.approx(0.0)
        # Row 2: prior=[m1 ok, m2 RET+lost] -> 1/2 = 0.5
        assert result["val"][2] == pytest.approx(0.5)
        # Row 3: prior=[m1 ok, m2 RET, m3 ok] -> 1/3
        assert result["val"][3] == pytest.approx(1 / 3, abs=0.01)
        # Row 4: prior=[m1 ok, m2 RET, m3 ok, m4 W/O] -> 2/4 = 0.5
        assert result["val"][4] == pytest.approx(0.5)

    def test_retirement_rate_rolling(self):
        from mvp.model.features.fitness import retirement_rate

        df = _make_fitness_df()
        result = df.with_columns(retirement_rate(days=365).alias("val"))
        # Same as alltime for this data (all within 365d)
        assert result["val"][0] is None
        assert result["val"][1] == pytest.approx(0.0)
        assert result["val"][4] == pytest.approx(0.5)

    def test_retirement_rate_ignores_opponent_retirement(self):
        """Opponent retiring (player won, reason=RET) should NOT count."""
        from mvp.model.features.fitness import retirement_rate

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
            ],
            "won": [True, True, True],
            "reason": [None, "RET", None],
            "draw_type": ["singles"] * 3,
        }).sort("effective_match_date")
        result = df.with_columns(retirement_rate(days=None).alias("val"))
        # Row 2: prior=[m1 ok, m2 opp retired] -> 0/2 = 0.0
        assert result["val"][2] == pytest.approx(0.0)

    def test_retirement_rate_excludes_doubles(self):
        """Doubles retirements should not affect singles retirement rate."""
        from mvp.model.features.fitness import retirement_rate

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
            ],
            "won": [True, False, True],
            "reason": [None, "RET", None],
            "draw_type": ["singles", "doubles", "singles"],
        }).sort("effective_match_date")
        result = df.with_columns(retirement_rate(days=None).alias("val"))
        # Row 2 (singles): prior singles=[m1 ok] -> 0/1 = 0.0
        # The doubles RET at m2 should not count
        assert result["val"][2] == pytest.approx(0.0)

    def test_last_match_retirement(self):
        from mvp.model.features.fitness import last_match_retirement

        df = _make_fitness_df()
        result = df.with_columns(last_match_retirement().alias("val"))
        # Row 0: no prior -> null
        assert result["val"][0] is None
        # Row 1: prev=m1 (won, no reason) -> 0
        assert result["val"][1] == pytest.approx(0.0)
        # Row 2: prev=m2 (lost, RET) -> 1
        assert result["val"][2] == pytest.approx(1.0)
        # Row 3: prev=m3 (won, no reason) -> 0
        assert result["val"][3] == pytest.approx(0.0)
        # Row 4: prev=m4 (lost, W/O) -> 1
        assert result["val"][4] == pytest.approx(1.0)


class TestFitnessMultiPlayer:
    """Test that features are independent across players."""

    def test_multi_player_independence(self):
        from mvp.model.features.fitness import retirement_rate

        df = pl.DataFrame({
            "player_id": ["A", "B", "A", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 2, 1),
                date(2024, 2, 5),
            ],
            "won": [False, True, True, True],
            "reason": ["RET", None, None, None],
            "draw_type": ["singles"] * 4,
        }).sort("effective_match_date")

        result = df.with_columns(retirement_rate(days=None).alias("val"))
        # A row 2: prior=[m1 RET] -> 1/1 = 1.0
        assert result["val"][2] == pytest.approx(1.0)
        # B row 3: prior=[m2 ok] -> 0/1 = 0.0
        assert result["val"][3] == pytest.approx(0.0)


class TestFitnessDiffFeatures:
    """Tests for fitness diff features."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        for name in ["retirement_rate_diff", "last_match_retirement_diff"]:
            feat = registry.get(name)
            assert feat.mirror is False
            assert feat.impute == 0
            assert len(feat.depends_on) == 1

    def test_retirement_rate_diff_computation(self):
        from mvp.model.features.fitness import retirement_rate_diff

        df = pl.DataFrame({
            "player_retirement_rate_365d": [0.1, 0.3],
            "opp_retirement_rate_365d": [0.05, 0.4],
        })
        result = df.with_columns(retirement_rate_diff(days=365).alias("diff"))
        assert result["diff"][0] == pytest.approx(0.05)
        assert result["diff"][1] == pytest.approx(-0.1)

    def test_last_match_retirement_diff_computation(self):
        from mvp.model.features.fitness import last_match_retirement_diff

        df = pl.DataFrame({
            "player_last_match_retirement": [1.0, 0.0],
            "opp_last_match_retirement": [0.0, 1.0],
        })
        result = df.with_columns(last_match_retirement_diff().alias("diff"))
        assert result["diff"][0] == pytest.approx(1.0)
        assert result["diff"][1] == pytest.approx(-1.0)


class TestFitnessFeatureCount:
    """Verify total feature count."""

    def test_total_count(self):
        registry = get_registry()
        names = [
            "retirement_rate", "last_match_retirement",
            "retirement_rate_diff", "last_match_retirement_diff",
        ]
        for name in names:
            registry.get(name)
        assert len(names) == 4
