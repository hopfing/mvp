"""Tests for form feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import form as form_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _make_form_df() -> pl.DataFrame:
    """4 matches for player A across 3 tournaments.

    T1: R16 (won), QF (won)
    T2: R16 (lost)
    T3: F (won)
    """
    return pl.DataFrame({
        "player_id": ["A", "A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 1, 3),
            date(2024, 2, 1),
            date(2024, 3, 1),
        ],
        "tournament_id": ["T1", "T1", "T2", "T3"],
        "round_order": [4, 5, 4, 6],
        "won": [True, True, False, True],
        "draw_type": ["singles"] * 4,
    }).sort("effective_match_date")


class TestDaysSinceLastMatch:
    """Tests for days_since_last_match feature."""

    def test_registered(self):
        feat = get_registry().get("days_since_last_match")
        assert feat.mirror is True
        assert feat.params == []

    def test_computation(self):
        from mvp.model.features.form import days_since_last_match

        df = _make_form_df()
        result = df.with_columns(days_since_last_match().alias("val"))
        # Row 0: first match -> null
        assert result["val"][0] is None
        # Row 1: Jan 1 -> Jan 3 = 2 days
        assert result["val"][1] == pytest.approx(2.0)
        # Row 2: Jan 3 -> Feb 1 = 29 days (2024 is leap year)
        assert result["val"][2] == pytest.approx(29.0)
        # Row 3: Feb 1 -> Mar 1 = 29 days (leap year)
        assert result["val"][3] == pytest.approx(29.0)

    def test_includes_doubles(self):
        """days_since_last_match should include doubles matches."""
        from mvp.model.features.form import days_since_last_match

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 20),
            ],
            "tournament_id": ["T1", "T1", "T2"],
            "round_order": [4, 4, 4],
            "won": [True, True, True],
            "draw_type": ["singles", "doubles", "singles"],
        }).sort("effective_match_date")

        result = df.with_columns(days_since_last_match().alias("val"))
        # Row 2 (singles): previous match is doubles on Jan 5 -> 15 days
        assert result["val"][2] == pytest.approx(15.0)

    def test_multi_player_independence(self):
        from mvp.model.features.form import days_since_last_match

        df = pl.DataFrame({
            "player_id": ["A", "B", "A", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 20),
            ],
            "tournament_id": ["T1", "T1", "T2", "T2"],
            "round_order": [4, 4, 5, 5],
            "won": [True, True, True, True],
            "draw_type": ["singles"] * 4,
        }).sort("effective_match_date")

        result = df.with_columns(days_since_last_match().alias("val"))
        # A: null, then 9 days (Jan 1 -> Jan 10)
        assert result["val"][0] is None
        assert result["val"][2] == pytest.approx(9.0)
        # B: null, then 15 days (Jan 5 -> Jan 20)
        assert result["val"][1] is None
        assert result["val"][3] == pytest.approx(15.0)


class TestPrevTournRoundReached:
    """Tests for prev_tourn_round_reached feature."""

    def test_registered(self):
        feat = get_registry().get("prev_tourn_round_reached")
        assert feat.mirror is True
        assert feat.params == []

    def test_computation(self):
        from mvp.model.features.form import prev_tourn_round_reached

        df = _make_form_df()
        result = df.with_columns(prev_tourn_round_reached().alias("val"))
        # Row 0: first match ever -> null
        assert result["val"][0] is None
        # Row 1: still in T1, no previous tournament -> null
        assert result["val"][1] is None
        # Row 2: first match in T2, prev tourn T1 last round = 5 (QF)
        assert result["val"][2] == pytest.approx(5.0)
        # Row 3: first match in T3, prev tourn T2 last round = 4 (R16)
        assert result["val"][3] == pytest.approx(4.0)

    def test_forward_fill_within_tournament(self):
        """Value carries forward through multi-round tournaments."""
        from mvp.model.features.form import prev_tourn_round_reached

        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 3),
                date(2024, 2, 1),
                date(2024, 2, 3),
                date(2024, 2, 5),
            ],
            "tournament_id": ["T1", "T1", "T2", "T2", "T2"],
            "round_order": [4, 5, 4, 5, 6],
            "won": [True, True, True, True, False],
            "draw_type": ["singles"] * 5,
        }).sort("effective_match_date")

        result = df.with_columns(prev_tourn_round_reached().alias("val"))
        # Rows 0-1: T1, no previous tournament -> null
        assert result["val"][0] is None
        assert result["val"][1] is None
        # Row 2: T2 start, prev T1 last round = 5
        assert result["val"][2] == pytest.approx(5.0)
        # Rows 3-4: still in T2, forward-filled from row 2
        assert result["val"][3] == pytest.approx(5.0)
        assert result["val"][4] == pytest.approx(5.0)

    def test_excludes_doubles(self):
        """Doubles tournaments should not affect singles prev_tourn_round_reached."""
        from mvp.model.features.form import prev_tourn_round_reached

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 15),
                date(2024, 2, 1),
            ],
            "tournament_id": ["T1", "T1", "T2"],
            "round_order": [5, 4, 4],
            "won": [True, True, True],
            "draw_type": ["singles", "doubles", "singles"],
        }).sort("effective_match_date")

        result = df.with_columns(prev_tourn_round_reached().alias("val"))
        # Row 2 (singles T2): previous singles tournament was T1 round 5
        # The doubles match at T1 round 4 should not overwrite this
        assert result["val"][2] == pytest.approx(5.0)

    def test_multi_player_independence(self):
        from mvp.model.features.form import prev_tourn_round_reached

        df = pl.DataFrame({
            "player_id": ["A", "B", "A", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 2, 1),
            ],
            "tournament_id": ["T1", "T1", "T2", "T2"],
            "round_order": [6, 4, 4, 5],
            "won": [True, True, True, True],
            "draw_type": ["singles"] * 4,
        }).sort("effective_match_date")

        result = df.with_columns(prev_tourn_round_reached().alias("val"))
        # A at T2: prev T1 round = 6
        assert result["val"][2] == pytest.approx(6.0)
        # B at T2: prev T1 round = 4
        assert result["val"][3] == pytest.approx(4.0)


class TestMatchCountMinMax:
    """Tests for match_count_min and match_count_max features."""

    def test_registered(self):
        registry = get_registry()
        for name in ["match_count_min", "match_count_max"]:
            feat = registry.get(name)
            assert feat.mirror is False
            assert feat.match_level is True
            assert feat.params == ["days"]
            assert feat.depends_on == ["match_count"]

    def test_min_alltime(self):
        from mvp.model.features.form import match_count_min

        df = pl.DataFrame({
            "player_match_count": [10, 50, 5],
            "opp_match_count": [20, 30, 5],
        })
        result = df.select(match_count_min().alias("val"))
        assert result["val"].to_list() == [10, 30, 5]

    def test_max_alltime(self):
        from mvp.model.features.form import match_count_max

        df = pl.DataFrame({
            "player_match_count": [10, 50, 5],
            "opp_match_count": [20, 30, 5],
        })
        result = df.select(match_count_max().alias("val"))
        assert result["val"].to_list() == [20, 50, 5]

    def test_min_with_days(self):
        from mvp.model.features.form import match_count_min

        df = pl.DataFrame({
            "player_match_count_30d": [3, 8],
            "opp_match_count_30d": [5, 2],
        })
        result = df.select(match_count_min(days=30).alias("val"))
        assert result["val"].to_list() == [3, 2]

    def test_max_with_days(self):
        from mvp.model.features.form import match_count_max

        df = pl.DataFrame({
            "player_match_count_30d": [3, 8],
            "opp_match_count_30d": [5, 2],
        })
        result = df.select(match_count_max(days=30).alias("val"))
        assert result["val"].to_list() == [5, 8]


class TestFormDiffFeatures:
    """Tests for form diff features."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        for name in ["days_since_last_match_diff", "prev_tourn_round_reached_diff"]:
            feat = registry.get(name)
            assert feat.mirror is False
            assert feat.impute == 0
            assert len(feat.depends_on) == 1

    def test_days_since_last_match_diff(self):
        days_since_last_match_diff = get_registry().get("days_since_last_match_diff").func

        df = pl.DataFrame({
            "player_days_since_last_match": [7.0, 14.0],
            "opp_days_since_last_match": [3.0, 21.0],
        })
        result = df.with_columns(days_since_last_match_diff().alias("diff"))
        assert result["diff"][0] == pytest.approx(4.0)
        assert result["diff"][1] == pytest.approx(-7.0)

    def test_prev_tourn_round_reached_diff(self):
        prev_tourn_round_reached_diff = get_registry().get("prev_tourn_round_reached_diff").func

        df = pl.DataFrame({
            "player_prev_tourn_round_reached": [6.0, 4.0],
            "opp_prev_tourn_round_reached": [4.0, 5.0],
        })
        result = df.with_columns(prev_tourn_round_reached_diff().alias("diff"))
        assert result["diff"][0] == pytest.approx(2.0)
        assert result["diff"][1] == pytest.approx(-1.0)


class TestFormFeatureCount:
    """Verify total feature count including new features."""

    def test_new_features_count(self):
        registry = get_registry()
        names = [
            "match_count", "match_count_diff",
            "match_count_min", "match_count_max",
            "days_since_last_match", "days_since_last_match_diff",
            "prev_tourn_round_reached", "prev_tourn_round_reached_diff",
        ]
        for name in names:
            registry.get(name)
        assert len(names) == 8
