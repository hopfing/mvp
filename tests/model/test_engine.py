"""Tests for the Feature Engine."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from mvp.model.engine import FeatureEngine, parse_feature_spec
from mvp.model.registry import feature, get_registry


class TestFeatureEngineInit:
    """Tests for FeatureEngine initialization."""

    def test_init_with_paths(self, tmp_path: Path):
        """Engine initializes with matches_path and cache_dir."""
        matches_path = tmp_path / "matches.parquet"
        cache_dir = tmp_path / "cache"

        engine = FeatureEngine(matches_path=matches_path, cache_dir=cache_dir)

        assert engine.matches_path == matches_path
        assert engine.cache_dir == cache_dir

    def test_init_creates_cache_dir(self, tmp_path: Path):
        """Engine creates cache directory if it doesn't exist."""
        matches_path = tmp_path / "matches.parquet"
        cache_dir = tmp_path / "new_cache_dir"

        assert not cache_dir.exists()
        FeatureEngine(matches_path=matches_path, cache_dir=cache_dir)
        assert cache_dir.exists()


class TestParseFeatureSpec:
    """Tests for parse_feature_spec function."""

    def test_player_feature_no_params(self):
        """Parse player feature with no parameters."""
        prefix, base_name, full_name, params = parse_feature_spec("player_win_rate")
        assert prefix == "player"
        assert base_name == "win_rate"
        assert full_name == "player_win_rate"
        assert params == {}

    def test_opp_feature_no_params(self):
        """Parse opp feature with no parameters."""
        prefix, base_name, full_name, params = parse_feature_spec("opp_win_rate")
        assert prefix == "opp"
        assert base_name == "win_rate"
        assert full_name == "opp_win_rate"
        assert params == {}

    def test_feature_with_single_param(self):
        """Parse feature with one parameter."""
        prefix, base_name, full_name, params = parse_feature_spec(
            "player_win_rate(days=30)"
        )
        assert prefix == "player"
        assert base_name == "win_rate"
        assert full_name == "player_win_rate"
        assert params == {"days": 30}

    def test_feature_with_multiple_params(self):
        """Parse feature with multiple parameters."""
        prefix, base_name, full_name, params = parse_feature_spec(
            "player_weighted_avg(days=90, decay=0.95)"
        )
        assert prefix == "player"
        assert base_name == "weighted_avg"
        assert full_name == "player_weighted_avg"
        assert params == {"days": 90, "decay": 0.95}

    def test_feature_with_string_param(self):
        """Parse feature with string parameter."""
        prefix, base_name, full_name, params = parse_feature_spec(
            "player_surface_win_pct(surface='clay')"
        )
        assert prefix == "player"
        assert base_name == "surface_win_pct"
        assert full_name == "player_surface_win_pct"
        assert params == {"surface": "clay"}

    def test_feature_with_double_quoted_string(self):
        """Parse feature with double-quoted string parameter."""
        prefix, base_name, full_name, params = parse_feature_spec(
            'opp_surface_win_pct(surface="clay")'
        )
        assert prefix == "opp"
        assert base_name == "surface_win_pct"
        assert full_name == "opp_surface_win_pct"
        assert params == {"surface": "clay"}

    def test_feature_with_spaces(self):
        """Parse feature with spaces around values."""
        prefix, base_name, full_name, params = parse_feature_spec(
            "player_win_rate( days = 30 )"
        )
        assert prefix == "player"
        assert base_name == "win_rate"
        assert full_name == "player_win_rate"
        assert params == {"days": 30}

    def test_invalid_spec_raises(self):
        """Invalid spec raises ValueError."""
        with pytest.raises(ValueError, match="Invalid feature spec"):
            parse_feature_spec("player_win_rate(days=)")

    def test_unclosed_parens_raises(self):
        """Unclosed parentheses raises ValueError."""
        with pytest.raises(ValueError, match="Invalid feature spec"):
            parse_feature_spec("player_win_rate(days=30")

    def test_unprefixed_feature_for_match_level(self):
        """Feature spec without prefix returns None for prefix (match-level feature)."""
        prefix, base, full, params = parse_feature_spec("is_clay")
        assert prefix is None
        assert base == "is_clay"
        assert full == "is_clay"
        assert params == {}

    def test_unprefixed_feature_with_params(self):
        """Unprefixed feature with params parses correctly."""
        prefix, base, full, params = parse_feature_spec("some_feature(days=30)")
        assert prefix is None
        assert base == "some_feature"
        assert full == "some_feature"
        assert params == {"days": 30}


@pytest.fixture
def sample_matches_df():
    """Create sample matches DataFrame for testing."""
    return pl.DataFrame(
        {
            "match_uid": ["m1", "m1", "m2", "m2", "m3", "m3", "m4", "m4"],
            "player_id": ["A", "B", "A", "C", "B", "C", "A", "B"],
            "opp_id": ["B", "A", "C", "A", "C", "B", "B", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 10),
                date(2024, 1, 15),
                date(2024, 1, 15),
            ],
            "won": [True, False, True, False, True, False, False, True],
        }
    )


@pytest.fixture
def matches_parquet(tmp_path: Path, sample_matches_df):
    """Write sample matches to parquet and return path."""
    path = tmp_path / "matches.parquet"
    sample_matches_df.write_parquet(path)
    return path


@pytest.fixture
def test_feature_registry():
    """Clear and populate registry with test feature."""
    registry = get_registry()
    registry.clear()

    @feature(name="test_win_rate", params=["days"], mirror=True)
    def test_win_rate(days: int) -> pl.Expr:
        from mvp.model.primitives import rolling_mean

        return rolling_mean("won", days=days, group_by="player_id")

    yield registry
    registry.clear()


class TestFeatureEngineCompute:
    """Tests for FeatureEngine.compute() method."""

    def test_compute_single_feature(
        self, matches_parquet: Path, tmp_path: Path, test_feature_registry
    ):
        """Compute a single feature from a spec."""
        cache_dir = tmp_path / "cache"
        engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

        result = engine.compute(["player_test_win_rate(days=30)"])

        # Should have the feature column
        assert "player_test_win_rate_30d" in result.columns

        # Player A: row 0 has no prior, row 2 has 1 win/1 match = 1.0,
        # row 6 has 2 wins/2 matches = 1.0
        a_rows = result.filter(pl.col("player_id") == "A").sort("effective_match_date")
        win_rates = a_rows["player_test_win_rate_30d"].to_list()

        # First match: no prior data
        assert win_rates[0] is None
        # Second match (day 5): 1 prior win
        assert win_rates[1] == 1.0
        # Third match (day 15): 2 prior wins, 2 matches
        assert win_rates[2] == 1.0

    def test_compute_unknown_feature_raises(
        self, matches_parquet: Path, tmp_path: Path, test_feature_registry
    ):
        """Computing unknown feature raises KeyError."""
        cache_dir = tmp_path / "cache"
        engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

        with pytest.raises(KeyError, match="not found"):
            engine.compute(["player_unknown_feature(days=30)"])


class TestFeatureEngineMirroring:
    """Tests for opponent mirroring functionality."""

    def test_mirror_creates_opp_column(
        self, matches_parquet: Path, tmp_path: Path, test_feature_registry
    ):
        """Mirroring creates opp_* column from player_* via match_uid join."""
        cache_dir = tmp_path / "cache"
        engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

        # Request both player and opp versions
        result = engine.compute([
            "player_test_win_rate(days=30)",
            "opp_test_win_rate(days=30)",
        ])

        # Should have both player_ and opp_ columns
        assert "player_test_win_rate_30d" in result.columns
        assert "opp_test_win_rate_30d" in result.columns

    def test_mirror_values_are_correct(
        self, matches_parquet: Path, tmp_path: Path, test_feature_registry
    ):
        """Opponent feature value matches player's value from same match."""
        cache_dir = tmp_path / "cache"
        engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

        # Request both player and opp versions
        result = engine.compute([
            "player_test_win_rate(days=30)",
            "opp_test_win_rate(days=30)",
        ])

        # For match m4 (A vs B on day 15):
        # - A's row should have opp_test_win_rate_30d = B's player_test_win_rate_30d
        # - B's row should have opp_test_win_rate_30d = A's player_test_win_rate_30d
        m4_a = result.filter(
            (pl.col("match_uid") == "m4") & (pl.col("player_id") == "A")
        )
        m4_b = result.filter(
            (pl.col("match_uid") == "m4") & (pl.col("player_id") == "B")
        )

        # A's opp_* should equal B's player_*
        assert m4_a["opp_test_win_rate_30d"][0] == m4_b["player_test_win_rate_30d"][0]
        # B's opp_* should equal A's player_*
        assert m4_b["opp_test_win_rate_30d"][0] == m4_a["player_test_win_rate_30d"][0]

    def test_no_mirror_when_not_requested(self, tmp_path: Path, test_feature_registry):
        """Requesting only player_* doesn't create opp_* columns."""
        # Create test data
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "opp_id": ["B", "A"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "won": [True, False],
            }
        )
        matches_path = tmp_path / "matches.parquet"
        df.write_parquet(matches_path)
        cache_dir = tmp_path / "cache"

        engine = FeatureEngine(matches_path=matches_path, cache_dir=cache_dir)
        # Only request player_ version, not opp_
        result = engine.compute(["player_test_win_rate(days=30)"])

        assert "player_test_win_rate_30d" in result.columns
        assert "opp_test_win_rate_30d" not in result.columns

    def test_mirror_derived_features(
        self, matches_parquet: Path, tmp_path: Path
    ):
        """Derived features with mirror=True get opp_* columns via second mirror pass."""
        registry = get_registry()
        registry.clear()

        # Base feature (no depends_on)
        @feature(name="test_base", params=[], mirror=True)
        def test_base() -> pl.Expr:
            return pl.col("won").cast(pl.Float64)

        # Derived feature with depends_on AND mirror=True (like is_counterpuncher)
        @feature(
            name="test_derived_bool",
            params=[],
            depends_on=["test_base"],
            mirror=True,
        )
        def test_derived_bool() -> pl.Expr:
            return (pl.col("player_test_base") > 0.5).cast(pl.Int8)

        # Matchup feature that needs opp_test_derived_bool (like matchup_aggressor_vs_counterpuncher)
        @feature(
            name="test_matchup_bool",
            params=[],
            depends_on=["test_derived_bool"],
            mirror=False,
        )
        def test_matchup_bool() -> pl.Expr:
            return (
                (pl.col("player_test_derived_bool") == 1)
                & (pl.col("opp_test_derived_bool") == 1)
            ).cast(pl.Int8)

        try:
            cache_dir = tmp_path / "cache"
            engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

            result = engine.compute([
                "player_test_base",
                "opp_test_base",
                "player_test_derived_bool",
                "opp_test_derived_bool",
                "player_test_matchup_bool",
            ])

            assert "player_test_derived_bool" in result.columns
            assert "opp_test_derived_bool" in result.columns
            assert "player_test_matchup_bool" in result.columns
        finally:
            registry.clear()


class TestFeatureEngineCaching:
    """Tests for feature caching functionality."""

    def test_cache_creates_files(
        self, matches_parquet: Path, tmp_path: Path, test_feature_registry
    ):
        """Computing features creates cache files."""
        cache_dir = tmp_path / "cache"
        engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

        engine.compute(["player_test_win_rate(days=30)"])

        # Should have manifest and parquet file
        assert (cache_dir / "manifest.json").exists()
        assert any(cache_dir.glob("*.parquet"))

    def test_cache_reuses_computed_features(
        self, matches_parquet: Path, tmp_path: Path, test_feature_registry
    ):
        """Second compute call reuses cached features."""
        cache_dir = tmp_path / "cache"
        engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

        # First compute
        result1 = engine.compute(["player_test_win_rate(days=30)"])

        # Modify the registry to return a different value (simulating code change)
        # But since it's cached, we should get the same result
        parquet_files = list(cache_dir.glob("*.parquet"))
        original_mtime = parquet_files[0].stat().st_mtime

        # Second compute - should use cache
        result2 = engine.compute(["player_test_win_rate(days=30)"])

        # File should not have been rewritten
        assert parquet_files[0].stat().st_mtime == original_mtime

        # Results should be identical
        assert (
            result1["player_test_win_rate_30d"].to_list()
            == result2["player_test_win_rate_30d"].to_list()
        )

    def test_cache_invalidated_on_matches_change(
        self, tmp_path: Path, test_feature_registry
    ):
        """Cache is invalidated when matches file changes."""
        # Create initial matches
        df1 = pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "opp_id": ["B", "A"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "won": [True, False],
            }
        )
        matches_path = tmp_path / "matches.parquet"
        df1.write_parquet(matches_path)
        cache_dir = tmp_path / "cache"

        engine = FeatureEngine(matches_path=matches_path, cache_dir=cache_dir)
        result1 = engine.compute(["player_test_win_rate(days=30)"])

        # Modify matches file (add a match)
        df2 = pl.DataFrame(
            {
                "match_uid": ["m1", "m1", "m2", "m2"],
                "player_id": ["A", "B", "A", "B"],
                "opp_id": ["B", "A", "B", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 5),
                ],
                "won": [True, False, True, False],
            }
        )
        df2.write_parquet(matches_path)

        # Compute again - should recompute due to matches change
        result2 = engine.compute(["player_test_win_rate(days=30)"])

        # Results should be different (more rows)
        assert len(result2) > len(result1)


class TestFeatureEngineCoverageReport:
    """Tests for coverage_report() method."""

    def test_coverage_report_returns_stats(
        self, matches_parquet: Path, tmp_path: Path, test_feature_registry
    ):
        """Coverage report returns statistics about computed features."""
        cache_dir = tmp_path / "cache"
        engine = FeatureEngine(matches_path=matches_parquet, cache_dir=cache_dir)

        result = engine.compute([
            "player_test_win_rate(days=30)",
            "opp_test_win_rate(days=30)",
        ])
        report = engine.coverage_report(result)

        # Should have entries for each feature column
        assert "player_test_win_rate_30d" in report
        assert "opp_test_win_rate_30d" in report

        # Each entry should have null_count and null_pct
        player_stats = report["player_test_win_rate_30d"]
        assert "null_count" in player_stats
        assert "null_pct" in player_stats
        assert "total_rows" in player_stats

    def test_coverage_report_computes_correct_stats(self, tmp_path: Path):
        """Coverage report computes correct null statistics."""
        registry = get_registry()
        registry.clear()

        @feature(name="simple_feature", params=[], mirror=False)
        def simple_feature() -> pl.Expr:
            # Return a column that has some nulls
            return pl.lit(None).cast(pl.Float64)

        # Create test data
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1", "m2", "m2"],
                "player_id": ["A", "B", "A", "B"],
                "opp_id": ["B", "A", "B", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 5),
                ],
                "won": [True, False, True, False],
            }
        )
        matches_path = tmp_path / "matches.parquet"
        df.write_parquet(matches_path)
        cache_dir = tmp_path / "cache"

        engine = FeatureEngine(matches_path=matches_path, cache_dir=cache_dir)
        result = engine.compute(["player_simple_feature"])
        report = engine.coverage_report(result)

        # All values should be null
        stats = report["player_simple_feature"]
        assert stats["null_count"] == 4
        assert stats["null_pct"] == 100.0
        assert stats["total_rows"] == 4

        registry.clear()
