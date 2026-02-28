"""Tests for the Feature Engine."""

from pathlib import Path

import pytest

from mvp.experimentation.engine import FeatureEngine, parse_feature_spec


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

    def test_simple_feature_no_params(self):
        """Parse feature with no parameters."""
        name, params = parse_feature_spec("win_rate")
        assert name == "win_rate"
        assert params == {}

    def test_feature_with_single_param(self):
        """Parse feature with one parameter."""
        name, params = parse_feature_spec("win_rate(days=30)")
        assert name == "win_rate"
        assert params == {"days": 30}

    def test_feature_with_multiple_params(self):
        """Parse feature with multiple parameters."""
        name, params = parse_feature_spec("weighted_avg(days=90, decay=0.95)")
        assert name == "weighted_avg"
        assert params == {"days": 90, "decay": 0.95}

    def test_feature_with_string_param(self):
        """Parse feature with string parameter."""
        name, params = parse_feature_spec("surface_win_rate(surface='clay')")
        assert name == "surface_win_rate"
        assert params == {"surface": "clay"}

    def test_feature_with_double_quoted_string(self):
        """Parse feature with double-quoted string parameter."""
        name, params = parse_feature_spec('surface_win_rate(surface="clay")')
        assert name == "surface_win_rate"
        assert params == {"surface": "clay"}

    def test_feature_with_spaces(self):
        """Parse feature with spaces around values."""
        name, params = parse_feature_spec("win_rate( days = 30 )")
        assert name == "win_rate"
        assert params == {"days": 30}

    def test_invalid_spec_raises(self):
        """Invalid spec raises ValueError."""
        with pytest.raises(ValueError, match="Invalid feature spec"):
            parse_feature_spec("win_rate(days=)")

    def test_unclosed_parens_raises(self):
        """Unclosed parentheses raises ValueError."""
        with pytest.raises(ValueError, match="Invalid feature spec"):
            parse_feature_spec("win_rate(days=30")
