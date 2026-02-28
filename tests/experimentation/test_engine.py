"""Tests for the Feature Engine."""

from pathlib import Path

from mvp.experimentation.engine import FeatureEngine


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
