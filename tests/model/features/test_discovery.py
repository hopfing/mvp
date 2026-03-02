"""Tests for feature auto-discovery."""

import pytest

from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_discovered():
    """Import features package to trigger auto-discovery."""
    # Import the features package to trigger auto-discovery
    import mvp.model.features  # noqa: F401

    yield


class TestFeatureDiscovery:
    """Tests for feature auto-discovery."""

    def test_all_features_registered_on_import(self):
        """All feature modules are auto-imported and features registered."""
        registry = get_registry()
        features = registry.list_features()

        # Check that all starter features are registered
        expected_features = [
            "win_pct",
            "matches_played",
            "win_pct_diff",
            "h2h_wins",
            "ranking_points_diff",
            "svc_first_serve_win_pct",
        ]

        for feat_name in expected_features:
            assert feat_name in features, f"Feature '{feat_name}' not found in registry"

    def test_registry_accessible_from_features_package(self):
        """get_registry is accessible from features package."""
        from mvp.model.features import get_registry as features_get_registry

        # Should be the same registry instance
        assert features_get_registry is get_registry

    def test_feature_count_minimum(self):
        """At least 6 features are registered."""
        registry = get_registry()
        assert len(registry.list_features()) >= 6
