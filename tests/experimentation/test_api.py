"""Tests for public API exports."""


class TestPublicAPI:
    """Tests for experimentation module public API."""

    def test_feature_context_exported(self):
        """FeatureContext is exported from experimentation module."""
        from mvp.experimentation import FeatureContext

        # Should be able to instantiate
        ctx = FeatureContext()
        assert ctx.group_by == "player_id"

    def test_feature_engine_exported(self):
        """FeatureEngine is exported from experimentation module."""
        from mvp.experimentation import FeatureEngine

        assert FeatureEngine is not None

    def test_feature_decorator_exported(self):
        """feature decorator is exported from experimentation module."""
        from mvp.experimentation import feature

        assert callable(feature)

    def test_get_registry_exported(self):
        """get_registry is exported from experimentation module."""
        from mvp.experimentation import get_registry

        registry = get_registry()
        assert hasattr(registry, "list_features")

    def test_all_exports_in_dunder_all(self):
        """All public exports are listed in __all__."""
        import mvp.experimentation as exp

        assert hasattr(exp, "__all__")
        expected = ["FeatureContext", "FeatureEngine", "feature", "get_registry"]
        assert set(exp.__all__) == set(expected)
