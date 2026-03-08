"""Tests for feature registry."""

import polars as pl

from mvp.model.registry import feature, get_registry


class TestFeatureDecorator:
    """Tests for @feature decorator."""

    def test_decorator_registers_feature(self, isolated_registry):
        """Decorated function is registered."""
        @feature(name="test_feature", params=["days"])
        def test_feature_func(ctx, days: int) -> pl.Expr:
            return pl.lit(1)

        assert "test_feature" in isolated_registry.list_features()

    def test_decorator_stores_metadata(self, isolated_registry):
        """Decorator stores name, params, description."""
        @feature(
            name="test_meta",
            params=["days", "cap"],
            description="A test feature",
        )
        def test_meta_func(ctx, days: int, cap: float = 10.0) -> pl.Expr:
            return pl.lit(1)

        feat = isolated_registry.get("test_meta")
        assert feat.name == "test_meta"
        assert feat.params == ["days", "cap"]
        assert feat.description == "A test feature"

    def test_decorator_stores_dependencies(self, isolated_registry):
        """Decorator stores depends_on."""
        @feature(name="base_feature")
        def base_func(ctx) -> pl.Expr:
            return pl.lit(1)

        @feature(name="derived_feature", depends_on=["base_feature"])
        def derived_func(ctx) -> pl.Expr:
            return pl.lit(2)

        feat = isolated_registry.get("derived_feature")
        assert feat.depends_on == ["base_feature"]
