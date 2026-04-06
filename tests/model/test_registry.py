"""Tests for feature registry."""

import polars as pl
import pytest

from mvp.model.registry import feature, get_registry, register_sum


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

    def test_decorator_stores_impute_default(self, isolated_registry):
        """Default impute is 'median'."""
        @feature(name="test_impute_default")
        def f() -> pl.Expr:
            return pl.lit(1)

        feat = isolated_registry.get("test_impute_default")
        assert feat.impute == "median"

    def test_decorator_stores_impute_constant(self, isolated_registry):
        """Impute can be a numeric constant."""
        @feature(name="test_impute_const", impute=0.5)
        def f() -> pl.Expr:
            return pl.lit(1)

        feat = isolated_registry.get("test_impute_const")
        assert feat.impute == 0.5

    def test_decorator_stores_impute_zero(self, isolated_registry):
        """Impute=0 is distinct from default."""
        @feature(name="test_impute_zero", impute=0)
        def f() -> pl.Expr:
            return pl.lit(1)

        feat = isolated_registry.get("test_impute_zero")
        assert feat.impute == 0


class TestRegisterSum:
    """Tests for register_sum helper."""

    def test_creates_feature_with_correct_metadata(self, isolated_registry):
        """register_sum creates a match-level sum feature."""
        @feature(name="base_feat", params=["days"], mirror=True)
        def base_func(days=None):
            return pl.lit(1)

        register_sum("base_feat")
        feat = isolated_registry.get("base_feat_sum")
        assert feat.match_level is True
        assert feat.mirror is False
        assert feat.depends_on == ["base_feat"]
        assert feat.impute == "median"
        assert "days" in feat.params

    def test_no_days_param(self, isolated_registry):
        """register_sum works for features without days param."""
        @feature(name="static_feat", params=[])
        def static_func():
            return pl.lit(1)

        register_sum("static_feat")
        feat = isolated_registry.get("static_feat_sum")
        assert feat.params == []

    def test_expression_unwindowed(self, isolated_registry):
        """Sum expression adds player + opp columns."""
        @feature(name="test_base", params=["days"], mirror=True)
        def base_func(days=None):
            return pl.lit(1)

        register_sum("test_base")
        feat = isolated_registry.get("test_base_sum")

        expr = feat.func()
        df = pl.DataFrame({
            "player_test_base": [0.6, 0.7],
            "opp_test_base": [0.5, 0.4],
        })
        result = df.select(expr.alias("sum")).to_series()
        assert result[0] == pytest.approx(1.1)
        assert result[1] == pytest.approx(1.1)

    def test_expression_windowed(self, isolated_registry):
        """Windowed sum expression uses _Nd suffixed columns."""
        @feature(name="test_w", params=["days"], mirror=True)
        def base_func(days=None):
            return pl.lit(1)

        register_sum("test_w")
        feat = isolated_registry.get("test_w_sum")

        expr = feat.func(days=90)
        df = pl.DataFrame({
            "player_test_w_90d": [0.65, 0.55],
            "opp_test_w_90d": [0.60, 0.45],
        })
        result = df.select(expr.alias("sum")).to_series()
        assert result[0] == pytest.approx(1.25)
        assert result[1] == pytest.approx(1.0)

    def test_custom_description(self, isolated_registry):
        """Custom description overrides the default."""
        @feature(name="desc_feat", params=[])
        def desc_func():
            return pl.lit(1)

        register_sum("desc_feat", description="Combined serve dominance")
        feat = isolated_registry.get("desc_feat_sum")
        assert feat.description == "Combined serve dominance"
