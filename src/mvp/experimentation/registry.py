"""Feature registry for experiment platform.

Features are registered via the @feature decorator and can be retrieved
by name for computation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import polars as pl


@dataclass
class FeatureDef:
    """Definition of a registered feature."""

    name: str
    func: Callable[..., pl.Expr]
    params: list[str] = field(default_factory=list)
    description: str = ""
    depends_on: list[str] = field(default_factory=list)
    mirror: bool = True  # Whether to generate opp_* column


class FeatureRegistry:
    """Central registry of all features."""

    def __init__(self) -> None:
        self._features: dict[str, FeatureDef] = {}

    def register(self, feature_def: FeatureDef) -> None:
        """Register a feature definition."""
        if feature_def.name in self._features:
            raise ValueError(f"Feature '{feature_def.name}' already registered")
        self._features[feature_def.name] = feature_def

    def get(self, name: str) -> FeatureDef:
        """Get a feature by name."""
        if name not in self._features:
            raise KeyError(f"Feature '{name}' not found")
        return self._features[name]

    def list_features(self) -> list[str]:
        """List all registered feature names."""
        return list(self._features.keys())

    def clear(self) -> None:
        """Clear all registered features. For testing."""
        self._features.clear()


# Global singleton registry
_registry: FeatureRegistry | None = None


def get_registry() -> FeatureRegistry:
    """Get the global feature registry singleton."""
    global _registry
    if _registry is None:
        _registry = FeatureRegistry()
    return _registry


def feature(
    name: str,
    params: list[str] | None = None,
    description: str = "",
    depends_on: list[str] | None = None,
    mirror: bool = True,
) -> Callable[[Callable[..., pl.Expr]], Callable[..., pl.Expr]]:
    """Decorator to register a feature function.

    Args:
        name: Unique feature name (e.g., "win_rate").
        params: Parameter names the feature accepts (e.g., ["days"]).
        description: Human-readable description.
        depends_on: Names of features that must be computed first.
        mirror: Whether to auto-generate opp_* column (default True).

    Returns:
        Decorator function.
    """

    def decorator(func: Callable[..., pl.Expr]) -> Callable[..., pl.Expr]:
        feature_def = FeatureDef(
            name=name,
            func=func,
            params=params or [],
            description=description,
            depends_on=depends_on or [],
            mirror=mirror,
        )
        get_registry().register(feature_def)
        return func

    return decorator
