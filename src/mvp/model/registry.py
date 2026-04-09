"""Feature registry for experiment platform.

Features are registered via the @feature decorator and can be retrieved
by name for computation.
"""


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
    match_level: bool = False  # Whether this is a match-level feature (no prefix)
    impute: float | str = "median"  # Imputation strategy: "median" or numeric constant


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
    match_level: bool = False,
    impute: float | str = "median",
) -> Callable[[Callable[..., pl.Expr]], Callable[..., pl.Expr]]:
    """Decorator to register a feature function.

    Args:
        name: Unique feature name (e.g., "win_rate").
        params: Parameter names the feature accepts (e.g., ["days"]).
        description: Human-readable description.
        depends_on: Names of features that must be computed first.
        mirror: Whether to auto-generate opp_* column (default True).
        match_level: Whether this is a match-level feature with no prefix (default False).
        impute: Imputation strategy — "median" (default) or a numeric constant.

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
            match_level=match_level,
            impute=impute,
        )
        get_registry().register(feature_def)
        return func

    return decorator


def register_diff(base_name: str) -> None:
    """Register a diff feature (player - opponent) for a base feature.

    Infers whether the diff is windowed from the base feature's params.
    Must be called after the base feature is registered.
    """
    base = get_registry().get(base_name)
    has_days = "days" in base.params
    diff_name = f"{base_name}_diff"

    @feature(
        name=diff_name,
        params=["days"] if has_days else [],
        description=f"{base_name} difference (player - opponent)",
        depends_on=[base_name],
        mirror=False,
        impute=0,
    )
    def _diff(days: int | None = None, _bn: str = base_name) -> pl.Expr:
        if days is None:
            return pl.col(f"player_{_bn}") - pl.col(f"opp_{_bn}")
        return pl.col(f"player_{_bn}_{days}d") - pl.col(f"opp_{_bn}_{days}d")


def register_matchup(
    name: str,
    player_col: str,
    opp_col: str,
    dep1: str,
    dep2: str,
    description: str = "",
) -> None:
    """Register a cross-domain matchup feature (player_A - opp_B).

    Infers whether the matchup is windowed from dep1's params.
    Must be called after both dependency features are registered.
    """
    base = get_registry().get(dep1)
    has_days = "days" in base.params

    @feature(
        name=name,
        params=["days"] if has_days else [],
        description=description,
        depends_on=[dep1, dep2],
        mirror=True,
        impute=0,
    )
    def _matchup(
        days: int | None = None, _pc: str = player_col, _oc: str = opp_col,
    ) -> pl.Expr:
        if days is None:
            return pl.col(_pc) - pl.col(_oc)
        return pl.col(f"{_pc}_{days}d") - pl.col(f"{_oc}_{days}d")


def register_sum(base_name: str, description: str = "") -> None:
    """Register a sum feature (player + opponent) for a base feature.

    Creates a match-level feature representing the combined value.
    Useful for projection: combined serve dominance, combined tightness, etc.

    Infers whether the sum is windowed from the base feature's params.
    Must be called after the base feature is registered.
    """
    base = get_registry().get(base_name)
    has_days = "days" in base.params
    sum_name = f"{base_name}_sum"

    @feature(
        name=sum_name,
        params=["days"] if has_days else [],
        description=description or f"{base_name} sum (player + opponent)",
        depends_on=[base_name],
        mirror=False,
        match_level=True,
        impute="median",
    )
    def _sum(days: int | None = None, _bn: str = base_name) -> pl.Expr:
        if days is None:
            return pl.col(f"player_{_bn}") + pl.col(f"opp_{_bn}")
        return pl.col(f"player_{_bn}_{days}d") + pl.col(f"opp_{_bn}_{days}d")
