"""Feature definitions for the experimentation platform.

This module auto-discovers and imports all feature modules in this package,
which registers the features via decorators.
"""

# Import all feature modules to trigger registration
from mvp.experimentation.features import h2h, ranking, serve, win_rate
from mvp.experimentation.registry import get_registry

__all__ = [
    "get_registry",
    "h2h",
    "ranking",
    "serve",
    "win_rate",
]
