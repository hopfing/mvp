"""Feature definitions for the model platform.

This module auto-discovers and imports all feature modules in this package,
which registers the features via decorators.
"""

# Import all feature modules to trigger registration
from mvp.model.features import (
    context,
    elo,
    form,
    h2h,
    points,
    ranking,
    returns,
    serve,
    static,
    surface,
    tiebreak,
    win_rate,
)
from mvp.model.registry import get_registry

__all__ = [
    "get_registry",
    "context",
    "elo",
    "form",
    "h2h",
    "points",
    "ranking",
    "returns",
    "serve",
    "static",
    "surface",
    "tiebreak",
    "win_rate",
]
