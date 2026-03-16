"""Feature definitions for the model platform.

This module auto-discovers and imports all feature modules in this package,
which registers the features via decorators.
"""

# Import all feature modules to trigger registration
from mvp.model.features import (
    context,
    elo,
    form,
    glicko,
    h2h,
    points,
    quality,
    ranking,
    returns,
    score_depth,
    serve,
    static,
    style,
    surface,
    tiebreak,
    tournament,
    transition,
    win_rate,
)
from mvp.model.registry import get_registry

__all__ = [
    "get_registry",
    "context",
    "elo",
    "form",
    "glicko",
    "h2h",
    "points",
    "quality",
    "ranking",
    "returns",
    "score_depth",
    "serve",
    "static",
    "style",
    "surface",
    "tiebreak",
    "tournament",
    "transition",
    "win_rate",
]
