"""Names for an earlier model's out-of-sample log-odds column.

Import-free on purpose: the config modules and the predictor need these
without pulling in the feature registry (``features/prior.py`` owns the
transform and imports this too).
"""

from __future__ import annotations

import re

_SPEC_RE = re.compile(r"^player_prior_logit\(model=([^)]+)\)$")
_COL_PREFIX = "player_prior_logit_"


def prior_spec(model: str) -> str:
    """Feature spec of ``model``'s log-odds: ``player_prior_logit(model=<m>)``."""
    return f"player_prior_logit(model={model})"


def prior_column(model: str) -> str:
    """Engine column the spec resolves to (non-window params are joined with
    ``_`` by ``build_column_name``)."""
    return f"{_COL_PREFIX}{model}"


def prior_model_of(feature: str) -> str | None:
    """The ``model`` a prior spec or column names, else None."""
    m = _SPEC_RE.match(feature.strip())
    if m:
        return m.group(1).strip().strip("'\"")
    if feature.startswith(_COL_PREFIX):
        return feature[len(_COL_PREFIX):]
    return None
