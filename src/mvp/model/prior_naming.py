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


# The chain transforms keyed by a projection stem. Each is spelled either as
# the transform (`chain_arm(model=X)`), as one of its output specs
# (`player_chain_w1_logit(model=X)`) or as an engine column
# (`player_chain_w1_logit_X`); a config lists them any of those ways, and
# promotion and serving must see the dependency whichever it uses.
ARM_VALUES = ("chain_fi_rate", "chain_w1_logit", "chain_w2_logit")
ARM_OUTPUTS = tuple(f"{side}_{v}" for side in ("player", "opp") for v in ARM_VALUES)
_TRANSFORM_RE = re.compile(r"^(chain_arm|chain_shape)\(model=([^)]+)\)$")
_OUTPUT_SPEC_RE = re.compile(r"^(\w+)\(model=([^)]+)\)$")


def _shape_outputs() -> tuple[str, ...]:
    # Only `player_` shape outputs are registered, so an `opp_` spelling can
    # never reach a frame and is not recognised.
    from mvp.common.chain_shape import SHAPE_COLUMNS

    return tuple(f"player_{c}" for c in SHAPE_COLUMNS)


def _kind_of_output(name: str) -> str | None:
    if name in ARM_OUTPUTS:
        return "chain_arm"
    if name in _shape_outputs():
        return "chain_shape"
    return None


def prior_kind_of(spec: str) -> tuple[str, str] | None:
    """(kind, stem) for any spelling through which a projection or model stem
    enters a config: kind "prior" (`player_prior_logit`), "chain_arm" or
    "chain_shape"; None for any other spec or column."""
    stem = prior_model_of(spec)
    if stem is not None:
        return "prior", stem
    s = spec.strip()
    m = _TRANSFORM_RE.match(s)
    if m:
        return m.group(1), m.group(2).strip().strip("'\"")
    m = _OUTPUT_SPEC_RE.match(s)
    if m:
        kind = _kind_of_output(m.group(1))
        return (kind, m.group(2).strip().strip("'\"")) if kind else None
    # Engine column: the longest output name that prefixes it wins, so no
    # output's name can swallow a longer one's.
    for name in sorted((*ARM_OUTPUTS, *_shape_outputs()), key=len, reverse=True):
        if s.startswith(f"{name}_") and len(s) > len(name) + 1:
            return _kind_of_output(name), s[len(name) + 1:]  # type: ignore[return-value]
    return None


def prior_stem_of(spec: str) -> str | None:
    """The stem half of `prior_kind_of`."""
    found = prior_kind_of(spec)
    return found[1] if found else None
