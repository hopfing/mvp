"""Shared fixtures for model tests."""

import importlib

import pytest

from mvp.model.registry import get_registry


def reload_all_feature_modules() -> None:
    """Reload all feature modules to re-register features in the global registry."""
    import mvp.model.features.context
    import mvp.model.features.elo
    import mvp.model.features.form
    import mvp.model.features.h2h
    import mvp.model.features.iid
    import mvp.model.features.points
    import mvp.model.features.ranking
    import mvp.model.features.returns
    import mvp.model.features.serve
    import mvp.model.features.static
    import mvp.model.features.style
    import mvp.model.features.surface
    import mvp.model.features.tiebreak
    import mvp.model.features.win_rate

    importlib.reload(mvp.model.features.context)
    importlib.reload(mvp.model.features.elo)
    importlib.reload(mvp.model.features.form)
    importlib.reload(mvp.model.features.h2h)
    importlib.reload(mvp.model.features.iid)
    importlib.reload(mvp.model.features.points)
    importlib.reload(mvp.model.features.ranking)
    importlib.reload(mvp.model.features.returns)
    importlib.reload(mvp.model.features.serve)
    importlib.reload(mvp.model.features.static)
    importlib.reload(mvp.model.features.style)
    importlib.reload(mvp.model.features.surface)
    importlib.reload(mvp.model.features.tiebreak)
    importlib.reload(mvp.model.features.win_rate)


@pytest.fixture
def isolated_registry():
    """Provide a clean registry that restores all features after the test.

    Usage: any test/fixture that needs to call registry.clear() should
    use this fixture instead of manually clearing.
    """
    registry = get_registry()
    saved = dict(registry._features)
    saved_outputs = dict(registry._output_to_feature)
    registry.clear()
    yield registry
    registry.clear()
    registry._features.update(saved)
    registry._output_to_feature.update(saved_outputs)


@pytest.fixture(autouse=True)
def _promoted_priors_in_tmp(tmp_path, monkeypatch):
    """Prior resolution prefers the PROMOTED store when a stem has a dir there
    (mvp.model.prior_promotion). Point it at tmp for every model test so no
    test stem can collide with a real promoted prior on the data root."""
    from mvp.model.features import prior

    monkeypatch.setattr(prior, "PROMOTED_PRIORS_ROOT", tmp_path / "_promoted")
    prior._cached_frame.cache_clear()
    yield
    prior._cached_frame.cache_clear()
