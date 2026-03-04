"""Tests for playing style feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import style as style_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield
