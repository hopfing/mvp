"""Repo-wide test fixtures."""

import pytest


@pytest.fixture(autouse=True)
def _isolate_artifact_root(tmp_path, monkeypatch):
    """Point the research-artifact root at a per-test temp dir.

    `get_artifact_root()` defaults to <repo>/data/artifacts, which Syncthing
    mirrors to the other dev machine. Without this, any test that records a
    fingerprint, backtest or confidence result writes junk into the real root
    (and ships it to the other machine). Tests that isolate MVP_DATA_ROOT and
    expect artifacts under that same root set MVP_ARTIFACT_ROOT themselves;
    fixtures requested by a test run after autouse ones, so theirs wins.
    """
    monkeypatch.setenv("MVP_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
