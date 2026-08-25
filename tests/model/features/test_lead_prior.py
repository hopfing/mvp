"""Tests for the lead-prior joinable transform."""

import polars as pl
import pytest

from mvp.model.features import lead_prior
from mvp.model.registry import get_registry


@pytest.fixture
def prior_file(tmp_path, monkeypatch):
    path = tmp_path / "lead_prior.parquet"
    pl.DataFrame({
        "match_uid": ["M1", "M1", "M2", "M2"],
        "player_id": ["A", "B", "C", "D"],
        "lead_prob": [0.7, 0.3, 0.55, 0.45],
        "lead_logit": [0.8473, -0.8473, 0.2007, -0.2007],
    }).write_parquet(path)
    monkeypatch.setattr(lead_prior, "prior_path", lambda: path)
    return path


def test_joins_on_match_and_player_and_leaves_unscored_null(prior_file):
    df = pl.DataFrame({
        "match_uid": ["M1", "M1", "M3"],
        "player_id": ["A", "B", "Z"],
        "won": [True, False, None],
    })
    out = lead_prior._lead_prior_transform(df)
    assert out.columns == [
        "match_uid", "player_id", "player_lead_prob", "player_lead_logit",
    ]
    assert out["player_lead_logit"].to_list()[:2] == pytest.approx([0.8473, -0.8473])
    assert out["player_lead_logit"][2] is None
    assert out["player_lead_prob"][2] is None


def test_missing_parquet_names_the_builder(tmp_path, monkeypatch):
    monkeypatch.setattr(lead_prior, "prior_path", lambda: tmp_path / "nope.parquet")
    with pytest.raises(FileNotFoundError, match="build_lead_prior.py"):
        lead_prior._lead_prior_transform(
            pl.DataFrame({"match_uid": [], "player_id": []})
        )


def test_registered_as_transform_for_its_outputs():
    reg = get_registry()
    for col in ("player_lead_prob", "player_lead_logit"):
        t = reg.transform_for_output(col)
        assert t is not None and t.name == "lead_prior"
