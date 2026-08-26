"""`offset.prior: <stem>` sugar on both config classes: expands to the prior
spec, pins the column, and restricts rows to where it exists."""

import pytest

from mvp.model.config import ExperimentConfig
from mvp.model.discovery.config import DiscoveryConfig

_STEM = "stage1_lead_residual__h19_t218"
_SPEC = f"player_prior_logit(model={_STEM})"
_COL = f"player_prior_logit_{_STEM}"


def _experiment(offset: dict) -> dict:
    return {
        "data": {
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "filters": {"draw_type": "singles"},
        },
        "features": {"include": ["player_elo_surface_diff"]},
        "model": {"type": "xgboost", "params": {"n_estimators": 5}},
        "offset": offset,
        "target": "won",
    }


def _discovery(offset: dict) -> dict:
    return {
        "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "model": {"type": "xgboost"},
        "validation": {"type": "walk_forward", "n_splits": 2},
        "discovery": {"features": {"base": []}},
        "offset": offset,
    }


class TestExperimentConfig:
    def test_prior_expands_pins_and_filters(self):
        cfg = ExperimentConfig.model_validate(_experiment({"prior": _STEM}))
        assert cfg.offset.feature == _SPEC
        assert cfg.offset.prior == _STEM
        assert _SPEC in cfg.features.include
        assert cfg.data.filters == {"draw_type": "singles", _COL: "not_null"}

    def test_explicit_feature_is_untouched(self):
        cfg = ExperimentConfig.model_validate(
            {**_experiment({"feature": "player_elo_surface_diff"})}
        )
        assert cfg.offset.prior is None
        assert cfg.data.filters == {"draw_type": "singles"}

    def test_exactly_one_of_feature_or_prior(self):
        with pytest.raises(ValueError, match="exactly one"):
            ExperimentConfig.model_validate(_experiment({}))
        with pytest.raises(ValueError, match="exactly one"):
            ExperimentConfig.model_validate(
                _experiment({"prior": _STEM, "feature": "player_elo_surface_diff"})
            )


class TestDiscoveryConfig:
    def test_prior_seeds_base_and_filters(self):
        cfg = DiscoveryConfig.model_validate(_discovery({"prior": _STEM}))
        assert cfg.offset.feature == _SPEC
        assert cfg.discovery.features.base == [_SPEC]
        assert cfg.data.filters == {_COL: "not_null"}

    def test_explicit_feature_still_requires_the_seed(self):
        with pytest.raises(ValueError, match="discovery.features.base"):
            DiscoveryConfig.model_validate(
                _discovery({"feature": "player_elo_surface_diff"})
            )
