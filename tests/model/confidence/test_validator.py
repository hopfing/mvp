"""Tests for confidence validator."""

import numpy as np
import polars as pl
import pytest

from mvp.model.confidence.validator import prepare_oof
from mvp.model.confidence.validator import ConfidenceValidator, ValidationResult


class TestPrepareOof:
    def test_concatenates_folds(self, make_oof_df):
        df1 = make_oof_df(n=100, seed=1)
        df2 = make_oof_df(n=100, seed=2)
        all_predictions = [
            {"df": df1.drop("y_true", "y_prob"), "y_true": df1["y_true"].to_numpy(), "y_prob": df1["y_prob"].to_numpy()},
            {"df": df2.drop("y_true", "y_prob"), "y_true": df2["y_true"].to_numpy(), "y_prob": df2["y_prob"].to_numpy()},
        ]
        result = prepare_oof(all_predictions)
        assert len(result) == 200

    def test_adds_favored_columns(self, make_oof_df):
        df = make_oof_df(n=100)
        all_predictions = [
            {"df": df.drop("y_true", "y_prob"), "y_true": df["y_true"].to_numpy(), "y_prob": df["y_prob"].to_numpy()},
        ]
        result = prepare_oof(all_predictions)
        assert "favored_prob" in result.columns
        assert "favored_won" in result.columns
        assert result["favored_prob"].min() >= 0.5

    def test_favored_orientation_above_half(self):
        df = pl.DataFrame({
            "match_uid": ["a"], "effective_match_date": [None],
            "circuit": ["chal"], "surface": ["hard"], "round": ["R32"],
            "player_elo": [1600.0], "opp_elo": [1400.0],
            "player_elo_rd": [50.0], "opp_elo_rd": [50.0],
            "player_rank": [50], "opp_rank": [100],
            "player_birth_date": [None], "opp_birth_date": [None],
        })
        y_prob = np.array([0.7])
        y_true = np.array([1])
        result = prepare_oof([{"df": df, "y_true": y_true, "y_prob": y_prob}])
        assert result["favored_prob"][0] == pytest.approx(0.7)
        assert result["favored_won"][0] == 1

    def test_favored_orientation_below_half(self):
        df = pl.DataFrame({
            "match_uid": ["a"], "effective_match_date": [None],
            "circuit": ["chal"], "surface": ["hard"], "round": ["R32"],
            "player_elo": [1400.0], "opp_elo": [1600.0],
            "player_elo_rd": [50.0], "opp_elo_rd": [50.0],
            "player_rank": [100], "opp_rank": [50],
            "player_birth_date": [None], "opp_birth_date": [None],
        })
        y_prob = np.array([0.3])
        y_true = np.array([0])
        result = prepare_oof([{"df": df, "y_true": y_true, "y_prob": y_prob}])
        assert result["favored_prob"][0] == pytest.approx(0.7)
        assert result["favored_won"][0] == 1

    def test_adds_prob_bucket(self, make_oof_df):
        df = make_oof_df(n=500)
        all_predictions = [
            {"df": df.drop("y_true", "y_prob"), "y_true": df["y_true"].to_numpy(), "y_prob": df["y_prob"].to_numpy()},
        ]
        result = prepare_oof(all_predictions)
        assert "prob_bucket" in result.columns
        buckets = result["prob_bucket"].unique().sort().to_list()
        for b in buckets:
            assert b.endswith("%")


class TestEndToEnd:
    def test_full_pipeline_produces_report(self, make_oof_df):
        """Full pipeline: prepare -> validate -> format report."""
        from mvp.model.confidence.report import format_report

        df = make_oof_df(n=5000, cal_bias=0.02)
        all_predictions = [{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }]

        validator = ConfidenceValidator(all_predictions)
        result = validator.validate()
        report = format_report(result, model_name="test_underconfident")

        assert "underconfident" in report.lower()
        assert "STRUCTURAL" in report
        assert "MODIFIER" in report
        overall = result.profiles["overall"]["overall"]
        assert overall.signed_cal > 0

    def test_overconfident_model_detected(self, make_oof_df):
        """Overconfident model shows negative signed calibration."""
        df = make_oof_df(n=5000, cal_bias=-0.03)
        all_predictions = [{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }]

        validator = ConfidenceValidator(all_predictions)
        result = validator.validate()

        overall = result.profiles["overall"]["overall"]
        assert overall.signed_cal < 0


class TestConfidenceValidator:
    def test_validate_returns_result(self, make_oof_df):
        df = make_oof_df(n=2000)
        all_predictions = [{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }]
        validator = ConfidenceValidator(all_predictions)
        result = validator.validate()
        assert isinstance(result, ValidationResult)

    def test_result_has_structural_profiles(self, make_oof_df):
        df = make_oof_df(n=2000)
        all_predictions = [{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }]
        validator = ConfidenceValidator(all_predictions)
        result = validator.validate()
        structural_keys = [k for k in result.profiles.keys() if k.startswith("circuit:")]
        assert len(structural_keys) >= 2

    def test_result_has_modifier_profiles(self, make_oof_df):
        df = make_oof_df(n=2000)
        all_predictions = [{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }]
        validator = ConfidenceValidator(all_predictions)
        result = validator.validate()
        modifier_keys = [k for k in result.profiles.keys() if k.startswith("elo_level:")]
        assert len(modifier_keys) == 5

    def test_profiles_have_bucket_breakdown(self, make_oof_df):
        df = make_oof_df(n=2000)
        all_predictions = [{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }]
        validator = ConfidenceValidator(all_predictions)
        result = validator.validate()
        chal_profiles = result.profiles.get("circuit:chal")
        assert chal_profiles is not None
        assert "overall" in chal_profiles
        bucket_keys = [k for k in chal_profiles if k != "overall"]
        assert len(bucket_keys) > 0
