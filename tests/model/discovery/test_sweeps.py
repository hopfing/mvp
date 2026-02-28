"""Tests for parameter sweep functionality."""

import pytest

from mvp.model.discovery.sweeps import (
    ParameterSweep,
    SweepResult,
    build_feature_spec,
    parse_feature_spec,
)


class TestParseFeatureSpec:
    """Tests for parse_feature_spec."""

    def test_parses_feature_with_int_param(self):
        """Should parse integer parameters."""
        name, params = parse_feature_spec("win_rate(window_days=30)")
        assert name == "win_rate"
        assert params == {"window_days": 30}

    def test_parses_feature_with_multiple_params(self):
        """Should parse multiple parameters."""
        name, params = parse_feature_spec("ranking_ratio(capped=True, cap=10)")
        assert name == "ranking_ratio"
        assert params == {"capped": True, "cap": 10}

    def test_parses_feature_with_no_params(self):
        """Should parse features with empty parens."""
        name, params = parse_feature_spec("h2h_record()")
        assert name == "h2h_record"
        assert params == {}

    def test_parses_feature_without_parens(self):
        """Should handle features without parentheses."""
        name, params = parse_feature_spec("simple_feature")
        assert name == "simple_feature"
        assert params == {}

    def test_parses_boolean_true(self):
        """Should parse boolean True."""
        name, params = parse_feature_spec("feature(flag=True)")
        assert params["flag"] is True

    def test_parses_boolean_false(self):
        """Should parse boolean False."""
        name, params = parse_feature_spec("feature(flag=false)")
        assert params["flag"] is False

    def test_parses_float_param(self):
        """Should parse float parameters."""
        name, params = parse_feature_spec("feature(threshold=0.5)")
        assert params["threshold"] == 0.5


class TestBuildFeatureSpec:
    """Tests for build_feature_spec."""

    def test_builds_spec_with_params(self):
        """Should build spec with parameters."""
        spec = build_feature_spec("win_rate", {"window_days": 30})
        assert spec == "win_rate(window_days=30)"

    def test_builds_spec_with_multiple_params(self):
        """Should build spec with sorted parameters."""
        spec = build_feature_spec("ranking_ratio", {"cap": 10, "capped": True})
        # Parameters should be sorted alphabetically
        assert spec == "ranking_ratio(cap=10, capped=True)"

    def test_builds_spec_with_no_params(self):
        """Should build spec with empty parens."""
        spec = build_feature_spec("h2h_record", {})
        assert spec == "h2h_record()"

    def test_roundtrip(self):
        """parse -> build should return equivalent spec."""
        original = "win_rate(window_days=30)"
        name, params = parse_feature_spec(original)
        rebuilt = build_feature_spec(name, params)
        assert rebuilt == original


class TestParameterSweepGeneration:
    """Tests for parameter combination generation."""

    def test_generates_single_param_combinations(self, tmp_path):
        """Should generate all values for single parameter."""
        sweep = ParameterSweep(
            base_config_path=tmp_path / "config.yaml",
            sweep_params={"win_rate": {"window_days": [7, 14, 30]}},
        )

        combinations = sweep._generate_combinations()

        assert len(combinations) == 3
        assert {"win_rate": {"window_days": 7}} in combinations
        assert {"win_rate": {"window_days": 14}} in combinations
        assert {"win_rate": {"window_days": 30}} in combinations

    def test_generates_cartesian_product(self, tmp_path):
        """Should generate cartesian product of all params."""
        sweep = ParameterSweep(
            base_config_path=tmp_path / "config.yaml",
            sweep_params={
                "win_rate": {"window_days": [7, 30]},
                "h2h": {"min_matches": [3, 5]},
            },
        )

        combinations = sweep._generate_combinations()

        # 2 x 2 = 4 combinations
        assert len(combinations) == 4

    def test_respects_max_combinations(self, tmp_path):
        """Should limit combinations when max_combinations set."""
        sweep = ParameterSweep(
            base_config_path=tmp_path / "config.yaml",
            sweep_params={"win_rate": {"window_days": [7, 14, 30, 60, 90]}},
            max_combinations=3,
        )

        combinations = sweep._generate_combinations()

        assert len(combinations) == 3

    def test_empty_sweep_params(self, tmp_path):
        """Should return single empty dict for no sweep params."""
        sweep = ParameterSweep(
            base_config_path=tmp_path / "config.yaml",
            sweep_params={},
        )

        combinations = sweep._generate_combinations()

        assert combinations == [{}]


class TestParameterSweepApply:
    """Tests for applying parameters to features."""

    def test_applies_params_to_matching_feature(self, tmp_path):
        """Should update params for matching feature."""
        sweep = ParameterSweep(
            base_config_path=tmp_path / "config.yaml",
            sweep_params={},
        )

        features = ["win_rate(window_days=30)", "h2h_record()"]
        param_combo = {"win_rate": {"window_days": 60}}

        result = sweep._apply_params(features, param_combo)

        assert result[0] == "win_rate(window_days=60)"
        assert result[1] == "h2h_record()"

    def test_preserves_unmodified_features(self, tmp_path):
        """Should not modify features not in param_combo."""
        sweep = ParameterSweep(
            base_config_path=tmp_path / "config.yaml",
            sweep_params={},
        )

        features = ["win_rate(window_days=30)", "ranking_ratio(cap=10)"]
        param_combo = {"win_rate": {"window_days": 60}}

        result = sweep._apply_params(features, param_combo)

        assert result[1] == "ranking_ratio(cap=10)"

    def test_merges_multiple_params(self, tmp_path):
        """Should merge new params with existing."""
        sweep = ParameterSweep(
            base_config_path=tmp_path / "config.yaml",
            sweep_params={},
        )

        features = ["ranking_ratio(capped=True, cap=10)"]
        param_combo = {"ranking_ratio": {"cap": 15}}

        result = sweep._apply_params(features, param_combo)

        # Should update cap but keep capped
        name, params = parse_feature_spec(result[0])
        assert params["cap"] == 15
        assert params["capped"] is True


class TestSweepResult:
    """Tests for SweepResult dataclass."""

    def test_holds_results(self):
        """Should store all result fields."""
        result = SweepResult(
            best_params={"win_rate": {"window_days": 30}},
            best_metric=0.042,
            all_results=[
                {"params": {"win_rate": {"window_days": 7}}, "metric": 0.051},
                {"params": {"win_rate": {"window_days": 30}}, "metric": 0.042},
            ],
            n_combinations=2,
        )

        assert result.best_params == {"win_rate": {"window_days": 30}}
        assert result.best_metric == 0.042
        assert len(result.all_results) == 2
        assert result.n_combinations == 2
