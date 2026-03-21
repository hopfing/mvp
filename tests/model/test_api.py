"""Tests for public API exports."""


class TestPublicAPI:
    """Tests for model module public API."""

    def test_feature_engine_exported(self):
        """FeatureEngine is exported from model module."""
        from mvp.model import FeatureEngine

        assert FeatureEngine is not None

    def test_feature_decorator_exported(self):
        """feature decorator is exported from model module."""
        from mvp.model import feature

        assert callable(feature)

    def test_get_registry_exported(self):
        """get_registry is exported from model module."""
        from mvp.model import get_registry

        registry = get_registry()
        assert hasattr(registry, "list_features")

    def test_diagnostics_exported(self):
        """Diagnostics is exported from model module."""
        from mvp.model import Diagnostics

        diag = Diagnostics()
        assert hasattr(diag, "compute_all")

    def test_diagnostic_results_exported(self):
        """DiagnosticResults is exported from model module."""
        from mvp.model import DiagnosticResults

        result = DiagnosticResults(
            segments={}, calibration={}, errors={}, temporal={}
        )
        assert hasattr(result, "metrics")
        assert hasattr(result, "to_json")

    def test_all_exports_in_dunder_all(self):
        """All public exports are listed in __all__."""
        import mvp.model as exp

        assert hasattr(exp, "__all__")
        expected = [
            "BaseSplitter",
            "BaseModel",
            "compute_metrics",
            "DiagnosticResults",
            "Diagnostics",
            "ExpandingWindowSplitter",
            "ExperimentConfig",
            "ExperimentLogger",
            "ExperimentRunner",
            "feature",
            "FeatureEngine",
            "get_model",
            "get_registry",
            "LogisticModel",
            "SlidingWindowSplitter",
            "WalkForwardSplitter",
            "XGBoostModel",
        ]
        assert set(exp.__all__) == set(expected)
