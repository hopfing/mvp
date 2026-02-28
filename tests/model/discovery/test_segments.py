"""Tests for segment analysis functionality."""

import numpy as np
import polars as pl
import pytest

from mvp.model.discovery.segments import (
    SegmentAnalyzer,
    SegmentImportanceResult,
    SplitComparisonResult,
    compute_segment_importance,
)
from mvp.model.models import XGBoostModel


@pytest.fixture
def sample_data_with_segments():
    """Generate sample data with segment column."""
    np.random.seed(42)
    n_samples = 600

    # Features
    x1 = np.random.randn(n_samples)
    x2 = np.random.randn(n_samples)
    X = np.column_stack([x1, x2])

    # Target
    y = (x1 + 0.5 * x2 + np.random.randn(n_samples) * 0.5 > 0).astype(int)

    # Segment column - 2/3 "tour", 1/3 "chal"
    circuit = ["tour"] * 400 + ["chal"] * 200
    df = pl.DataFrame({
        "circuit": circuit,
        "surface": ["Hard"] * 300 + ["Clay"] * 200 + ["Grass"] * 100,
    })

    feature_names = ["feature_1", "feature_2"]

    return X, y, df, feature_names


@pytest.fixture
def trained_model(sample_data_with_segments):
    """Train model on sample data."""
    X, y, _, _ = sample_data_with_segments
    model = XGBoostModel({"n_estimators": 30, "max_depth": 3})
    model.fit(X, y)
    return model


class TestComputeSegmentImportance:
    """Tests for compute_segment_importance."""

    def test_returns_importance_result(
        self, trained_model, sample_data_with_segments
    ):
        """Should return SegmentImportanceResult."""
        X, y, df, feature_names = sample_data_with_segments

        result = compute_segment_importance(
            model=trained_model,
            X=X,
            y=y,
            df=df,
            feature_names=feature_names,
            segment_column="circuit",
            method="gain",
        )

        assert isinstance(result, SegmentImportanceResult)
        assert result.segment_column == "circuit"

    def test_computes_overall_importance(
        self, trained_model, sample_data_with_segments
    ):
        """Should include overall importance."""
        X, y, df, feature_names = sample_data_with_segments

        result = compute_segment_importance(
            model=trained_model,
            X=X,
            y=y,
            df=df,
            feature_names=feature_names,
            segment_column="circuit",
            method="gain",
        )

        assert "feature_1" in result.overall_importance
        assert "feature_2" in result.overall_importance

    def test_computes_per_segment_importance(
        self, trained_model, sample_data_with_segments
    ):
        """Should compute importance for each segment."""
        X, y, df, feature_names = sample_data_with_segments

        result = compute_segment_importance(
            model=trained_model,
            X=X,
            y=y,
            df=df,
            feature_names=feature_names,
            segment_column="circuit",
            method="gain",
        )

        assert "tour" in result.importance_by_segment
        assert "chal" in result.importance_by_segment

    def test_raises_for_missing_column(
        self, trained_model, sample_data_with_segments
    ):
        """Should raise if segment column not in DataFrame."""
        X, y, df, feature_names = sample_data_with_segments

        with pytest.raises(ValueError, match="not in DataFrame"):
            compute_segment_importance(
                model=trained_model,
                X=X,
                y=y,
                df=df,
                feature_names=feature_names,
                segment_column="nonexistent",
                method="gain",
            )

    def test_skips_small_segments(
        self, trained_model, sample_data_with_segments
    ):
        """Should skip segments with too few samples."""
        X, y, df, feature_names = sample_data_with_segments

        # Create df with tiny segment using row_nr
        df_with_tiny = df.with_row_index("idx").with_columns(
            pl.when(pl.col("idx") < 10)
            .then(pl.lit("tiny"))
            .otherwise(pl.col("circuit"))
            .alias("circuit")
        ).drop("idx")

        result = compute_segment_importance(
            model=trained_model,
            X=X,
            y=y,
            df=df_with_tiny,
            feature_names=feature_names,
            segment_column="circuit",
            method="gain",
        )

        # "tiny" segment (10 samples) should be skipped (< 100)
        assert "tiny" not in result.importance_by_segment


class TestSegmentImportanceResult:
    """Tests for SegmentImportanceResult dataclass."""

    def test_holds_data(self):
        """Should store all fields."""
        result = SegmentImportanceResult(
            segment_column="circuit",
            segment_values=["tour", "chal"],
            importance_by_segment={
                "tour": {"f1": 0.6, "f2": 0.4},
                "chal": {"f1": 0.5, "f2": 0.5},
            },
            overall_importance={"f1": 0.55, "f2": 0.45},
        )

        assert result.segment_column == "circuit"
        assert result.segment_values == ["tour", "chal"]
        assert result.importance_by_segment["tour"]["f1"] == 0.6


class TestSplitComparisonResult:
    """Tests for SplitComparisonResult dataclass."""

    def test_holds_data(self):
        """Should store all fields."""
        result = SplitComparisonResult(
            segment_column="circuit",
            single_model_metrics={"accuracy": 0.65, "calibration_error": 0.04},
            split_model_metrics={"accuracy": 0.66, "calibration_error": 0.038},
            per_segment_metrics={
                "tour": {"accuracy": 0.68},
                "chal": {"accuracy": 0.64},
            },
            recommendation="Split models improve calibration_error by 0.002",
        )

        assert result.segment_column == "circuit"
        assert result.single_model_metrics["accuracy"] == 0.65
        assert "tour" in result.per_segment_metrics


class TestSegmentAnalyzer:
    """Tests for SegmentAnalyzer class."""

    def test_initializes_with_defaults(self, tmp_path):
        """Should initialize with default segment columns."""
        config_path = tmp_path / "config.yaml"
        config_path.touch()

        analyzer = SegmentAnalyzer(config_path=config_path)

        assert analyzer.segment_columns == ["circuit"]
        assert analyzer.importance_method == "permutation"
        assert analyzer.metric == "calibration_error"

    def test_custom_segments(self, tmp_path):
        """Should accept custom segment columns."""
        config_path = tmp_path / "config.yaml"
        config_path.touch()

        analyzer = SegmentAnalyzer(
            config_path=config_path,
            segment_columns=["circuit", "surface"],
        )

        assert analyzer.segment_columns == ["circuit", "surface"]

    def test_analyze_importance_returns_dict(
        self, trained_model, sample_data_with_segments, tmp_path
    ):
        """Should return dict of importance results."""
        X, y, df, feature_names = sample_data_with_segments
        config_path = tmp_path / "config.yaml"
        config_path.touch()

        analyzer = SegmentAnalyzer(
            config_path=config_path,
            segment_columns=["circuit"],
            importance_method="gain",
        )

        results = analyzer.analyze_importance(
            model=trained_model,
            X=X,
            y=y,
            df=df,
            feature_names=feature_names,
        )

        assert isinstance(results, dict)
        assert "circuit" in results
        assert isinstance(results["circuit"], SegmentImportanceResult)

    def test_analyze_importance_skips_missing_columns(
        self, trained_model, sample_data_with_segments, tmp_path
    ):
        """Should skip columns not in DataFrame."""
        X, y, df, feature_names = sample_data_with_segments
        config_path = tmp_path / "config.yaml"
        config_path.touch()

        analyzer = SegmentAnalyzer(
            config_path=config_path,
            segment_columns=["circuit", "nonexistent"],
            importance_method="gain",
        )

        results = analyzer.analyze_importance(
            model=trained_model,
            X=X,
            y=y,
            df=df,
            feature_names=feature_names,
        )

        assert "circuit" in results
        assert "nonexistent" not in results
