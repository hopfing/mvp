"""Feature-error analysis pipeline.

Per the plan at `mvp-docs/experiments/2026-06-03-feature-error-analysis-plan.md`,
finds feature-grounded failure modes — which feature configurations produce
overconfident wrong predictions, where the model's representation breaks down.

Public API:
    join_predictions_with_features: Step 1 — load fold_predictions.parquet (or
        backtest.csv) and join with FeatureEngine output.
"""
from mvp.model.error_analysis.feature_join import (
    join_predictions_with_features,
)

__all__ = ["join_predictions_with_features"]
