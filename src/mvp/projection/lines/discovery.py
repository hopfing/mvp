"""Forward-selection orchestrator for line-market binary classifiers.

Thin wrapper around `FeatureSelector` (the generic FS engine) plus the
`FastLinesSelector` precompute. Mirrors `ProjectionDiscovery`'s shape:
load config → resolve candidate pool → build scorer → run FS → save promoted
config.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from mvp.model.discovery.discover import get_all_feature_specs
from mvp.model.discovery.selection import FeatureSelector, SelectionResult
from mvp.projection.lines.config import LinesDiscoveryConfig
from mvp.projection.lines.fast_selection import FastLinesSelector


logger = logging.getLogger(__name__)


# Lines metrics — all minimization targets.
_DIRECTION = "minimize"


@dataclass
class LinesDiscoveryResult:
    selected_features: list[str]
    selection_result: SelectionResult | None = None
    final_metric: float = 0.0
    n_experiments: int = 0


class LinesDiscovery:
    """Orchestrates forward selection for line-market binary classifiers."""

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        verbose: bool = False,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = LinesDiscoveryConfig.from_file(config_path)
        self.matches_path = matches_path
        self.cache_dir = cache_dir
        self.verbose = verbose

    def run(
        self,
        checkpoint_path: Path | None = None,
        checkpoint_interval: int | None = None,
    ) -> LinesDiscoveryResult:
        logger.info("Lines Discovery: %s", self.config_path.stem)
        logger.info("=" * 60)

        feat_cfg = self.config.discovery.features
        all_features = get_all_feature_specs(window_sizes=feat_cfg.window_sizes)

        if feat_cfg.include:
            included = set(feat_cfg.include)
            all_features = [f for f in all_features if f in included]
            logger.info("Restricted to %d features via include", len(all_features))

        if feat_cfg.exclude:
            excluded = set(feat_cfg.exclude)
            all_features = [f for f in all_features if f not in excluded]
            logger.info("Excluding %d features", len(excluded))

        base = list(feat_cfg.base)
        if base:
            missing = [b for b in base if b not in all_features]
            if missing:
                # Base features must be in the precomputed candidate pool so the
                # scorer can resolve their column indices.
                all_features = list(all_features) + missing

        logger.info(
            "Precomputing %d feature specs for fast forward selection (target=%s, metric=%s)",
            len(all_features), self.config.discovery.target, self.config.discovery.metric,
        )
        fast = FastLinesSelector(
            config=self.config,
            all_feature_specs=all_features,
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )
        fast.precompute()
        scorer = fast.create_scorer()

        selector = FeatureSelector(
            scorer=scorer,
            all_features=all_features,
            method=self.config.discovery.selection_method,
            direction=_DIRECTION,
            min_features=1,
            max_features=feat_cfg.max,
            base_features=base,
        )

        kwargs: dict[str, Any] = {"verbose": True, "checkpoint_path": checkpoint_path}
        if checkpoint_interval is not None:
            kwargs["checkpoint_interval"] = checkpoint_interval
        selection_result = selector.run(**kwargs)
        selected = selection_result.selected_features

        if not selected:
            logger.info("No features selected.")
            return LinesDiscoveryResult(
                selected_features=[],
                selection_result=selection_result,
            )

        final_metric = selection_result.final_metric
        logger.info("")
        logger.info("RESULTS")
        logger.info("-" * 30)
        logger.info("Feature set (%d features):", len(selected))
        for f in selected:
            logger.info("  - %s", f)
        logger.info("Final %s: %.6f", self.config.discovery.metric, final_metric)

        return LinesDiscoveryResult(
            selected_features=selected,
            selection_result=selection_result,
            final_metric=final_metric,
        )

    def save_config(
        self, output_path: Path | str, result: LinesDiscoveryResult,
    ) -> None:
        """Emit a runnable IID projection config from the FS-selected features.

        The score-state chain pipeline is the production projector; the lines
        proxy hands off its discovered match-level features into a score-state
        config with an empty point-level set (curate or run a separate
        point-level FS afterwards).
        """
        config_dict = self._to_iid_projection_config_dict(result.selected_features)
        with open(output_path, "w") as f:
            yaml.safe_dump(config_dict, f, sort_keys=False)
        logger.info("Saved config to: %s", output_path)

    def _to_iid_projection_config_dict(
        self, selected_match_level: list[str],
    ) -> dict[str, Any]:
        from mvp.model.engine import parse_feature_spec

        include_specs: list[str] = []
        seen: set[str] = set()
        for spec in selected_match_level:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            if prefix == "player":
                swap_full = f"opp_{base_name}"
            elif prefix == "opp":
                swap_full = f"player_{base_name}"
            else:
                swap_full = full_name

            if params:
                param_str = ", ".join(f"{k}={v}" for k, v in params.items())
                own_spec = f"{full_name}({param_str})"
                swap_spec = f"{swap_full}({param_str})"
            else:
                own_spec = full_name
                swap_spec = swap_full

            for s in (own_spec, swap_spec):
                if s not in seen:
                    include_specs.append(s)
                    seen.add(s)

        return {
            "description": (
                self.config.description
                or f"IID score-state projection from lines FS (target={self.config.discovery.target})"
            ),
            "data": self.config.data.model_dump(),
            "features": {"include": include_specs},
            "serve_model": {
                "type": "score_state",
                "model_type": "xgboost",
                "match_level_features": list(selected_match_level),
                "point_level_features": [],
                "params": {},
            },
            "validation": self.config.validation.model_dump(),
        }
