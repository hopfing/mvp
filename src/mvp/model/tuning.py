"""Model hyperparameter tuning via grid search."""

import itertools
import json
import logging
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

DEFAULT_GRIDS: dict[str, dict[str, list[Any]]] = {
    "xgboost": {
        "max_depth": [3, 4, 5, 6],
        "learning_rate": [0.03, 0.05, 0.1, 0.15],
        "n_estimators": [100, 200, 300, 500],
        "min_child_weight": [3, 5, 10],
        "subsample": [0.7, 0.8, 0.9],
        "colsample_bytree": [0.7, 0.8, 0.9, 1.0],
        "colsample_bylevel": [0.7, 0.8, 0.9, 1.0],
        "colsample_bynode": [0.7, 0.8, 0.9, 1.0],
        "gamma": [0, 0.1, 0.5, 1.0, 5.0],
        "reg_alpha": [0, 0.01, 0.1, 1.0],
        "reg_lambda": [0.1, 1.0, 5.0, 10.0],
        "max_delta_step": [0, 1, 5],
        "scale_pos_weight": [0.9, 1.0, 1.1],
    },
    "logistic": {
        "C": [0.01, 0.1, 1.0, 10.0],
    },
    "random_forest": {
        "n_estimators": [100, 200, 500],
        "max_depth": [4, 6, 8],
        "min_samples_leaf": [10, 20, 50],
    },
    "neural_net": {
        "hidden_layers": [[32, 16], [64, 32], [128, 64], [64, 32, 16]],
        "dropout": [0.1, 0.2, 0.3, 0.5],
        "learning_rate": [0.0001, 0.0005, 0.001, 0.005],
        "batch_size": [256, 512, 1024, 2048],
        "epochs": [15, 30, 50],
        "patience": [3, 5, 10],
        "batch_norm": [True, False],
    },
}


@dataclass
class TuneResult:
    """Result from a single tuning combo."""

    params: dict[str, Any]
    metrics: dict[str, float]
    duration_s: float


@dataclass
class TuneState:
    """Persistent state for a tuning session."""

    config_path: str
    model_type: str
    results: list[TuneResult] = field(default_factory=list)

    def save(self, path: Path) -> None:
        data = {
            "config_path": self.config_path,
            "model_type": self.model_type,
            "results": [
                {
                    "params": r.params,
                    "metrics": r.metrics,
                    "duration_s": r.duration_s,
                }
                for r in self.results
            ],
        }
        path.write_text(json.dumps(data, indent=2))

    @classmethod
    def load(cls, path: Path) -> "TuneState":
        data = json.loads(path.read_text())
        results = [
            TuneResult(
                params=r["params"],
                metrics=r["metrics"],
                duration_s=r["duration_s"],
            )
            for r in data["results"]
        ]
        return cls(
            config_path=data["config_path"],
            model_type=data["model_type"],
            results=results,
        )

    def _params_key(self, params: dict[str, Any]) -> tuple:
        return tuple(
            (k, tuple(v) if isinstance(v, list) else v)
            for k, v in sorted(params.items())
        )

    @property
    def _completed_keys(self) -> set[tuple]:
        if not hasattr(self, "_cache"):
            self._cache = {self._params_key(r.params) for r in self.results}
        return self._cache

    def _invalidate_cache(self) -> None:
        if hasattr(self, "_cache"):
            del self._cache

    def already_run(self, params: dict[str, Any]) -> bool:
        """Check if a param combo has already been evaluated."""
        return self._params_key(params) in self._completed_keys


def _param_combo_str(params: dict[str, Any]) -> str:
    return ", ".join(f"{k}={v}" for k, v in sorted(params.items()))


class HyperparamTuner:
    """Grid search over model hyperparameters."""

    def __init__(
        self,
        config_path: Path | str,
        param_grid: dict[str, list[Any]] | None = None,
        param_overrides: dict[str, Any] | None = None,
        metric: str = "log_loss",
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        state_dir: Path | str | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        self.metric = metric
        self.matches_path = matches_path
        self.cache_dir = cache_dir

        with open(self.config_path) as f:
            self.base_config = yaml.safe_load(f)

        self.model_type = self.base_config["model"]["type"]

        if param_grid is not None:
            self.param_grid = param_grid
        elif self.model_type in DEFAULT_GRIDS:
            self.param_grid = dict(DEFAULT_GRIDS[self.model_type])
        else:
            raise ValueError(
                f"No default grid for model type '{self.model_type}'"
                " — pass param_grid explicitly"
            )

        # Fix specific params to a single value, removing them from the sweep
        if param_overrides:
            for k, v in param_overrides.items():
                self.param_grid[k] = [v]

        state_dir = Path(state_dir) if state_dir else Path(
            "data/tuning"
        )
        state_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = state_dir / f"{self.config_path.stem}.json"

        if self.state_path.exists():
            self.state = TuneState.load(self.state_path)
            logger.info(
                "Resumed tuning state: %d combos already run",
                len(self.state.results),
            )
        else:
            self.state = TuneState(
                config_path=str(self.config_path),
                model_type=self.model_type,
            )

    def _count_combos(self) -> int:
        """Count total combinations without generating them."""
        count = 1
        for values in self.param_grid.values():
            count *= len(values)
        return count

    def _iter_combos(self):
        """Yield param combinations, baseline first, skipping already-run.

        Generator — does not materialize the full grid in memory.
        """
        keys = sorted(self.param_grid.keys())
        values = [self.param_grid[k] for k in keys]

        # Extract current params from the base config as baseline
        base_params = self.base_config.get("model", {}).get("params", {})
        baseline = {k: base_params[k] for k in keys if k in base_params}

        baseline_in_grid = all(
            baseline.get(k) in self.param_grid.get(k, [])
            for k in keys
        )

        # Yield baseline first if applicable
        if baseline_in_grid and not self.state.already_run(baseline):
            yield baseline

        for combo_vals in itertools.product(*values):
            params = dict(zip(keys, combo_vals))
            if baseline_in_grid and params == baseline:
                continue
            if self.state.already_run(params):
                continue
            yield params

    def _run_one(self, params: dict[str, Any]) -> TuneResult:
        """Run a single param combination through ExperimentRunner."""
        from mvp.model.runner import ExperimentRunner

        config = dict(self.base_config)
        config["model"] = dict(config["model"])
        base_params = dict(config["model"].get("params") or {})
        base_params.update(params)
        config["model"]["params"] = base_params

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False,
        ) as f:
            yaml.dump(config, f, default_flow_style=False)
            temp_path = Path(f.name)

        try:
            t0 = time.perf_counter()
            # Suppress per-fold logging during tuning
            runner_logger = logging.getLogger("mvp.model.runner")
            engine_logger = logging.getLogger("mvp.model.engine")
            prev_runner = runner_logger.level
            prev_engine = engine_logger.level
            runner_logger.setLevel(logging.WARNING)
            engine_logger.setLevel(logging.WARNING)
            try:
                runner = ExperimentRunner(
                    config_path=temp_path,
                    matches_path=self.matches_path,
                    cache_dir=self.cache_dir,
                    run_name=f"tune_{self.config_path.stem}",
                    log_to_mlflow=True,
                )
                result = runner.run()
            finally:
                runner_logger.setLevel(prev_runner)
                engine_logger.setLevel(prev_engine)
            duration = time.perf_counter() - t0

            return TuneResult(
                params=params,
                metrics=result["metrics"],
                duration_s=round(duration, 1),
            )
        finally:
            temp_path.unlink(missing_ok=True)

    def run(self, verbose: bool = True) -> TuneState:
        """Run the grid search, skipping already-completed combos."""
        total_grid = self._count_combos()
        already_done = len(self.state.results)

        logger.info(
            "Tuning %s: %d in grid, %d done",
            self.config_path.stem, total_grid, already_done,
        )

        from tqdm import tqdm

        # Count remaining by checking how many grid combos are already done
        done_in_grid = sum(
            1 for combo_vals in itertools.product(
                *[self.param_grid[k] for k in sorted(self.param_grid)]
            )
            if self.state.already_run(
                dict(zip(sorted(self.param_grid), combo_vals))
            )
        )
        remaining = total_grid - done_in_grid

        run_count = 0
        for params in tqdm(
            self._iter_combos(),
            desc="Tuning",
            total=remaining,
        ):
            run_count += 1
            try:
                result = self._run_one(params)
                self.state.results.append(result)
                self.state._invalidate_cache()
                self.state.save(self.state_path)
            except Exception as e:
                logger.error("FAILED: %s", e)
                logger.error("Stopping tuning due to failure")
                break

        logger.info(
            "Tuning complete: %d results in %s",
            len(self.state.results), self.state_path,
        )
        return self.state
